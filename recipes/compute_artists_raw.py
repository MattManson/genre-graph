# ------------------------------------------------------------
# Dataiku Recipe: lastfm_snowball_pipeline
# Outputs: artists_raw, pipeline_runs
#
# Run behaviour:
#   1. Load existing artists from Snowflake → used ONLY for refresh, not for blocking discovery
#   2. Sample existing artists → re-pull and write update rows if changed
#   3. Snowball new artists until MAX_NEW_ARTISTS or RUN_TIME_LIMIT_S reached
#   4. Batch-write throughout, log to pipeline_runs on completion
#
# KEY FIX: seen_artists for the snowball starts EMPTY each run.
# existing.keys() is only used by the refresh phase.
# This allows the snowball to re-discover and re-enrich freely,
# growing the dataset with each run rather than stalling after first pass.
# Dedup is handled downstream in ARTISTS_CLEAN via ROW_NUMBER() on RUN_TS.
#
# Append-only: run_id + run_ts on every row = full audit trail.
# ------------------------------------------------------------

import dataiku
import pandas as pd
import requests
import time
import uuid
import json
import random
import logging
from datetime import datetime, timezone
from collections import deque
from typing import Optional, List, Tuple

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY             = dataiku.get_custom_variables()["LASTFM_API_KEY"]
BASE_URL            = "https://ws.audioscrobbler.com/2.0/"
RUN_ID              = str(uuid.uuid4())
RUN_TS              = datetime.now(timezone.utc)

# ── Production config ─────────────────────────────────────────────────────────
RUN_TIME_LIMIT_S    = 7200           # 2 hour hard stop
MAX_NEW_ARTISTS     = 5_000          # max new artists to add per run
MAX_TAGS            = 10_000         # max unique tags to track across the snowball
MIN_LISTENERS       = 100            # drop artists below this threshold
MIN_TAG_COUNT       = 1              # drop tags used fewer than N times on an artist
REFRESH_SAMPLE_SIZE = 50             # how many existing artists to refresh per run
LISTENER_DRIFT_PCT  = 0.10           # refresh trigger threshold
TOP_ARTISTS_PAGES   = 5              # chart.getTopArtists seed pages (50/page)
TOP_TAGS_PAGES      = 20             # tag.getTopTags seed pages (50/page)
ARTISTS_PER_TAG     = 100            # tag.getTopArtists expansion width

# ── overnight config (uncomment to use) ───────────────────────────────────────
# RUN_TIME_LIMIT_S    = 32400
# MAX_NEW_ARTISTS     = 500_000
# MAX_TAGS            = 50_000
# TOP_ARTISTS_PAGES   = 20
# TOP_TAGS_PAGES      = 20
# ARTISTS_PER_TAG     = 50
# REFRESH_SAMPLE_SIZE = 50
# LISTENER_DRIFT_PCT  = 0.10
# MIN_LISTENERS       = 100
# MIN_TAG_COUNT       = 1

# ── Test config (uncomment to use) ────────────────────────────────────────────
# RUN_TIME_LIMIT_S    = 300
# MAX_NEW_ARTISTS     = 200
# MAX_TAGS            = 500
# MIN_TAG_COUNT       = 1
# MIN_LISTENERS       = 100
# REFRESH_SAMPLE_SIZE = 10
# LISTENER_DRIFT_PCT  = 0.10
# TOP_ARTISTS_PAGES   = 1
# TOP_TAGS_PAGES      = 1
# ARTISTS_PER_TAG     = 10

# API rate
REQUESTS_PER_SEC    = 4
SLEEP_BETWEEN_CALLS = 1.0 / REQUESTS_PER_SEC

# Batch write size (rows before flushing to Snowflake)
WRITE_BATCH_SIZE    = 1_000

# ── Dataiku datasets ──────────────────────────────────────────────────────────
artists_raw_ds   = dataiku.Dataset("artists_raw")
pipeline_runs_ds = dataiku.Dataset("pipeline_runs")

# ── API helper ────────────────────────────────────────────────────────────────

def lfm_get(method: str, params: dict, retries: int = 4) -> Optional[dict]:
    """Last.fm GET with exponential backoff."""
    payload = {"method": method, "api_key": API_KEY, "format": "json", **params}
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=payload, timeout=15)
            if resp.status_code == 429:
                wait = 15 * (attempt + 1)
                log.warning(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                log.warning(f"HTTP {resp.status_code} on {method} attempt {attempt+1}")
                time.sleep(2 ** attempt)
                continue
            data = resp.json()
            if "error" in data:
                if data["error"] == 6:
                    return None
                log.warning(f"API error {data['error']}: {data.get('message')} on {method}")
                return None
            return data
        except requests.RequestException as e:
            log.warning(f"Request exception on {method} attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return None


def throttle():
    time.sleep(SLEEP_BETWEEN_CALLS)
    
def safe_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

# ── Step 1: Load existing artists from Snowflake ──────────────────────────────
# Returns the existing dict for use in the REFRESH phase only.
# Do NOT feed this into seen_artists for the snowball — that set starts empty.

def load_existing_artists() -> dict:
    log.info("Loading existing artists from Snowflake...")
    try:
        df = artists_raw_ds.get_dataframe(
            columns=["ARTIST_NAME", "LISTENERS", "TAGS", "RUN_TS"]
        )
        if df.empty:
            log.info("  artists_raw is empty — this is a first run.")
            return {}
        df["RUN_TS"] = pd.to_datetime(df["RUN_TS"], utc=True)
        df = df.sort_values("RUN_TS").groupby("ARTIST_NAME", as_index=False).last()
        existing = {
            row["ARTIST_NAME"].lower(): {
                "artist_name": row["ARTIST_NAME"],
                "listeners":   int(row["LISTENERS"] or 0),
                "tags":        row["TAGS"] or "[]",
            }
            for _, row in df.iterrows()
        }
        log.info(f"  Loaded {len(existing):,} existing artists (for refresh only).")
        return existing
    except Exception as e:
        log.warning(f"Could not load existing artists (first run?): {e}")
        return {}


# ── Step 2: Change detection on existing artists ──────────────────────────────

def refresh_existing_artists(existing: dict, writer) -> int:
    """Re-pull a random sample of known artists and write a new row if anything changed."""
    if not existing:
        return 0

    sample_keys = random.sample(list(existing.keys()), min(REFRESH_SAMPLE_SIZE, len(existing)))
    log.info(f"Refreshing {len(sample_keys)} existing artists for changes...")

    updates = []
    checked = 0

    for key in sample_keys:
        baseline  = existing[key]
        data      = lfm_get("artist.getInfo", {"artist": baseline["artist_name"], "autocorrect": 1})
        throttle()
        checked  += 1

        if not data:
            continue

        artist        = data.get("artist", {})
        stats         = artist.get("stats", {})
        new_listeners = int(stats.get("listeners", 0) or 0)
        raw_tags      = artist.get("tags", {}).get("tag", [])
        new_tags      = sorted([t["name"].strip().lower() for t in raw_tags
                                 if t.get("name") and safe_int(t.get("count", 0)) >= MIN_TAG_COUNT])
        old_tags      = sorted(json.loads(baseline["tags"]) if baseline["tags"] else [])
        old_listeners = baseline["listeners"]

        listener_drift = (
            abs(new_listeners - old_listeners) / max(old_listeners, 1)
        ) > LISTENER_DRIFT_PCT
        tags_changed = new_tags != old_tags

        if listener_drift or tags_changed:
            bio = artist.get("bio", {}).get("summary", "").strip()
            if "<a href" in bio:
                bio = bio[:bio.index("<a href")].strip()

            updates.append({
                "RUN_ID":        RUN_ID,
                "RUN_TS":        RUN_TS,
                "ARTIST_NAME":   artist.get("name", baseline["artist_name"]).strip(),
                "MBID":          artist.get("mbid", ""),
                "LISTENERS":     new_listeners,
                "PLAYCOUNT":     int(stats.get("playcount", 0) or 0),
                "TAGS":          json.dumps(new_tags),
                "BIO_SUMMARY":   bio[:2000],
                "SOURCE":        "refresh",
                "DISCOVERY_TAG": "",
            })

        if checked % 50 == 0:
            log.info(f"  Refresh progress: {checked}/{len(sample_keys)} checked, {len(updates)} changes found")

    written = flush_batch(updates, writer)
    log.info(f"Refresh complete — {checked} checked, {written} updates written.")
    return written


# ── Step 3: Snowball — seed queues ────────────────────────────────────────────

def seed_tags(tag_queue: deque, seen_tags: set):
    log.info("Seeding tag queue from global top tags...")
    for page in range(1, TOP_TAGS_PAGES + 1):
        data = lfm_get("tag.getTopTags", {"num_res": 50, "page": page})
        throttle()
        if not data:
            continue
        for t in data.get("toptags", {}).get("tag", []):
            name = t.get("name", "").strip().lower()
            if name and name not in seen_tags:
                seen_tags.add(name)
                tag_queue.append(name)
    log.info(f"  {len(tag_queue)} tags seeded.")


def seed_artists_from_chart(artist_queue: deque, seen_artists: set) -> List[dict]:
    """
    Seed from chart. seen_artists here is the SNOWBALL set (starts empty),
    so chart artists will be queued even if they're already in Snowflake.
    This ensures they get re-enriched with current tags/bio on this run.
    """
    log.info("Seeding artist queue from chart...")
    seed_rows = []
    for page in range(1, TOP_ARTISTS_PAGES + 1):
        data = lfm_get("chart.getTopArtists", {"limit": 50, "page": page})
        throttle()
        if not data:
            continue
        for a in data.get("artists", {}).get("artist", []):
            name      = (a.get("name") or "").strip()
            listeners = int(a.get("listeners", 0) or 0)
            if not name or name.lower() in seen_artists or listeners < MIN_LISTENERS:
                continue
            seen_artists.add(name.lower())
            artist_queue.append((name, "chart_seed"))
            seed_rows.append({
                "RUN_ID":        RUN_ID,
                "RUN_TS":        RUN_TS,
                "ARTIST_NAME":   name,
                "MBID":          a.get("mbid", ""),
                "LISTENERS":     listeners,
                "PLAYCOUNT":     int(a.get("playcount", 0) or 0),
                "TAGS":          json.dumps([]),
                "BIO_SUMMARY":   "",
                "SOURCE":        "chart_seed",
                "DISCOVERY_TAG": "chart_seed",
            })
    log.info(f"  {len(seed_rows)} chart artists queued.")
    return seed_rows


# ── Step 3: Snowball — enrich + expand ───────────────────────────────────────

def enrich_artist(
    artist_name: str,
    discovery_tag: str,
    tag_queue: deque,
    seen_tags: set,
) -> Optional[dict]:
    data = lfm_get("artist.getInfo", {"artist": artist_name, "autocorrect": 1})
    throttle()
    if not data:
        return None

    artist    = data.get("artist", {})
    stats     = artist.get("stats", {})
    listeners = int(stats.get("listeners", 0) or 0)

    if listeners < MIN_LISTENERS:
        return None

    raw_tags  = artist.get("tags", {}).get("tag", [])
    tag_names = [t["name"].strip().lower() for t in raw_tags
                 if t.get("name") and safe_int(t.get("count", 0)) >= MIN_TAG_COUNT]

    for tag in tag_names:
        if tag and tag not in seen_tags and len(seen_tags) < MAX_TAGS:
            seen_tags.add(tag)
            tag_queue.append(tag)

    bio = artist.get("bio", {}).get("summary", "").strip()
    if "<a href" in bio:
        bio = bio[:bio.index("<a href")].strip()

    return {
        "RUN_ID":        RUN_ID,
        "RUN_TS":        RUN_TS,
        "ARTIST_NAME":   artist.get("name", artist_name).strip(),
        "MBID":          artist.get("mbid", ""),
        "LISTENERS":     listeners,
        "PLAYCOUNT":     int(stats.get("playcount", 0) or 0),
        "TAGS":          json.dumps(tag_names),
        "BIO_SUMMARY":   bio[:2000],
        "SOURCE":        "snowball",
        "DISCOVERY_TAG": discovery_tag,
    }


def expand_tag(tag_name: str, artist_queue: deque, seen_artists: set) -> int:
    """Expand a tag into artists. Stores (artist_name, tag_name) tuples in the queue."""
    data = lfm_get("tag.getTopArtists", {"tag": tag_name, "limit": ARTISTS_PER_TAG})
    throttle()
    if not data:
        return 0
    queued = 0
    for a in data.get("topartists", {}).get("artist", []):
        name = (a.get("name") or "").strip()
        key  = name.lower()
        if name and key not in seen_artists:
            seen_artists.add(key)
            artist_queue.append((name, tag_name))
            queued += 1
    return queued


# ── Batch writer ──────────────────────────────────────────────────────────────

def flush_batch(batch: list, writer) -> int:
    if not batch:
        return 0
    df = pd.DataFrame(batch)
    df["RUN_TS"]    = pd.to_datetime(df["RUN_TS"], utc=True)
    df["LISTENERS"] = df["LISTENERS"].astype(int)
    df["PLAYCOUNT"] = df["PLAYCOUNT"].astype(int)
    writer.write_dataframe(df)
    return len(df)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info(f"=== Run ID: {RUN_ID} | Started: {RUN_TS} ===")
    start = time.time()

    def elapsed():
        return time.time() - start

    def time_remaining():
        return RUN_TIME_LIMIT_S - elapsed()

    # ── 1. Load existing artists — for REFRESH only ───────────────────────────
    existing = load_existing_artists()

    # KEY FIX: seen_artists for the snowball starts EMPTY.
    # existing.keys() is NOT fed in here — that was the bug causing the pipeline
    # to stall after the first run. The snowball needs to freely re-discover
    # artists; dedup is handled downstream in ARTISTS_CLEAN.
    seen_artists:     set = set()                 # snowball: starts empty every run
    seen_tags:        set = set()                 # fresh each run, grows via enrichment
    written_this_run: set = set()                 # prevent within-run dupes only
    artist_queue:   deque = deque()
    tag_queue:      deque = deque()

    total_written     = 0
    new_artists_added = 0
    updates_written   = 0
    artists_processed = 0

    with artists_raw_ds.get_writer() as writer:

        # ── 2. Refresh sample of existing artists ─────────────────────────────
        # existing dict is passed here — refresh uses it to detect drift
        updates_written = refresh_existing_artists(existing, writer)
        total_written  += updates_written

        log.info(f"Time after refresh: {round(elapsed())}s — {round(time_remaining()/60, 1)} mins remaining")

        # ── 3. Seed queues for new discovery ──────────────────────────────────
        seed_tags(tag_queue, seen_tags)
        # seen_artists (empty) is passed — chart artists will ALL be queued this run
        seed_rows = seed_artists_from_chart(artist_queue, seen_artists)
        total_written += flush_batch(seed_rows, writer)
        new_artists_added += len(seed_rows)

        log.info(
            f"Seeding done — artist_q: {len(artist_queue):,} | "
            f"tag_q: {len(tag_queue):,} | "
            f"time remaining: {round(time_remaining()/60, 1)} mins"
        )

        # ── 4. Snowball new artists until time/cap limit ───────────────────────────
        current_batch: list = []
        last_flush_ts = time.time()  # ADD THIS

        while (artist_queue or tag_queue) and new_artists_added < MAX_NEW_ARTISTS:

            if time_remaining() < 60:
                log.info("Approaching time limit — stopping discovery and flushing.")
                break

            if artist_queue:
                artist_name, discovery_tag = artist_queue.popleft()
                artists_processed += 1

                row = enrich_artist(artist_name, discovery_tag, tag_queue, seen_tags)
                if row:
                    artist_key = row["ARTIST_NAME"].lower()
                    if artist_key not in written_this_run:
                        current_batch.append(row)
                        written_this_run.add(artist_key)
                        new_artists_added += 1

            elif tag_queue:
                expand_tag(tag_queue.popleft(), artist_queue, seen_artists)

            # REPLACE the existing batch flush block with this:
            time_since_flush = time.time() - last_flush_ts
            if len(current_batch) >= WRITE_BATCH_SIZE or (current_batch and time_since_flush >= 600):
                written = flush_batch(current_batch, writer)
                total_written += written
                current_batch = []
                last_flush_ts = time.time()  # reset timer
                log.info(
                    f"  Batch flushed: {written:,} rows | "
                    f"total: {total_written:,} | "
                    f"new artists: {new_artists_added:,} | "
                    f"tags: {len(seen_tags):,} | "
                    f"time left: {round(time_remaining()/60, 1)}m"
                )

            elif tag_queue:
                # Only expand tags when artist queue is empty — keeps the loop
                # preferring enrichment over expansion to avoid unbounded queue growth
                expand_tag(tag_queue.popleft(), artist_queue, seen_artists)

        if current_batch:
            total_written += flush_batch(current_batch, writer)

    elapsed_s = round(elapsed(), 1)
    log.info(
        f"=== Complete === "
        f"{total_written:,} rows written | "
        f"{new_artists_added:,} new artists | "
        f"{updates_written:,} updates | "
        f"{len(seen_tags):,} tags | "
        f"{elapsed_s}s ({round(elapsed_s/3600, 2)}hrs)"
    )

    # ── 5. Write pipeline_runs row ────────────────────────────────────────────
    run_meta = pd.DataFrame([{
        "RUN_ID":             RUN_ID,
        "RUN_TS":             RUN_TS,
        "STATUS":             "success" if total_written > 0 else "empty",
        "ROWS_WRITTEN":       total_written,
        "NEW_ARTISTS_ADDED":  new_artists_added,
        "UPDATES_WRITTEN":    updates_written,
        "ARTISTS_SEEN_TOTAL": len(seen_artists) + len(written_this_run),
        "TAGS_DISCOVERED":    len(seen_tags),
        "ELAPSED_SECONDS":    int(elapsed_s),
        "CONFIG": json.dumps({
            "RUN_TIME_LIMIT_S":   RUN_TIME_LIMIT_S,
            "MAX_NEW_ARTISTS":    MAX_NEW_ARTISTS,
            "MAX_TAGS":           MAX_TAGS,
            "MIN_LISTENERS":      MIN_LISTENERS,
            "REFRESH_SAMPLE_SIZE":REFRESH_SAMPLE_SIZE,
            "LISTENER_DRIFT_PCT": LISTENER_DRIFT_PCT,
            "TOP_ARTISTS_PAGES":  TOP_ARTISTS_PAGES,
            "TOP_TAGS_PAGES":     TOP_TAGS_PAGES,
            "ARTISTS_PER_TAG":    ARTISTS_PER_TAG,
        }),
    }])

    with pipeline_runs_ds.get_writer() as writer:
        writer.write_dataframe(run_meta)
    log.info("pipeline_runs row written — done.")

    return total_written


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run()
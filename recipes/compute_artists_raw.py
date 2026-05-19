# ------------------------------------------------------------
# Dataiku Recipe: lastfm_snowball_pipeline
# Outputs: artists_raw, pipeline_runs
#
# Run behaviour:
#   1. Load existing artists from Snowflake → skip for new discovery
#   2. Sample existing artists → re-pull and write update rows if changed
#   3. Snowball new artists until MAX_NEW_ARTISTS or RUN_TIME_LIMIT_S reached
#   4. Batch-write throughout, log to pipeline_runs on completion
#
# Designed to run repeatedly. Each run grows the dataset and checks for drift.
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
from typing import Optional, List

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY             = dataiku.get_custom_variables()["LASTFM_API_KEY"]
BASE_URL            = "https://ws.audioscrobbler.com/2.0/"
RUN_ID              = str(uuid.uuid4())
RUN_TS              = datetime.now(timezone.utc)

# ── Production config (uncomment to use) ──────────────────────────────────────
# RUN_TIME_LIMIT_S    = 14400          # 4 hour hard stop
# MAX_NEW_ARTISTS     = 5_000          # max new artists to add per run
# MAX_TAGS            = 10_000         # max unique tags to track across the snowball
# MIN_LISTENERS       = 100            # drop artists below this threshold
# MIN_TAG_COUNT       = 3              # drop tags used fewer than N times on an artist
# REFRESH_SAMPLE_SIZE = 50             # how many existing artists to refresh per run
# LISTENER_DRIFT_PCT  = 0.10           # refresh trigger threshold
# TOP_ARTISTS_PAGES   = 5              # chart.getTopArtists seed pages (50/page)
# TOP_TAGS_PAGES      = 5              # tag.getTopTags seed pages (50/page)
# ARTISTS_PER_TAG     = 50             # tag.getTopArtists expansion width

# ── Test config ───────────────────────────────────────────────────────────────
RUN_TIME_LIMIT_S    = 300       # 5 minutes hard stop
MAX_NEW_ARTISTS     = 200       # cap new discoveries
MAX_TAGS            = 500       # limit tag snowball
MIN_TAG_COUNT       = 3         # drop tags used fewer than N times on an artist
MIN_LISTENERS       = 100       # minimum artist popularity filter
REFRESH_SAMPLE_SIZE = 10        # how many existing artists to refresh per run
LISTENER_DRIFT_PCT  = 0.10      # refresh trigger threshold
TOP_ARTISTS_PAGES   = 1         # pages of top artists to seed from
TOP_TAGS_PAGES      = 1         # pages of top tags to seed from
ARTISTS_PER_TAG     = 10        # artists to pull per tag

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


# ── Step 1: Load existing artists from Snowflake ──────────────────────────────

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
        log.info(f"  Loaded {len(existing):,} existing artists.")
        return existing

    except Exception as e:
        log.warning(f"Could not load existing artists (first run?): {e}")
        return {}


# ── Step 2: Change detection on existing artists ──────────────────────────────

def refresh_existing_artists(existing: dict, writer) -> int:
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
        new_tags      = sorted([t["name"].strip().lower() for t in raw_tags if t.get("name")])
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
            artist_queue.append(name)
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
                "DISCOVERY_TAG": "",
            })
    log.info(f"  {len(seed_rows)} new chart artists queued.")
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
    tag_names = [t["name"].strip().lower() for t in raw_tags if t.get("name")]

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
            artist_queue.append(name)
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

    # ── 1. Load existing artists from Snowflake ───────────────────────────────
    existing = load_existing_artists()

    seen_artists: set = set(existing.keys())
    seen_tags:    set = set()
    artist_queue: deque = deque()
    tag_queue:    deque = deque()

    total_written     = 0
    new_artists_added = 0
    updates_written   = 0
    artists_processed = 0

    with artists_raw_ds.get_writer() as writer:

        # ── 2. Refresh sample of existing artists ─────────────────────────────
        updates_written = refresh_existing_artists(existing, writer)
        total_written  += updates_written

        log.info(f"Time after refresh: {round(elapsed())}s — {round(time_remaining()/60, 1)} mins remaining")

        # ── 3. Seed queues for new discovery ──────────────────────────────────
        seed_tags(tag_queue, seen_tags)
        seed_rows = seed_artists_from_chart(artist_queue, seen_artists)
        total_written += flush_batch(seed_rows, writer)
        new_artists_added += len(seed_rows)

        log.info(
            f"Seeding done — artist_q: {len(artist_queue):,} | "
            f"tag_q: {len(tag_queue):,} | "
            f"time remaining: {round(time_remaining()/60, 1)} mins"
        )

        # ── 4. Snowball new artists until time/cap limit ───────────────────────
        current_batch: list = []

        while (artist_queue or tag_queue) and new_artists_added < MAX_NEW_ARTISTS:

            if time_remaining() < 5 * 60:
                log.info(f"Approaching time limit — stopping discovery and flushing.")
                break

            if artist_queue:
                artist_name = artist_queue.popleft()
                row = enrich_artist(artist_name, "", tag_queue, seen_tags)
                if row:
                    current_batch.append(row)
                    new_artists_added += 1
                artists_processed += 1

                if len(current_batch) >= WRITE_BATCH_SIZE:
                    written = flush_batch(current_batch, writer)
                    total_written += written
                    current_batch = []
                    log.info(
                        f"  Batch flushed: {written:,} rows | "
                        f"total: {total_written:,} | "
                        f"new artists: {new_artists_added:,} | "
                        f"tags: {len(seen_tags):,} | "
                        f"time left: {round(time_remaining()/60, 1)}m"
                    )

            if tag_queue:
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
        "ARTISTS_SEEN_TOTAL": len(seen_artists),
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
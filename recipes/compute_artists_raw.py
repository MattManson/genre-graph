# ------------------------------------------------------------
# Dataiku Recipe: lastfm_snowball_pipeline
# Outputs: artists_raw, pipeline_runs
# ------------------------------------------------------------

import dataiku
import pandas as pd
import requests
import time
import uuid
import json
import logging
from datetime import datetime, timezone
from collections import deque

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY          = "e4dfce9570521e545ee28165fbe85a82"
BASE_URL         = "https://ws.audioscrobbler.com/2.0/"
RUN_ID           = str(uuid.uuid4())
RUN_TS           = datetime.now(timezone.utc)

# Snowball controls
TOP_ARTISTS_PAGES   =         # chart.getTopArtists pages to seed (50 artists/page = 250 seeds)
TOP_TAGS_PAGES      = 10        # tag.getTopTags pages to also seed (50 tags/page = 250 tag seeds)
MAX_ARTISTS         = 50_000   # hard cap on unique artists to process
MAX_TAGS            = 10_000   # hard cap on unique tags to discover
ARTISTS_PER_TAG     = 100      # tag.getTopArtists limit (max 1000, 50 is a good balance vs API cost)
TAGS_PER_ARTIST     = 20       # artist.getTopTags limit
MIN_TAG_COUNT       = 3        # ignore tags with < N uses on an artist (noise filter)
MIN_LISTENERS       = 100      # ignore artists with < N listeners
REQUESTS_PER_SEC    = 4        # Last.fm allows 5/s; stay under
SLEEP_BETWEEN_CALLS = 1.0 / REQUESTS_PER_SEC

# ── Dataiku output datasets ───────────────────────────────────────────────────
artists_raw_ds   = dataiku.Dataset("artists_raw")
pipeline_runs_ds = dataiku.Dataset("pipeline_runs")

# ── Helpers ───────────────────────────────────────────────────────────────────

def lfm_get(method: str, params: dict, retries: int = 3) -> dict | None:
    """Single Last.fm API call with retry on transient errors."""
    payload = {
        "method":  method,
        "api_key": API_KEY,
        "format":  "json",
        **params,
    }
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=payload, timeout=15)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                log.warning(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                log.warning(f"HTTP {resp.status_code} on {method} attempt {attempt+1}")
                time.sleep(2)
                continue
            data = resp.json()
            if "error" in data:
                log.warning(f"API error {data['error']}: {data.get('message')} on {method}")
                return None
            return data
        except requests.RequestException as e:
            log.warning(f"Request exception on {method} attempt {attempt+1}: {e}")
            time.sleep(3)
    return None


def throttle():
    time.sleep(SLEEP_BETWEEN_CALLS)


# ── Phase 1: Seed tag queue from Last.fm global top tags ──────────────────────

def seed_tags_from_top_tags(tag_queue: deque, seen_tags: set):
    """
    Pull Last.fm's chart of globally most-used tags.
    This covers mainstream genres (pop, rock, hip-hop, jazz, classical…)
    without any hard-coded seed list.
    """
    log.info("Seeding tag queue from global top tags…")
    for page in range(1, TOP_TAGS_PAGES + 1):
        data = lfm_get("tag.getTopTags", {"num_res": 50, "page": page})
        throttle()
        if not data:
            continue
        tags = data.get("toptags", {}).get("tag", [])
        for t in tags:
            name = t.get("name", "").strip().lower()
            if name and name not in seen_tags:
                seen_tags.add(name)
                tag_queue.append(name)
        log.info(f"  top-tags page {page}: {len(tags)} tags — queue size {len(tag_queue)}")


# ── Phase 2: Seed artist queue from global top artists ───────────────────────

def seed_artists_from_chart(artist_queue: deque, seen_artists: set) -> list[dict]:
    """
    Pull the global top-artists chart — mainstream coverage from day one.
    Returns raw seed artist rows.
    """
    log.info("Seeding artist queue from global top artists chart…")
    seed_rows = []
    for page in range(1, TOP_ARTISTS_PAGES + 1):
        data = lfm_get("chart.getTopArtists", {"limit": 50, "page": page})
        throttle()
        if not data:
            continue
        artists = data.get("artists", {}).get("artist", [])
        for a in artists:
            name = a.get("name", "").strip()
            mbid = a.get("mbid", "")
            listeners = int(a.get("listeners", 0) or 0)
            if not name or name.lower() in seen_artists or listeners < MIN_LISTENERS:
                continue
            seen_artists.add(name.lower())
            artist_queue.append(name)
            seed_rows.append({
                "run_id":           RUN_ID,
                "run_ts":           RUN_TS,
                "artist_name":      name,
                "mbid":             mbid,
                "listeners":        listeners,
                "playcount":        int(a.get("playcount", 0) or 0),
                "tags":             json.dumps([]),   # filled in snowball phase
                "bio_summary":      "",
                "source":           "chart_seed",
                "discovery_tag":    "",
            })
        log.info(f"  chart page {page}: {len(artists)} artists — queue size {len(artist_queue)}")
    return seed_rows


# ── Phase 3: Fetch tags for one artist ───────────────────────────────────────

def get_artist_tags(artist_name: str) -> list[str]:
    """Return list of tag names for an artist (filtered by MIN_TAG_COUNT)."""
    data = lfm_get("artist.getTopTags", {"artist": artist_name, "autocorrect": 1})
    throttle()
    if not data:
        return []
    raw = data.get("toptags", {}).get("tag", [])
    return [
        t["name"].strip().lower()
        for t in raw
        if int(t.get("count", 0) or 0) >= MIN_TAG_COUNT and t.get("name")
    ][:TAGS_PER_ARTIST]


# ── Phase 4: Fetch artists for one tag ───────────────────────────────────────

def get_tag_artists(tag_name: str) -> list[dict]:
    """Return list of artist dicts for a tag."""
    data = lfm_get("tag.getTopArtists", {"tag": tag_name, "limit": ARTISTS_PER_TAG})
    throttle()
    if not data:
        return []
    return data.get("topartists", {}).get("artist", [])


# ── Phase 5: Enrich one artist (full info) ───────────────────────────────────

def enrich_artist(artist_name: str, discovery_tag: str, seen_artists: set, tag_queue: deque, seen_tags: set) -> dict | None:
    """
    Full artist.getInfo call.
    Adds any new tags found to the tag queue for further snowballing.
    Returns a row dict or None if below listener threshold.
    """
    data = lfm_get("artist.getInfo", {"artist": artist_name, "autocorrect": 1})
    throttle()
    if not data:
        return None

    artist = data.get("artist", {})
    stats  = artist.get("stats", {})
    listeners = int(stats.get("listeners", 0) or 0)

    if listeners < MIN_LISTENERS:
        return None

    # Extract tags from full info
    raw_tags = artist.get("tags", {}).get("tag", [])
    tag_names = [t["name"].strip().lower() for t in raw_tags if t.get("name")]

    # Feed new tags back into the queue — this is the snowball
    for tag in tag_names:
        if tag and tag not in seen_tags and len(seen_tags) < MAX_TAGS:
            seen_tags.add(tag)
            tag_queue.append(tag)

    bio = artist.get("bio", {}).get("summary", "").strip()
    # Strip the Last.fm "Read more" anchor that always appears
    if "<a href" in bio:
        bio = bio[:bio.index("<a href")].strip()

    return {
        "run_id":        RUN_ID,
        "run_ts":        RUN_TS,
        "artist_name":   artist.get("name", artist_name).strip(),
        "mbid":          artist.get("mbid", ""),
        "listeners":     listeners,
        "playcount":     int(stats.get("playcount", 0) or 0),
        "tags":          json.dumps(tag_names),
        "bio_summary":   bio[:2000],   # cap for Snowflake VARCHAR comfort
        "source":        "snowball",
        "discovery_tag": discovery_tag,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run():
    log.info(f"=== Run ID: {RUN_ID} | Started: {RUN_TS} ===")

    seen_artists: set = set()
    seen_tags:    set = set()
    artist_queue: deque = deque()
    tag_queue:    deque = deque()
    all_rows:     list  = []

    errors = 0
    start  = time.time()

    # ── Seed both queues ──────────────────────────────────────────────────────
    seed_tags_from_top_tags(tag_queue, seen_tags)
    seed_rows = seed_artists_from_chart(artist_queue, seen_artists)
    all_rows.extend(seed_rows)

    log.info(f"Seeds: {len(artist_queue)} artists, {len(tag_queue)} tags in queue")

    # ── Snowball loop ─────────────────────────────────────────────────────────
    # We alternate between draining the artist queue and the tag queue so
    # neither starves.  The tag queue keeps injecting new artists; the artist
    # enrichment keeps injecting new tags.

    while (artist_queue or tag_queue) and len(seen_artists) < MAX_ARTISTS:

        # ── Process one artist off the queue ──────────────────────────────────
        if artist_queue:
            artist_name = artist_queue.popleft()
            row = enrich_artist(artist_name, "", seen_artists, tag_queue, seen_tags)
            if row:
                all_rows.append(row)

            if len(all_rows) % 500 == 0:
                log.info(
                    f"Progress — artists processed: {len(seen_artists):,} | "
                    f"rows: {len(all_rows):,} | "
                    f"tags discovered: {len(seen_tags):,} | "
                    f"artist_q: {len(artist_queue):,} | "
                    f"tag_q: {len(tag_queue):,}"
                )

        # ── Process one tag off the queue, expand into more artists ───────────
        if tag_queue and len(seen_artists) < MAX_ARTISTS:
            tag_name = tag_queue.popleft()
            tag_artists = get_tag_artists(tag_name)

            for a in tag_artists:
                name = (a.get("name") or "").strip()
                if not name:
                    continue
                key = name.lower()
                if key in seen_artists or len(seen_artists) >= MAX_ARTISTS:
                    continue
                seen_artists.add(key)
                artist_queue.append(name)

    elapsed = round(time.time() - start, 1)
    log.info(
        f"Snowball complete — {len(all_rows):,} rows | "
        f"{len(seen_tags):,} tags | "
        f"{elapsed}s elapsed | errors: {errors}"
    )

    # ── Write artists_raw ─────────────────────────────────────────────────────
    if all_rows:
        df = pd.DataFrame(all_rows)
        # Ensure consistent column order / types
        df["run_ts"]    = pd.to_datetime(df["run_ts"], utc=True)
        df["listeners"] = df["listeners"].astype(int)
        df["playcount"] = df["playcount"].astype(int)
        df = df.drop_duplicates(subset=["run_id", "artist_name"])

        with artists_raw_ds.get_writer() as writer:
            writer.write_dataframe(df)
        log.info(f"Wrote {len(df):,} rows to artists_raw")
    else:
        log.warning("No rows to write — check API key and network connectivity")

    # ── Write pipeline_runs ───────────────────────────────────────────────────
    run_meta = pd.DataFrame([{
        "run_id":           RUN_ID,
        "run_ts":           RUN_TS,
        "status":           "success" if all_rows else "empty",
        "rows_written":     len(all_rows),
        "artists_seen":     len(seen_artists),
        "tags_discovered":  len(seen_tags),
        "elapsed_seconds":  elapsed,
        "errors":           errors,
        "config": json.dumps({
            "MAX_ARTISTS":       MAX_ARTISTS,
            "MAX_TAGS":          MAX_TAGS,
            "MIN_LISTENERS":     MIN_LISTENERS,
            "TOP_ARTISTS_PAGES": TOP_ARTISTS_PAGES,
            "TOP_TAGS_PAGES":    TOP_TAGS_PAGES,
            "ARTISTS_PER_TAG":   ARTISTS_PER_TAG,
            "TAGS_PER_ARTIST":   TAGS_PER_ARTIST,
        }),
    }])

    with pipeline_runs_ds.get_writer() as writer:
        writer.write_dataframe(run_meta)
    log.info("Wrote pipeline_run metadata row")

    return len(all_rows)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run()
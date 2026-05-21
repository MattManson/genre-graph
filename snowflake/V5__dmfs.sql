-- ============================================================
-- V5__dmfs.sql
-- Snowflake Data Metric Functions for source and transformed layers.
-- Run as ACCOUNTADMIN.
--
-- Schedule is set at the table level before DMFs are attached.
-- All DMFs on a table share the same schedule.
--
-- Note: Freshness checking is handled in the DSS Scenario via
-- PIPELINE_RUNS, not here. DMFs cover content assertions only.
--
-- Note: ARTISTS_RAW is append-only by design — intra-run duplicates
-- are expected as the same artist can appear across multiple runs.
-- Deduplication happens in ARTISTS_CLEAN via ROW_NUMBER(), so
-- duplicate checking belongs there, not on the raw table.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE MUSIC_GENRES;

-- ============================================================
-- DMFs on ARTISTS_RAW (source layer)
-- ============================================================

-- ── 1. Null rate on ARTIST_NAME ───────────────────────────────────────────────
-- Returns count of rows where ARTIST_NAME is null or blank.
-- Expected: 0 at all times.
CREATE OR REPLACE DATA METRIC FUNCTION MUSIC_GENRES.RAW.DMF_NULL_ARTIST_NAME(
    ARG_T TABLE(ARTIST_NAME VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE ARTIST_NAME IS NULL
       OR TRIM(ARTIST_NAME) = ''
$$;

-- ── Set schedule on ARTISTS_RAW, then attach DMF ─────────────────────────────
ALTER TABLE MUSIC_GENRES.RAW.ARTISTS_RAW
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

ALTER TABLE MUSIC_GENRES.RAW.ARTISTS_RAW
    ADD DATA METRIC FUNCTION MUSIC_GENRES.RAW.DMF_NULL_ARTIST_NAME
    ON (ARTIST_NAME);


-- ============================================================
-- DMFs on ARTISTS_CLEAN (transformed layer)
-- ============================================================

-- ── 2. Duplicate artists after deduplication ─────────────────────────────────
-- Returns count of artist names appearing more than once in ARTISTS_CLEAN.
-- Expected: 0. The ROW_NUMBER() partition in the dynamic table definition
-- should guarantee uniqueness — any value >0 means the dedup logic has a gap.
CREATE OR REPLACE DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_DUPLICATE_ARTISTS(
    ARG_T TABLE(ARTIST_NAME VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM (
        SELECT UPPER(ARTIST_NAME)
        FROM ARG_T
        GROUP BY UPPER(ARTIST_NAME)
        HAVING COUNT(*) > 1
    )
$$;

-- ── 3. Non-chart-seed artists with TAG_COUNT = 0 ─────────────────────────────
-- Returns count of snowball/refresh artists that slipped through with no tags.
-- Expected: 0. If >0, the tag filtering logic in the dynamic table has a gap.
CREATE OR REPLACE DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_UNTAGGED_NON_SEEDS(
    ARG_T TABLE(TAG_COUNT NUMBER, SOURCE VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE TAG_COUNT = 0
      AND SOURCE != 'chart_seed'
$$;

-- ── 4. POPULARITY_TIER distribution — niche artist rate ──────────────────────
-- Returns the percentage of artists in the 'niche' tier (listeners < 10k).
-- Expected: < 80. A very high niche rate suggests the listener filter
-- isn't working or the dataset is skewed toward obscure artists.
CREATE OR REPLACE DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_NICHE_RATE(
    ARG_T TABLE(POPULARITY_TIER VARCHAR)
)
RETURNS NUMBER
AS
$$
    SELECT ROUND(
        100.0 * COUNT(CASE WHEN POPULARITY_TIER = 'niche' THEN 1 END)
        / NULLIF(COUNT(*), 0)
    , 1)
    FROM ARG_T
$$;

-- ── Set schedule on ARTISTS_CLEAN, then attach all three DMFs ────────────────
ALTER TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

ALTER TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN
    ADD DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_DUPLICATE_ARTISTS
    ON (ARTIST_NAME);

ALTER TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN
    ADD DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_UNTAGGED_NON_SEEDS
    ON (TAG_COUNT, SOURCE);

ALTER TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN
    ADD DATA METRIC FUNCTION MUSIC_GENRES.ANALYTICS.DMF_NICHE_RATE
    ON (POPULARITY_TIER);

GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DSS_ROLE;GRANT OPERATE ON DYNAMIC TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN TO ROLE DSS_ROLE;
GRANT OPERATE ON DYNAMIC TABLE MUSIC_GENRES.ANALYTICS.TAG_CO_OCCURRENCE TO ROLE DSS_ROLE;

GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DSS_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORE_VIEWER TO ROLE DSS_ROLE;


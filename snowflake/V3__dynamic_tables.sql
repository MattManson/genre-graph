-- ============================================================
-- V3__dynamic_tables.sql
-- ARTISTS_CLEAN, TAG_CO_OCCURRENCE, and row access policy.
-- Run as ACCOUNTADMIN.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── Dynamic Table 1: ARTISTS_CLEAN ───────────────────────────────────────────
-- Deduped, flattened, quality-filtered view of ARTISTS_RAW.
-- chart_seed rows are kept even with no tags (enriched on later runs).
CREATE OR REPLACE DYNAMIC TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE  = DSS_WH
AS
WITH latest_per_artist AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY UPPER(ARTIST_NAME)
               ORDER BY RUN_TS DESC
           ) AS rn
    FROM MUSIC_GENRES.RAW.ARTISTS_RAW
    WHERE LISTENERS   >= 100
      AND ARTIST_NAME IS NOT NULL
      AND TRIM(ARTIST_NAME) != ''
),
parsed AS (
    SELECT
        ARTIST_NAME,
        MBID,
        LISTENERS,
        PLAYCOUNT,
        BIO_SUMMARY,
        SOURCE,
        DISCOVERY_TAG,
        RUN_ID,
        RUN_TS,
        TRY_PARSE_JSON(TAGS) AS TAGS_PARSED
    FROM latest_per_artist
    WHERE rn = 1
)
SELECT
    ARTIST_NAME,
    MBID,
    LISTENERS,
    PLAYCOUNT,
    BIO_SUMMARY,
    SOURCE,
    DISCOVERY_TAG,
    RUN_ID,
    RUN_TS,
    TAGS_PARSED              AS TAGS,
    ARRAY_SIZE(TAGS_PARSED)  AS TAG_COUNT,
    CASE
        WHEN LISTENERS >= 1000000 THEN 'mega'
        WHEN LISTENERS >= 100000  THEN 'major'
        WHEN LISTENERS >= 10000   THEN 'mid'
        ELSE                           'niche'
    END AS POPULARITY_TIER
FROM parsed
WHERE SOURCE = 'chart_seed'
   OR ARRAY_SIZE(TAGS_PARSED) > 0;

GRANT ALL ON TABLE MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN TO ROLE DSS_ROLE;

-- ── Dynamic Table 2: TAG_CO_OCCURRENCE ───────────────────────────────────────
-- Which tags appear together most often across artists.
-- Only pairs co-occurring on 3+ artists are kept.
CREATE OR REPLACE DYNAMIC TABLE MUSIC_GENRES.ANALYTICS.TAG_CO_OCCURRENCE
    TARGET_LAG = '1 hour'
    WAREHOUSE  = DSS_WH
AS
WITH artist_tags AS (
    SELECT
        ARTIST_NAME,
        LISTENERS,
        POPULARITY_TIER,
        f.value::STRING AS TAG
    FROM MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN,
         LATERAL FLATTEN(input => TAGS) f
),
tag_pairs AS (
    SELECT
        a.TAG        AS TAG_A,
        b.TAG        AS TAG_B,
        a.ARTIST_NAME,
        a.LISTENERS
    FROM artist_tags a
    JOIN artist_tags b
        ON  a.ARTIST_NAME = b.ARTIST_NAME
        AND a.TAG < b.TAG
)
SELECT
    TAG_A,
    TAG_B,
    COUNT(DISTINCT ARTIST_NAME)  AS ARTIST_COUNT,
    SUM(LISTENERS)               AS TOTAL_LISTENERS,
    AVG(LISTENERS)               AS AVG_LISTENERS
FROM tag_pairs
GROUP BY TAG_A, TAG_B
HAVING COUNT(DISTINCT ARTIST_NAME) >= 3;

GRANT ALL ON TABLE MUSIC_GENRES.ANALYTICS.TAG_CO_OCCURRENCE TO ROLE DSS_ROLE;

-- ── Row Access Policy on TAG_CO_OCCURRENCE ────────────────────────────────────
-- ACCOUNTADMIN and DSS_ROLE see everything.
-- All other roles see only well-established pairs (ARTIST_COUNT >= 10).
CREATE OR REPLACE ROW ACCESS POLICY MUSIC_GENRES.ANALYTICS.TAG_VISIBILITY_POLICY
    AS (ARTIST_COUNT NUMBER) RETURNS BOOLEAN ->
        CASE
            WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DSS_ROLE') THEN TRUE
            WHEN ARTIST_COUNT >= 10 THEN TRUE
            ELSE FALSE
        END;

ALTER DYNAMIC TABLE MUSIC_GENRES.ANALYTICS.TAG_CO_OCCURRENCE
    ADD ROW ACCESS POLICY MUSIC_GENRES.ANALYTICS.TAG_VISIBILITY_POLICY
    ON (ARTIST_COUNT);

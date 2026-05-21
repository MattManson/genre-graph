-- ============================================================
-- V4__artist_embeddings.sql
-- ARTIST_EMBEDDINGS table + one-time bootstrap INSERT.
--
-- Design notes:
--   - Kept separate from ARTISTS_CLEAN (a dynamic table) because
--     VECTOR columns are not supported in dynamic tables on free tier
--   - Bio content changes ~1-2x per year per artist; decoupling
--     controls Cortex compute cost
--   - Incremental refresh (MERGE) is run on a weekly schedule via
--     DSS Scenario, not as part of this deployment script
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE TABLE IF NOT EXISTS MUSIC_GENRES.ANALYTICS.ARTIST_EMBEDDINGS (
    ARTIST_NAME     VARCHAR         NOT NULL,
    BIO_EMBEDDING   VECTOR(FLOAT, 768),
    BIO_HASH        VARCHAR,        -- SHA2 of BIO_SUMMARY; used to detect changes cheaply
    EMBEDDED_AT     TIMESTAMP_TZ,
    SOURCE_RUN_ID   VARCHAR         -- RUN_ID from ARTISTS_CLEAN at embed time
);

GRANT ALL ON TABLE MUSIC_GENRES.ANALYTICS.ARTIST_EMBEDDINGS TO ROLE DSS_ROLE;

-- ── Initial load (run once after first ingestion completes) ───────────────────
-- Embeds all artists with a non-empty bio not yet in the embeddings table.
-- Safe to re-run: LEFT JOIN / IS NULL guard prevents duplicate inserts.

-- Bootstrap INSERT requires Cortex (unavailable on trial accounts)
-- Uncomment on paid tier:
/*
INSERT INTO MUSIC_GENRES.ANALYTICS.ARTIST_EMBEDDINGS (
    ARTIST_NAME,
    BIO_EMBEDDING,
    BIO_HASH,
    EMBEDDED_AT,
    SOURCE_RUN_ID
)
SELECT
    ac.ARTIST_NAME,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', ac.BIO_SUMMARY),
    SHA2(ac.BIO_SUMMARY),
    CURRENT_TIMESTAMP(),
    ac.RUN_ID
FROM MUSIC_GENRES.ANALYTICS.ARTISTS_CLEAN ac
LEFT JOIN MUSIC_GENRES.ANALYTICS.ARTIST_EMBEDDINGS ae
    ON ac.ARTIST_NAME = ae.ARTIST_NAME
WHERE ae.ARTIST_NAME IS NULL
  AND ac.BIO_SUMMARY IS NOT NULL
  AND TRIM(ac.BIO_SUMMARY) != '';
*/
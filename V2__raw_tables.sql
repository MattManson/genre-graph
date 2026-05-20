-- ============================================================
-- V2__raw_tables.sql
-- Raw ingestion tables written to by the DSS pipeline.
-- Run as ACCOUNTADMIN.
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE TABLE IF NOT EXISTS MUSIC_GENRES.RAW.ARTISTS_RAW (
    RUN_ID          VARCHAR,
    RUN_TS          TIMESTAMP_TZ,
    ARTIST_NAME     VARCHAR,
    MBID            VARCHAR,
    LISTENERS       BIGINT,
    PLAYCOUNT       BIGINT,
    TAGS            VARCHAR,        -- JSON array of tag strings
    BIO_SUMMARY     VARCHAR(2000),
    SOURCE          VARCHAR,        -- 'chart_seed' | 'snowball' | 'refresh'
    DISCOVERY_TAG   VARCHAR
);

CREATE TABLE IF NOT EXISTS MUSIC_GENRES.RAW.PIPELINE_RUNS (
    RUN_ID              VARCHAR,
    RUN_TS              TIMESTAMP_TZ,
    STATUS              VARCHAR,
    ROWS_WRITTEN        BIGINT,
    NEW_ARTISTS_ADDED   BIGINT,
    UPDATES_WRITTEN     BIGINT,
    ARTISTS_SEEN_TOTAL  BIGINT,
    TAGS_DISCOVERED     BIGINT,
    ELAPSED_SECONDS     BIGINT,
    CONFIG              VARCHAR     -- JSON blob of run config
);

GRANT ALL ON TABLE MUSIC_GENRES.RAW.ARTISTS_RAW   TO ROLE DSS_ROLE;
GRANT ALL ON TABLE MUSIC_GENRES.RAW.PIPELINE_RUNS TO ROLE DSS_ROLE;

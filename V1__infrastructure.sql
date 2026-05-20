-- ============================================================
-- V1__infrastructure.sql
-- Warehouse, role, user, database, schemas, and grants.
-- Run as ACCOUNTADMIN.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── Warehouse ─────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS DSS_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

-- ── Role & User ───────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS DSS_ROLE;

CREATE USER IF NOT EXISTS DSS_USER
    PASSWORD          = '<your_strong_password>'   -- set via Snowflake UI or secrets manager
    DEFAULT_ROLE      = DSS_ROLE
    DEFAULT_WAREHOUSE = DSS_WH;

GRANT ROLE DSS_ROLE TO USER DSS_USER;
GRANT USAGE ON WAREHOUSE DSS_WH TO ROLE DSS_ROLE;

-- ── Database & Schemas ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS MUSIC_GENRES;

CREATE SCHEMA IF NOT EXISTS MUSIC_GENRES.RAW;
CREATE SCHEMA IF NOT EXISTS MUSIC_GENRES.ANALYTICS;

-- ── Grants ────────────────────────────────────────────────────────────────────
GRANT ALL ON DATABASE MUSIC_GENRES                          TO ROLE DSS_ROLE;
GRANT ALL ON SCHEMA   MUSIC_GENRES.RAW                      TO ROLE DSS_ROLE;
GRANT ALL ON SCHEMA   MUSIC_GENRES.ANALYTICS                TO ROLE DSS_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA MUSIC_GENRES.RAW       TO ROLE DSS_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA MUSIC_GENRES.ANALYTICS TO ROLE DSS_ROLE;

# genre-graph

A music genre aggregation pipeline built on Last.fm, Dataiku DSS, and Snowflake.

Ingests artist and tag data via a snowball pipeline, transforms it into clean analytical tables, and enables genre co-occurrence analysis and artist similarity search via Snowflake Cortex embeddings.

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Dataiku DSS (Python recipe) |
| Storage | Snowflake |
| Transformation | Snowflake Dynamic Tables |
| Embeddings | Snowflake Cortex (`EMBED_TEXT_768`) |
| Orchestration | Dataiku DSS Scenario |
| CI/CD | GitHub Actions + SnowflakeCLI |

---

## Snowflake Objects

### RAW schema
- `ARTISTS_RAW` — append-only artist rows written per pipeline run
- `PIPELINE_RUNS` — one row per DSS scenario execution with run metadata

### ANALYTICS schema
- `ARTISTS_CLEAN` — dynamic table; deduped, tag-parsed, tiered view of `ARTISTS_RAW`
- `TAG_CO_OCCURRENCE` — dynamic table; tag pair co-occurrence counts across artists
- `ARTIST_EMBEDDINGS` — static table; Cortex bio embeddings for similarity search

---

## Deployment

Snowflake objects are deployed in order via GitHub Actions on push to `main`.

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `xy12345.eu-west-1`) |
| `SNOWFLAKE_USER` | `DSS_USER` or any ACCOUNTADMIN-privileged user |
| `SNOWFLAKE_PASSWORD` | Corresponding password |

### File order

```
snowflake/
  V1__infrastructure.sql      -- warehouse, role, user, database, schemas, grants
  V2__raw_tables.sql          -- ARTISTS_RAW, PIPELINE_RUNS
  V3__dynamic_tables.sql      -- ARTISTS_CLEAN, TAG_CO_OCCURRENCE, row access policy
  V4__artist_embeddings.sql   -- ARTIST_EMBEDDINGS table + one-time bootstrap INSERT
  V5__dmfs.sql                -- Data Metric Functions on ARTISTS_RAW and ARTISTS_CLEAN
```

---

## Orchestration

The DSS scenario `genre_graph_orchestration` runs nightly at 02:00 UTC with four steps:

1. **Ingest from Last.fm** — runs `lastfm_snowball_pipeline`, appends to `ARTISTS_RAW` and logs to `PIPELINE_RUNS`
2. **Check pipeline run success** — queries `PIPELINE_RUNS`, aborts if no successful run in last 24h
3. **Refresh dynamic tables** — force-refreshes `ARTISTS_CLEAN` and `TAG_CO_OCCURRENCE`
4. **Check DMF results** — queries `SNOWFLAKE.CORE.DATA_METRIC_FUNCTION_RESULTS`; soft-fails on trial accounts where this schema is unavailable

---

## Notes

- `V4__artist_embeddings.sql` includes an initial load `INSERT` that is safe to re-run (guarded by a `LEFT JOIN / IS NULL` check). In production, incremental updates are handled by a `MERGE` statement executed via DSS Scenario.
- The row access policy on `TAG_CO_OCCURRENCE` restricts non-privileged roles to pairs with `ARTIST_COUNT >= 10`.
- `ARTIST_EMBEDDINGS` is intentionally decoupled from `ARTISTS_CLEAN` — bio content changes infrequently and Cortex compute is non-trivial; weekly refresh is sufficient.
- Cortex AI functions (`EMBED_TEXT_768`) are unavailable on Snowflake free trial. The table structure, MERGE logic, and similarity search query are production-ready in `V4__artist_embeddings.sql`.
- `SNOWFLAKE.CORE.DATA_METRIC_FUNCTION_RESULTS` is unavailable on free trial despite not being listed in Snowflake's documented trial limitations. DMF definitions are correct; results are queryable on paid tiers.
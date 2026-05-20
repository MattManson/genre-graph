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
```

---

## Notes

- `V4__artist_embeddings.sql` includes an initial load `INSERT` that is safe to re-run (guarded by a `LEFT JOIN / IS NULL` check). In production, incremental updates are handled by a `MERGE` statement executed weekly via DSS Scenario.
- The row access policy on `TAG_CO_OCCURRENCE` restricts non-privileged roles to pairs with `ARTIST_COUNT >= 10`.
- `ARTIST_EMBEDDINGS` is intentionally decoupled from `ARTISTS_CLEAN` — bio content changes infrequently and Cortex compute is non-trivial; weekly refresh is sufficient.

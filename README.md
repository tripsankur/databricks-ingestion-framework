# databricks-ingestion-framework

Static, versioned ingestion framework for the Pipeline Factory system (ADR-008/009).
**One engine, N sources** — per-source behavior is 100% metadata (`ctl.dataflow_spec`
rows written by Pipeline Factory); this repo contains the only executable code in
the data path.

## What runs where

| Asset | Kind | Created by |
|---|---|---|
| `engine/ingest_pipeline.py` | Generic SDP ETL engine (bronze→silver stitch + DQ from metadata) | referenced by app-created ETL pipelines (`slvr_{source}_etl`) via libraries glob |
| `engine/recon_job.py` | Generic reconciliation (entry point + key/row/attribute compare + record diffs) | `pf-framework-recon` job (this bundle) / workflow task |
| `adapters/sfdc_describe.py` | Salesforce schema discovery fallback (describe-only) | `pf-framework-sfdc-describe` job (this bundle) |
| Ingestion source→bronze | **Lakeflow Connect managed ingestion pipelines** | Pipeline Factory (not this repo) |

Per-source assets (ingestion pipeline `brnz_{source}_ingest`, ETL pipeline
`slvr_{source}_etl`, workflow `{source}_workflow`) are provisioned by the app
against the deployed engine path — this bundle intentionally registers none.

## Patterns

The prescriptive catalog (which bronze pattern per source type, workflow shapes,
per-pattern metadata requirements) lives in the app repo:
[pipeline-factory/docs/FRAMEWORK_PATTERNS.md](https://github.com/tripsankur/pipeline-factory/blob/main/docs/FRAMEWORK_PATTERNS.md).

## Metadata contract

`{catalog}.ctl.dataflow_spec` — one row per entity: `dataflow_id`, `dataflow_group`
(=source), `entity`, `source_format`, `source_details`, `target_details`
(bronze/silver/crosswalk tables), `select_columns`, `crosswalk_keys` (JSON),
`column_transforms` (JSON incl. per-column `compare`), `data_quality_expectations`
(JSON: expect / expect_or_drop / expect_or_fail), `table_properties`, `cluster_by`,
**`is_active`** (tombstone — rows are never deleted; ADR-010), `framework_min_version`.

The engine reads only active rows, logs every tombstoned exclusion, and fails fast
when `framework_min_version` exceeds `ENGINE_VERSION`.

## Deploy

```bash
databricks bundle validate
databricks bundle deploy -t dev
```

Engine files land under the bundle's workspace file path; Pipeline Factory points
`PF_FRAMEWORK_ENGINE_PATH` there.

## Develop

```bash
pip install pytest ruff
ruff check .
pytest
```

Versioning: semver in `VERSION` (+ `ENGINE_VERSION` in `engine/spec_reader.py`).
Breaking metadata-schema changes bump MAJOR and must raise `framework_min_version`
written by the app.

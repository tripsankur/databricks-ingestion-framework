# Changelog

## 1.0.0 (2026-07-06)
- Initial framework: generic SDP ETL engine (metadata-driven factory, tombstone-aware), generic recon job (entry point, key/row/attribute compare, record diffs with values), Salesforce describe-only discovery adapter, DAB bundle (pf-framework-recon, pf-framework-sfdc-describe), CI.

## 1.1.0 (2026-07-14)
- run_logger.py: per-run ingestion telemetry (ctl.ingestion_runs) + observed
  watermarks (ctl.watermarks) — runs as a workflow task on every execution (ADR-011).
- recon_job.py: --recon-id/--run-id now optional (self-generated / {{job.run_id}})
  so recon rides the scheduled workflow.

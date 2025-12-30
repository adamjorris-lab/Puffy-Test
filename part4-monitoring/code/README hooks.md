
# Hooks (Production Integration Stubs)

These are lightweight integration points you'd wire into Airflow/Dagster/dbt Cloud:

- `quarantine_snapshot`: copies the produced warehouse outputs to `quarantine/` when monitoring finds ERRORs.
- `rollback`: in a real warehouse, this would:
  - swap views to last-known-good partition
  - stop downstream dashboard refresh
  - notify stakeholders

This repo keeps hooks intentionally simple and file-based for the skills test.

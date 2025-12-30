# Puffy Skills Test — Part 1: Incoming Data Quality Framework

This repo contains a lightweight data quality validation framework for Puffy's raw event exports.

## Structure
- `data/raw/` — input CSV partitions (one per day)
- `dq_framework/run_validation.py` — validation runner
- `reports/` — generated outputs (Markdown + JSON)
- `docs/` — framework documentation & data dictionary

## Quickstart

```bash
python dq_framework/run_validation.py --input_dir data/raw --output_dir reports
```

## What this checks
See `docs/incoming_data_quality_framework.md`.

## Notes
- The validator canonicalizes `client_id` across known aliases (`client_id`, `clientId`).
- It treats `checkout_completed` as the purchase event in this dataset (based on presence of `transaction_id` and `revenue`).


## Productionization notes

This repo includes a lightweight Python validator that can run:
- Locally (dev)
- In CI (GitHub Actions workflow at `.github/workflows/data_quality.yml`)
- As a scheduled job in orchestration (Airflow/Dagster/etc.) with a **fail-fast** non-zero exit when `--fail_on_error` is provided.

### Config-driven rules
Edit `config/dq_rules.yml` to tune thresholds (null rates, duplicate transaction IDs, zero/negative revenue rates, required columns, and canonical column aliases).

Example:
```bash
python dq_framework/run_validation.py --input_dir data/raw --output_dir reports --rules config/dq_rules.yml --fail_on_error
```

### dbt-style integration (suggested)
In a production warehouse, the same checks can be implemented as:
- **Schema tests** (accepted values for `event_name`, not-null columns, unique transaction IDs)
- **Freshness/volume checks** (partition row counts and event mix drift)
- **Revenue sanity checks** (non-negative, expected distributions)

The CI workflow provides a template for gating merges on data quality.

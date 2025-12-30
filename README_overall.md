
# Puffy Analytics Skills Test (Parts 1–4)

This repository contains my submission for the Puffy **Head of Data Infrastructure & Analytics** skills test.  
It includes: (1) incoming data quality validation, (2) transformation/sessionization + attribution, (3) executive analysis with supporting charts/statistics, and (4) daily production monitoring.

---

## Repository Structure

```
/part1-data-quality/
  /code/
  documentation.md
  README.md

/part2-transformation/
  /code/
  architecture-diagram.png
  documentation.md
  README.md

/part3-analysis/
  executive-summary.pdf
  (optional) supporting-analysis/
  README.md

/part4-monitoring/
  /code/
  documentation.md
  README.md

README.md  (this file)
```

---

## Prerequisites (All Parts)

- Python **3.9+**
- `pip`
- Recommended: a virtual environment per repo

Create and activate a venv (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

---

## Part 1 — Incoming Data Quality Framework

**Goal:** Validate raw event data before it enters production analytics.

### Install

```bash
cd part1-data-quality
pip install -r requirements.txt
```

### Run

```bash
python code/dq_framework/run_checks.py   --input_dir ../data/raw_events   --rules code/config/dq_rules.yml   --output_dir reports   --fail_on_error
```

### Outputs

```
part1-data-quality/reports/
  dq_report.md
  dq_report.json
```

Notes:
- Use `--fail_on_error` in CI/orchestration so bad data does not move downstream.
- All thresholds are configurable in the rules YAML.

---

## Part 2 — Transformation Pipeline (Sessionization + Attribution)

**Goal:** Transform validated events into analytics-ready tables:
- `fct_sessions`
- `fct_orders`
- `fct_attribution` (first-click + last-click, **7-day lookback**)

### Install

```bash
cd ../part2-transformation
pip install -r requirements.txt
```

### Run

```bash
python code/pipeline/run_pipeline.py   --input_dir ../data/raw_events   --output_dir warehouse
```

### Outputs

```
part2-transformation/warehouse/
  fct_sessions.csv
  fct_orders.csv
  fct_attribution.csv
```

### Validation (Optional)

```bash
pytest -q
```

---

## Part 3 — Business Analysis (Executive Summary + Supporting Stats)

**Goal:** Communicate business performance and marketing effectiveness clearly to leadership.

### Primary Deliverable
- `part3-analysis/executive-summary.pdf` (1–2 pages)

### Optional Supporting Analysis (Charts + Code)

If included:

```bash
cd ../part3-analysis
pip install -r supporting-analysis/code/requirements.txt
python supporting-analysis/code/supporting_analysis.py
```

If included (advanced statistics pack):

```bash
pip install -r supporting-analysis/part3-advanced-statistics/code/requirements.txt
python supporting-analysis/part3-advanced-statistics/code/advanced_models.py
```

These scripts expect Part 2 outputs available locally (typically copied into a local `warehouse/` directory).

---

## Part 4 — Production Monitoring

**Goal:** Daily monitoring to ensure dashboards and marketing decisions are based on accurate data.

Monitors:
- Freshness & completeness (SLA)
- Revenue integrity (duplicates, negatives, zero spikes)
- Funnel sanity (conversion)
- Marketing mix drift (channel share shifts)
- Trend anomalies (z-score + percent change)

### Install

```bash
cd ../part4-monitoring
pip install -r code/requirements.txt
```

### Run (Local)

```bash
python code/monitoring/run_monitoring.py   --warehouse_dir ../part2-transformation/warehouse   --rules code/config/monitoring_rules.yml   --output_dir reports   --fail_on_error
```

### Run (Orchestrator / Scheduler Mode)

Pass the partition you expect the pipeline to have produced:

```bash
python code/monitoring/run_monitoring.py   --warehouse_dir ../part2-transformation/warehouse   --expected_run_date 2025-03-08   --fail_on_error
```

### Outputs

```
part4-monitoring/reports/
  monitoring_report.md
  monitoring_report.json
  quarantine_path.txt   (only if quarantined)
```

If configured, ERROR findings snapshot outputs to:

```
part4-monitoring/quarantine/
  warehouse_quarantine_<timestamp>/
```

---

## Recommended End-to-End Run Order

In production, the daily run looks like:

1. **Part 1:** Validate incoming raw event exports  
2. **Part 2:** Build sessions, orders, and attribution tables  
3. **Part 4:** Monitor freshness, revenue integrity, funnel sanity, and channel mix drift  
4. **Part 3:** (not daily) executive reporting / periodic business analysis

---

## Notes on Interpretation (Attribution vs Incrementality)

Attribution assigns credit across touchpoints; it is not the same as incrementality.  
For causal budget decisions, follow-up measurement should include incrementality testing (geo holdouts, suppression tests, branded search lift).

---

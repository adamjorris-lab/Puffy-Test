
# Part 4 — Production Monitoring
## How to Run

This module runs daily after transformations to ensure the warehouse outputs are accurate and safe for dashboards and marketing decisions.

It monitors:
- Freshness & completeness (SLA)
- Revenue integrity (duplicates, negatives, zero-revenue spikes)
- Funnel sanity (conversion)
- Marketing mix drift (channel share shifts)
- Trend anomalies (z-score + percent change vs rolling baseline)

---

## Prerequisites
- Python **3.9+**
- `pip` installed

---

## Installation

From the `part4-monitoring/` directory:

```bash
pip install -r code/requirements.txt
```

---

## Run Monitoring (Local)

```bash
python code/monitoring/run_monitoring.py   --warehouse_dir warehouse   --rules code/config/monitoring_rules.yml   --output_dir reports   --fail_on_error
```

If any **ERROR** findings occur, the command exits non-zero (useful for CI/orchestrators).

---

## Scheduler / Orchestrator Use

To align freshness checks with the partition you expect the pipeline to produce, pass an explicit expected date:

```bash
python code/monitoring/run_monitoring.py   --warehouse_dir warehouse   --expected_run_date 2025-03-08   --fail_on_error
```

---

## Outputs

```
reports/
  monitoring_report.md
  monitoring_report.json
  quarantine_path.txt       # only when quarantined
```

If configured (`quarantine_on_error: true`), ERROR findings also produce a snapshot:

```
quarantine/
  warehouse_quarantine_<timestamp>/
    fct_sessions.csv
    fct_orders.csv
    fct_attribution.csv
    REASON.txt
```

---

## Configuration

Tune thresholds without code changes via:

```
code/config/monitoring_rules.yml
```

Key knobs:
- freshness: expected lag, allowed missing days, volume floors
- hard rules: duplicate txn threshold, null-rate limits
- anomaly detection: z-score + percent-change thresholds, channel share drift

---

## Production Pattern

Run Part 4 immediately after Part 2 and before BI refresh:

```
Part 2 Transform → Part 4 Monitor → Dashboard Refresh
```

If ERROR:
- quarantine snapshot for rollback
- halt downstream refresh
- notify stakeholders (routing is represented as a practical stub)


# Part 1 — Incoming Data Quality Framework
## How to Run

This module validates raw event data **before** it enters production analytics. It is designed to be run locally, in CI, or as part of an orchestration workflow (Airflow, dbt Cloud, Dagster, etc.).

---

## Prerequisites

- Python **3.9+**
- `pip` installed
- Raw event CSV files available locally

---

## Installation

From the `part1-data-quality/` directory:

```bash
pip install -r requirements.txt
```

If you are using a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running the Data Quality Checks

### Basic Run (Local)

```bash
python dq_framework/run_checks.py   --input_dir path/to/raw_events/   --rules config/dq_rules.yml   --output_dir reports/
```

This will:
- Load all raw event files in `input_dir`
- Apply schema, null-rate, and business-rule checks
- Write a human-readable report to `reports/`

---

### Fail the Pipeline on Errors (Production Mode)

```bash
python dq_framework/run_checks.py   --input_dir path/to/raw_events/   --rules config/dq_rules.yml   --output_dir reports/   --fail_on_error
```

If any **ERROR**-severity checks fail, the process exits with a non-zero status code, allowing CI or orchestration tools to stop downstream jobs.

---

## Configuration

All thresholds and checks are defined in:

```
config/dq_rules.yml
```

Examples:
- Allowed null-rate by column
- Revenue sanity thresholds
- Required columns / schema guardrails

This allows tuning sensitivity without code changes.

---

## Output

After each run, the following files are produced:

```
reports/
├── dq_report.md      # Human-readable summary
└── dq_report.json    # Machine-readable findings
```

---

## How This Runs in Production

Typical flow:

```
Raw Events
   ↓
Part 1: Data Quality Checks  ← (this module)
   ↓
Staging / Warehouse Load
   ↓
Downstream Transformations
```

If **ERROR** findings occur:
- The pipeline halts
- Bad data is prevented from entering analytics
- Engineers can investigate using the generated report

---

## Notes

- This framework is intentionally conservative: it prioritizes preventing incorrect revenue and attribution data over avoiding false positives.
- Thresholds should be revisited after observing several weeks of production behavior.


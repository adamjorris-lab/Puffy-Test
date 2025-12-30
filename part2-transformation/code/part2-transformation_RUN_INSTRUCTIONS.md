
# Part 2 — Transformation Pipeline
## How to Run

This module transforms validated raw events into analytics-ready tables, including:
- Sessionized behavior (`fct_sessions`)
- Orders (`fct_orders`)
- Attribution (`fct_attribution`) supporting **first-click** and **last-click** with a **7-day lookback**

---

## Prerequisites
- Python **3.9+**
- `pip` installed

---

## Installation

From the `part2-transformation/` directory:

```bash
pip install -r requirements.txt
```

Recommended (virtual env):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Inputs

This pipeline expects validated raw event exports (CSV) in a directory, typically produced after Part 1 passes.

Example input layout:

```
data/raw_events/
  events_YYYYMMDD.csv
  events_YYYYMMDD.csv
  ...
```

---

## Run the Pipeline (Local)

```bash
python pipeline/run_pipeline.py   --input_dir data/raw_events   --output_dir warehouse
```

The pipeline will:
1. Load all event partitions in `--input_dir`
2. Build sessions and users (sessionization rules defined in code/docs)
3. Build orders and join revenue fields
4. Generate attribution outputs (first-click + last-click, 7-day window)
5. Write outputs to `--output_dir`

---

## Outputs

Default output folder structure:

```
warehouse/
  fct_sessions.csv
  fct_orders.csv
  fct_attribution.csv
```

---

## Validation / Tests

If tests are included in the submission, run:

```bash
pytest -q
```

Recommended reconciliation checks:
- Sum of `orders.revenue` reconciles to purchase events (post-dedup rules)
- Order counts match distinct `transaction_id`
- Attribution revenue by model reconciles to total order revenue

---

## Configuration Notes

- Sessionization and attribution definitions are documented in `documentation.md`
- Lookback window for attribution is enforced at **7 days**
- For production, wire this step after Part 1 and before Part 4 monitoring

---

## Typical Production Flow

```
Raw Events
  ↓
Part 1: Data Quality Gate
  ↓
Part 2: Transform + Sessionize + Attribution  ← (this module)
  ↓
Warehouse / BI
  ↓
Part 4: Monitoring + Alerting
```

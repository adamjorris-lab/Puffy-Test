# Puffy Skills Test — Part 2: Transformation Pipeline

This repo contains a reference transformation layer that turns **validated raw events** into **analytics-ready tables** for:
1) **Behavior analytics** (sessions, devices, actions, sources)
2) **Marketing attribution** with a **7-day lookback** supporting **first-click** and **last-click**

> Note: In a real warehouse this would typically be implemented as **dbt + SQL** models.  
> For this skills test, I’m providing a maintainable **Python/pandas** implementation that produces the same star-schema-style outputs.

## Inputs
- `data/raw/events_YYYYMMDD.csv` (14 daily partitions)
- `docs/data_dictionary.docx` (provided dictionary)

## Outputs (written to `warehouse/`)
- `stg_events.csv` — standardized/enriched events
- `fct_sessions.csv` — sessionized user behavior
- `fct_orders.csv` — deduped orders from `checkout_completed`
- `fct_attribution.csv` — first-click + last-click attribution rows per order

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt

python pipeline/run_pipeline.py   --input_dir data/raw   --output_dir warehouse   --reports_dir reports
```

Run automated tests:

```bash
pytest -q
```

## Design highlights
- **Sessionization**: 30-minute inactivity rule (configurable via `--session_timeout_min`)
- **User definition**: `client_id` (device cookie id) — consistent with provided dictionary
- **Attribution**: touchpoints are **non-direct, non-internal sessions**; lookback window **7 days** (configurable via `--lookback_days`)
- **Revenue safety**: dedupe `transaction_id` by keeping the latest `checkout_completed` event per order

See `docs/transformation_methodology.md` for full methodology, trade-offs, and validation.

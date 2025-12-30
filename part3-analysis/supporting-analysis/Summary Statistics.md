
# Part 3 — Supporting Analysis Summary

This folder contains additional analysis and charts that support the executive narrative. The goal is to strengthen confidence in the conclusions (behavior patterns, attribution interpretation, and operational priorities) using transparent, reproducible calculations.

## Key Quantitative Takeaways (from transformed tables)

### Topline
- Sessions: **38,267**
- Orders: **290**
- Revenue: **$285,516**
- AOV: **$984.54**
- Session→Order conversion rate: **0.76%**

### Device conversion gap (with statistical test)
The analysis computes conversion rate by device and runs a **2×2 chi-square test** comparing desktop vs mobile conversion.
- Result (desktop vs mobile): **chi² = 5.78**
- p-value: **0.0162** (if available)

Interpretation: the desktop vs mobile conversion gap is unlikely to be random noise; it is consistent with meaningful UX / friction differences.

### Multi-session conversion behavior (7-day window)
For each order, we count how many sessions that same `client_id` had in the prior **7 days** (aligned to the attribution lookback).
- Mean sessions in lookback: **1.42**
- Median: **1**
- 75th percentile: **2**
- 90th percentile: **2**

This supports the executive claim that most purchases are not single-session and that attribution must account for assists.

### Attribution: first-click vs last-click
We compute channel-attributed revenue separately for first-click and last-click models. The comparison chart highlights how channel roles differ between **demand creation** (first) and **conversion capture** (last).

## Contents

### Charts (`supporting-analysis/charts/`)
- `daily_revenue.png` — revenue trend (detects volatility and mid-period shifts)
- `daily_orders.png` — order volume trend
- `daily_funnel_rates.png` — ATC / checkout / purchase rates by day
- `conversion_by_device.png` — device conversion rate comparison
- `attribution_first_vs_last.png` — channel revenue comparison (first vs last click)
- `sessions_before_purchase_hist.png` — distribution of sessions per purchase (7-day window)

### Code (`supporting-analysis/code/`)
- `supporting_analysis.py` — reproducible analysis + statistical checks
- `requirements.txt` — minimal dependencies

## How to run locally
From the repo root (with Part 2 outputs available under `warehouse/`):

```bash
pip install -r supporting-analysis/code/requirements.txt
python supporting-analysis/code/supporting_analysis.py
```


# Part 4 — Production Monitoring (Daily)

This pipeline runs daily and powers dashboards that influence marketing spend. Monitoring must therefore protect **data correctness** and **business-critical KPIs**, while minimizing alert fatigue.

## What We Monitor (and Why)

### 1) Freshness & Completeness (SLA)
**Goal:** ensure the daily load happened and includes the expected partition.
- **Partition freshness:** latest `session_start`/`order_ts` date equals expected partition date (default: **UTC yesterday**).
- **Missing days:** detect gaps in the date range (catastrophic for trend reporting).
- **Volume floors:** minimum sessions/orders per day to catch partial loads.

**Why:** Dashboards can look “fine” while missing a day/half a day, leading to wrong ROAS/revenue decisions.

### 2) Schema & Structural Integrity
**Goal:** catch breaking upstream changes early.
- Required columns present (guardrail against schema drift).
- Timestamp parsability (date derivation).
- Duplicated primary keys (e.g., `transaction_id`).

**Why:** Schema drift often fails silently and breaks attribution/funnel logic downstream.

### 3) Revenue Integrity (Business-Critical)
- Duplicate transactions (should be 0 in production order fact).
- Negative revenue (should be 0).
- Excess zero-revenue orders (warn — can indicate instrumentation/payment issues).

**Why:** Revenue is the most business-critical metric; errors propagate directly to spend decisions.

### 4) Funnel Sanity
- Sessions → purchase conversion rate and related step changes.

**Why:** Checkout outages, payment provider issues, or tag regressions show up as sudden conversion drops.

### 5) Marketing Mix Stability (Attribution Health)
- Monitor daily channel **revenue share** (last-click default) for top channels.
- Flag large share shifts (e.g., Meta suddenly goes to 0 and “Direct” spikes).

**Why:** Tracking breaks often reclassify traffic to Direct/None and can misdirect budget.

## How We Detect “Something is Wrong”

### A) Hard Rules (high signal, low noise)
Configured in `config/monitoring_rules.yml`:
- Duplicate transactions > 0 → **ERROR**
- Negative revenue > 0 → **ERROR**
- Critical null rate thresholds (e.g., `client_id`, `marketing_source`) → **ERROR**
- Zero-revenue orders above tolerance → **WARN**
- Freshness mismatch / missing days → **ERROR**

### B) Baseline Anomaly Detection (trend-aware)
For volatile metrics (sessions, orders, revenue, AOV, conversion rate), we alert only when:
- **|z-score| ≥ 3** AND
- **|percent change| ≥ 30%** vs rolling baseline (default: 7 days)

This avoids alert fatigue from normal day-to-day variance while catching step-function incidents.

### C) Marketing Mix Drift
For top channels, alert on **absolute share change** (default 15 points) vs baseline.

## Practical Daily Operations

### Alert routing (pragmatic)
- **ERROR** → PagerDuty + #data-alerts
- **WARN** → #data-warnings
- **INFO** → observability channel

(Implemented as a routing plan + console output stub in this repo; in production it would call Slack/PagerDuty APIs.)

### Quarantine / Rollback behavior
- If any **ERROR** findings occur and `quarantine_on_error: true`, the system:
  - Copies the current warehouse outputs to `quarantine/warehouse_quarantine_<timestamp>/`
  - Writes a reason file for investigation
  - Allows quick rollback to last-known-good outputs in orchestration

### Where this runs
Run monitoring immediately after the daily transformation build, and before dashboards refresh:
- If `--fail_on_error`, the job exits non-zero to stop propagation of bad data.

## Files
- Code: `monitoring/run_monitoring.py`
- Rules: `config/monitoring_rules.yml`
- Output: `reports/monitoring_report.md` and `reports/monitoring_report.json`
- Quarantine snapshots: `quarantine/`

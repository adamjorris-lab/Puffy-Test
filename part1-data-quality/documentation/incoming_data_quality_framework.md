# Incoming Data Quality Framework (Raw Events)

## What we validate (and why)

This framework is designed as a **“gate”** on raw event exports before they enter production analytics.  
It aims to catch the failure modes that typically distort **revenue**, **funnels**, and **channel attribution**:

1. **Schema drift / missing columns**
   - Detect missing required columns and known alias drift (e.g., `client_id` vs `clientId`).
   - Why: silent schema changes can break joins, sessions, and aggregation logic.

2. **Completeness thresholds**
   - `client_id` (canonicalized across aliases) null-rate threshold (default: >5% = **ERROR**).
   - Why: missing user/device keys breaks funnels, deduping, and conversion rate calculations.

3. **Timestamp validity**
   - Enforce ISO 8601 UTC format (`YYYY-MM-DDTHH:MM:SS.sssZ`) and parseability.
   - Why: timestamp parse failures cause events to fall out of partitions and distort time series.

4. **Allowed values**
   - Validate `event_name` against an allowlist (future-proofed to accept both `checkout_completed` and `purchase`).
   - Why: unexpected event names often indicate tracking regression or mis-mapped events.

5. **`event_data` JSON integrity**
   - Non-null `event_data` must be valid JSON.
   - Why: purchase revenue and transaction identifiers live in JSON; parse failures lead to “missing revenue”.

6. **Purchase-specific business rules** (for `checkout_completed`/`purchase`)
   - Require `transaction_id` and `revenue`
   - Flag revenue `<= 0`
   - Flag **non-integer revenue** (common cents/dollars mismatch)
   - Flag **duplicate `transaction_id`** (causes revenue inflation)

7. **Attribution health**
   - Warn when `referrer` is missing entirely or >95% null for a partition.
   - Why: channel-level ROI and revenue attribution will be wrong (or impossible).

## Issues identified in the provided dataset

**Schema drift**
- `client_id` column switches to `clientId` beginning **2025-02-27** (Feb 27–Mar 8 partitions).  
- The `referrer` column is **missing entirely** from partitions beginning **2025-03-04** (Mar 4–Mar 8).  

**High missing `client_id`**
- Canonical `client_id` null-rate spikes above threshold on multiple days, most notably:
  - **2025-03-02 (~15.8%)**, **2025-03-08 (~14.3%)**, and several days ~8–9%.  
  This would break user-level funnels and could distort “unique purchasers”, conversion rate, and LTV models.

**Revenue / purchase integrity problems**
- **Duplicate transaction IDs** occur (including cases where the same `transaction_id` appears multiple times **with different revenue values**), which will **inflate or corrupt revenue** if summed naively.
- **Zero-revenue purchases** occur on multiple dates (warnings) — revenue would appear “too low” compared to actual orders.
- **Non-integer revenue** values appear on at least two purchases (possible cents/dollars or type regression).

These issues explain a mid-period dashboard anomaly:
- Attribution breaks starting **Mar 4** (referrer missing) → channel revenue shifts/zeros.
- Client-id completeness degrades starting **Mar 2** → funnel/unique metrics drift.
- Purchase deduping becomes necessary to prevent double-counted revenue due to duplicated `transaction_id`.

## How to run

```bash
python dq_framework/run_validation.py --input_dir data/raw --output_dir reports
```

Outputs:
- `reports/dq_report.json` (machine readable)
- `reports/dq_report.md` (human readable)

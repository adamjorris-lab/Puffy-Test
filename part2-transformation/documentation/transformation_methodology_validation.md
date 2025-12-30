# Part 2 Documentation — Transformation Methodology & Architecture

## 1) Methodology & Architecture

### High-level approach
I model the warehouse layer as a simple, scalable **star schema** built from a standardized staging table:

- **`stg_events`**: canonicalized + enriched event stream (one row per raw event)
- **`fct_sessions`**: session-level behavior rollups (one row per session)
- **`fct_orders`**: order facts extracted from purchase events (one row per transaction_id)
- **`fct_attribution`**: attribution facts (two rows per order: first-click + last-click)

This separation is intentional:
- It keeps raw facts (`stg_events`) immutable and auditable.
- It allows multiple downstream marts (marketing, CRO, product) to reuse consistent definitions.
- It is easy to translate into dbt models later (each table is a clear model boundary).

### Key implementation choices
- **Deterministic event key**: `event_id` is assigned on load so downstream joins do not depend on fragile natural keys.
- **Schema drift tolerance**: `client_id` is canonicalized from either `client_id` or `clientId` (observed drift in the dataset).
- **Enrichment**:
  - `ref_domain` derived from `referrer`
  - `utm_source/utm_medium/utm_campaign` parsed from URL query keys (values are hashed, keys are stable)
  - `marketing_source` inferred primarily via stable click-id keys (e.g., `gclid`, `fbclid`) and known referrer domains
  - `device_type` parsed heuristically from `user_agent`

## 2) How I define users, sessions, and attribution

### User definition
- **User = `client_id`** (cookie/device identifier).  
  This matches the dataset’s definition: the same person on different devices/browsers will appear as different `client_id`s.

### Session definition (sessionization)
- **Partition** events by `client_id`, sort by event timestamp (`ts`)
- Start a **new session** when the time gap between consecutive events is **> 30 minutes**
- Assign `session_id = client_id + session_index`

Why this approach:
- It matches standard analytics practice (Google Analytics-style sessionization).
- It is robust without requiring page grouping assumptions.
- It scales naturally in SQL using window functions.

Trade-off:
- If a user leaves a tab open and returns after >30 min, behavior becomes two sessions (acceptable for marketing funnels).
- Because `client_id` is device-based, cross-device journeys remain separate unless you have login/identity stitching.

### Attribution definition (7-day lookback; first-click and last-click)
- **Touchpoint = session** where `marketing_source` is NOT `direct` and NOT `internal`
- For each order at time `order_ts`, find touchpoint sessions for the same `client_id` in the window:

`[order_ts - 7 days, order_ts]`

- **First-click attribution**: earliest touchpoint in the window
- **Last-click attribution**: latest touchpoint in the window
- If no touchpoints exist, attribute to **direct**

Why session-level touchpoints:
- It avoids double-counting multiple page views inside the same visit.
- It aligns with how media teams think about “visits” and “clicks”.
- It works even when UTM values are hashed, because the presence of click-id keys can still define paid source.

Trade-offs / limitations
- Without readable campaign names (hashed), campaign-level reporting is limited unless there is a mapping table.
- If a channel touch occurs but click-id stripping happens mid-session, the model can under-credit that channel.
- Multi-touch credit allocation (linear/time-decay) is not included, but the structure supports it.

## 3) Metrics and attributes included (and why)

### `fct_sessions`
Core session behavior + funnel flags:
- `session_start`, `session_end`, `duration_sec`
- counts of event types: `pageviews`, `add_to_cart`, `checkout_started`, `purchases`
- acquisition context from the **first event**:
  - `marketing_source`, `utm_*`, `ref_domain`, `device_type`, `page_url`

This table supports questions like:
- “How do users engage by device/source?”
- “What session behaviors correlate with purchasing?”
- “What do funnel drop-offs look like by source?”

### `fct_orders`
- `transaction_id` (deduped)
- `order_ts`, `revenue`, `items_count`
- `client_id`, `session_id`
- context fields (`marketing_source`, `utm_*`, `device_type`) from the order event

### `fct_attribution`
- `transaction_id`
- `model` in {`first_click`, `last_click`}
- `attributed_source`, `attributed_session_id` (+ utm/ref fields)

This table supports:
- channel revenue by model
- attribution QA (e.g., “what % ends up as direct?”)
- comparing first vs last click for budgeting decisions

## 4) Validation: proving correctness

I validate correctness using two layers:

### A) Automated unit tests (`pytest`)
- Ensures the **30-min rule** creates a new session at the right boundary.
- Ensures the **7-day lookback** excludes touchpoints older than 7 days.

### B) Reconciliation checks (generated every run)
`reports/reconciliation.md` + `reports/reconciliation.json`:
- event row counts
- event type counts
- session coverage vs missing client_id
- purchase event count vs deduped order count
- raw revenue sum vs deduped revenue sum (expected to differ only due to duplicate transaction_ids)

## 5) Answers to the 5 evaluation questions (in detail)

### (1) How do you approach sessionization?
- Use a **standard inactivity timeout** (30 minutes) per device user id.
- Ensure it’s **deterministic** (stable ordering using `event_id`).
- Carry **first-event acquisition fields** (landing context) into the session table so sessions can be grouped by channel/device without re-windowing.
- In production SQL: implement with `LAG(ts)` and a cumulative sum over `new_session` flags.

### (2) What attributes and metrics do you choose to calculate?
I choose metrics that:
- map directly to marketing + CRO questions,
- are stable over time,
- and are easily testable.

Session metrics:
- counts of key funnel events (`pageviews`, `add_to_cart`, `checkout_started`, `purchases`)
- session duration and timestamps
- acquisition context (source/referrer/UTM, device)

Order metrics:
- revenue, items_count, timestamp
- join keys back to sessions + events for drilldown

Attribution metrics:
- attributed source/session for first-click and last-click (two rows per order)

### (3) How do you handle attribution in a real e-commerce context?
- Use a **7-day lookback** as a hard business constraint.
- Use **sessions as touchpoints** to prevent “one click = many pageviews” overcounting.
- Use stable URL query keys (gclid/fbclid/etc.) + known referrer domains to infer marketing source when campaign values are hashed.
- Output both **first-click** (good for prospecting credit) and **last-click** (good for conversion capture) so budget owners can compare.

### (4) Does your output reconcile with your inputs?
Yes:
- `stg_events` is one-to-one with raw events (same row count).
- `fct_orders` is a **deduped** representation of purchase events:
  - raw purchase events = count of `checkout_completed`
  - orders = unique `transaction_id` after selecting the latest event per transaction
- The reconciliation report explicitly surfaces:
  - duplicates in raw purchases
  - raw vs deduped revenue sums

### (5) Is your code maintainable and your architecture scalable?
Maintainability:
- clear model boundaries (`stg_events` → `sessions/orders` → `attribution`)
- deterministic keys (`event_id`) prevent subtle join bugs
- unit tests protect core business logic

Scalability:
- The same logic translates cleanly into warehouse SQL (dbt models) using window functions.
- Sessionization and attribution can be partitioned by `client_id` and processed incrementally by event_date.
- Output tables are facts/dimensions that support additional marts (LTV, cohorting, MMM inputs) without refactoring.

---
**How to run:** see repo README.  
**Primary entrypoint:** `pipeline/run_pipeline.py`.

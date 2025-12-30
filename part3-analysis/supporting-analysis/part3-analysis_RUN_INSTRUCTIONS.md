
# Part 3 — Business Analysis
## How to Run / Use

Part 3 is designed for executive consumption first, with optional reproducible analysis artifacts.

---

## Primary Deliverable

- `executive-summary.pdf` (1–2 pages)  
  This is the document intended for senior management review.

If you are working from the Markdown version:
- `executive-summary.md` can be rendered in GitHub or pasted into Docs/Word and exported to PDF.

---

## Optional: Supporting Analysis (Charts + Code)

If you include supporting analysis folders (recommended), they typically look like:

```
supporting-analysis/
  SUMMARY.md
  charts/
  code/
```
and/or
```
supporting-analysis/part3-advanced-statistics/
  SUMMARY.md
  charts/
  code/
```

### Install dependencies (if running code)

From `part3-analysis/`:

```bash
pip install -r supporting-analysis/code/requirements.txt
```

(If you included the advanced statistics pack, install its requirements too.)

---

## Run Supporting Analysis Scripts

Example:

```bash
python supporting-analysis/code/supporting_analysis.py
```

and (if included):

```bash
python supporting-analysis/part3-advanced-statistics/code/advanced_models.py
```

These scripts:
- Load `warehouse/` tables produced by Part 2
- Recreate the charts used to support the executive narrative
- Produce quantified evidence for:
  - device conversion gap
  - funnel behavior
  - first vs last click channel role differences
  - multi-session journeys

---

## Inputs Required

Supporting analysis expects Part 2 outputs available locally, typically at:

```
warehouse/
  fct_sessions.csv
  fct_orders.csv
  fct_attribution.csv
```

If your files live elsewhere, update the script parameters or paths accordingly.

---

## Notes on Interpretation (Important)

Supporting statistics are **associational**:
- They validate observed patterns are unlikely to be random noise
- They do **not** claim causal lift (incrementality requires experiments)

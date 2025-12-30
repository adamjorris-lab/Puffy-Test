
# Advanced Statistical Support (Part 3)

## What Was Modeled (Association, Not Causation)

### Logistic Regression
**Model:** purchase ~ device + funnel depth  
- Mobile sessions have a statistically significant lower conversion probability.
- Add-to-cart and checkout events dominate conversion likelihood.

**Interpretation:** Device and funnel position are strong predictors of purchase, even when modeled jointly.

### Partial Dependence (Sessions-in-Lookback)
Conversion probability increases from 1 → ~3 sessions, then plateaus.

**Interpretation:** Multi-session journeys matter, but returns diminish — consistent with high-consideration buying.

### Correlation Structure
Checkout > Add-to-cart > Purchase show strong positive correlations.
Revenue aligns with conversion, not early funnel events.

**Interpretation:** Downstream funnel integrity matters more than raw traffic.

### Poisson Regression (Orders ~ Sessions)
Sessions are positively associated with order volume.

**Interpretation:** Demand and revenue scale together, but this is not incrementality.

### OLS with HAC Errors (Trend-Safe)
After accounting for autocorrelation, sessions remain associated with orders.

**Interpretation:** Relationship is robust to short-term trends, but still descriptive.

## Caveats (Critical)
- These models show **association, not causality**
- Attribution ≠ incrementality
- True lift requires experiments (geo / holdout)

## Executive-Safe Conclusion
Statistical analysis confirms that observed patterns are not noise. Device experience, funnel depth, and multi-session behavior materially influence conversion outcomes. Attribution differences reflect structural measurement effects, not randomness.

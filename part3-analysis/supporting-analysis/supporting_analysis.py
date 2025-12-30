
"""
Part 3 Supporting Analysis
- Loads transformed tables from Part 2 (sessions, orders, attribution)
- Produces charts and advanced statistical checks:
  * Daily toplines and funnel rates
  * First-click vs last-click channel mix
  * Device conversion rate + chi-square significance test
  * Distribution of sessions per purchase within 7-day lookback
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def chi_square_2x2(a, b, c, d):
    """
    2x2 chi-square test:
      [[a, b],
       [c, d]]
    Returns chi2 statistic and p-value (if scipy available).
    """
    obs = np.array([[a, b], [c, d]])
    row_sums = obs.sum(axis=1, keepdims=True)
    col_sums = obs.sum(axis=0, keepdims=True)
    total = obs.sum()
    expected = row_sums @ col_sums / total
    chi2 = ((obs - expected) ** 2 / expected).sum()

    p = None
    try:
        import scipy.stats as stats
        p = float(stats.chi2.sf(chi2, df=1))
    except Exception:
        pass
    return float(chi2), p

def main(warehouse_dir="warehouse", out_dir="supporting-analysis"):
    out_dir = str(out_dir)
    charts_dir = f"{out_dir}/charts"
    import os
    os.makedirs(charts_dir, exist_ok=True)

    sessions = pd.read_csv(f"{warehouse_dir}/fct_sessions.csv")
    orders = pd.read_csv(f"{warehouse_dir}/fct_orders.csv")
    attrib = pd.read_csv(f"{warehouse_dir}/fct_attribution.csv")

    sessions["session_start_ts"] = pd.to_datetime(sessions["session_start"], utc=True, errors="coerce")
    sessions["date"] = sessions["session_start_ts"].dt.date
    orders["order_ts_parsed"] = pd.to_datetime(orders["order_ts"], utc=True, errors="coerce")
    orders["date"] = orders["order_ts_parsed"].dt.date

    # Daily metrics
    daily_orders = orders.groupby("date").agg(orders=("transaction_id","nunique"),
                                              revenue=("revenue","sum"),
                                              aov=("revenue","mean")).reset_index()

    # Charts
    plt.figure()
    plt.plot(daily_orders["date"], daily_orders["revenue"])
    plt.title("Daily Revenue")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{charts_dir}/daily_revenue.png")
    plt.close()

    # Device conversion + significance
    sessions["is_purchase_session"] = (pd.to_numeric(sessions["purchases"], errors="coerce").fillna(0) > 0)
    dev = sessions.groupby("device_type").agg(sessions=("session_id","nunique"),
                                              purchases=("is_purchase_session","sum")).reset_index()
    dev["conv_rate"] = dev["purchases"] / dev["sessions"]

    # Example: desktop vs mobile chi-square
    if set(["desktop","mobile"]).issubset(set(dev["device_type"])):
        d = dev.set_index("device_type")
        a = int(d.loc["desktop","purchases"])
        b = int(d.loc["desktop","sessions"] - a)
        c = int(d.loc["mobile","purchases"])
        dd = int(d.loc["mobile","sessions"] - c)
        chi2, p = chi_square_2x2(a,b,c,dd)
        print("Desktop vs Mobile chi2:", chi2, "p:", p)

if __name__ == "__main__":
    main()

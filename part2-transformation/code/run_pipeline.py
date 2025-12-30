#!/usr/bin/env python3
"""
Puffy Skills Test - Part 2: Transformation Pipeline (Python reference implementation)

This script:
1) Loads 14 daily CSV partitions
2) Standardizes + enriches raw events (stg_events)
3) Builds session table (fct_sessions) using 30-min inactivity rule
4) Builds orders table (fct_orders) from checkout_completed event_data JSON
5) Builds 7-day lookback first-click + last-click attribution (fct_attribution)

Outputs are written to ./warehouse/ (CSV) and reconciliation to ./reports/.
"""

import argparse
import glob
import json
import os
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd


DEFAULT_SESSION_TIMEOUT_MIN = 30
DEFAULT_LOOKBACK_DAYS = 7


def _safe_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce")


def _ref_domain(ref) -> str | None:
    if pd.isna(ref) or ref == "":
        return None
    try:
        return urlparse(ref).netloc.lower()
    except Exception:
        return None


def _query_dict(url: str) -> dict:
    if not isinstance(url, str) or url == "":
        return {}
    try:
        return parse_qs(urlparse(url).query)
    except Exception:
        return {}


def _query_keys(url: str) -> set:
    return set(_query_dict(url).keys())


def _marketing_source(page_url: str, ref_domain: str | None) -> str:
    keys = _query_keys(page_url)

    # Paid identifiers (best-effort; values are hashed, keys are stable)
    if any(k in keys for k in ("gclid", "gbraid", "wbraid", "gad_source")):
        return "google_ads"
    if "fbclid" in keys:
        return "meta_ads"
    if "ttclid" in keys:
        return "tiktok_ads"
    if "msclkid" in keys:
        return "microsoft_ads"

    # UTM tagging present, but values are anonymized/hashed
    if any(k.startswith("utm_") for k in keys):
        return "utm_tagged"

    # Organic based on readable referrers (per data dictionary, google/bing not anonymized)
    if ref_domain in ("google.com", "www.google.com"):
        return "google_organic"
    if ref_domain in ("bing.com", "www.bing.com"):
        return "bing_organic"

    # Default buckets
    if ref_domain is None:
        return "direct"
    if ref_domain.endswith("puffy.com"):
        return "internal"
    return "other_referrer"


def _device_type(user_agent: str | None) -> str | None:
    if not isinstance(user_agent, str) or user_agent == "":
        return None
    ua = user_agent.lower()
    if "ipad" in ua or "tablet" in ua:
        return "tablet"
    if "iphone" in ua or "android" in ua or "mobile" in ua:
        return "mobile"
    return "desktop"


def _parse_event_data(s: str) -> dict:
    if not isinstance(s, str) or s.strip() == "":
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}


def build_stg_events(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # Row id is critical for stable joins across downstream transforms
    df["event_id"] = np.arange(len(df), dtype=np.int64)

    # Canonicalize client_id (schema drift: client_id -> clientId)
    df["client_id"] = df["client_id"].fillna(df.get("clientId"))

    df["ts"] = _safe_dt(df["timestamp"])
    df["event_date"] = df["ts"].dt.date

    df["ref_domain"] = df.get("referrer").apply(_ref_domain)

    qd = df["page_url"].apply(_query_dict)
    df["utm_source"] = qd.apply(lambda d: d.get("utm_source", [None])[0])
    df["utm_medium"] = qd.apply(lambda d: d.get("utm_medium", [None])[0])
    df["utm_campaign"] = qd.apply(lambda d: d.get("utm_campaign", [None])[0])

    df["marketing_source"] = [
        _marketing_source(u, r) for u, r in zip(df["page_url"], df["ref_domain"])
    ]
    df["device_type"] = df.get("user_agent").apply(_device_type)

    # Parse checkout_completed JSON
    ed = df["event_data"].apply(_parse_event_data)
    df["transaction_id"] = ed.apply(lambda d: d.get("transaction_id"))
    df["revenue"] = ed.apply(lambda d: d.get("revenue"))
    df["items_count"] = ed.apply(
        lambda d: len(d.get("items", [])) if isinstance(d.get("items", []), list) else None
    )

    # Ensure expected event types exist (per test prompt + dictionary)
    # We do not drop unknowns here; we surface them for monitoring.
    return df


def build_sessions(stg: pd.DataFrame, timeout_min: int = DEFAULT_SESSION_TIMEOUT_MIN) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sessionization:
    - Partition by client_id (device identifier)
    - Sort by ts
    - New session when inactivity gap > timeout_min
    """
    df = stg[stg["client_id"].notna()].copy()
    df = df.sort_values(["client_id", "ts", "event_id"])

    df["prev_ts"] = df.groupby("client_id")["ts"].shift(1)
    df["gap_min"] = (df["ts"] - df["prev_ts"]).dt.total_seconds() / 60.0
    df["new_session"] = df["prev_ts"].isna() | (df["gap_min"] > timeout_min)

    df["session_index"] = df.groupby("client_id")["new_session"].cumsum().astype(int)
    df["session_id"] = df["client_id"].astype(str) + "-" + df["session_index"].astype(str)

    # Attach session_id back to stg by event_id
    stg_out = stg.merge(df[["event_id", "session_id"]], on="event_id", how="left")

    # Session aggregates
    sess = df.groupby("session_id").agg(
        client_id=("client_id", "first"),
        session_start=("ts", "min"),
        session_end=("ts", "max"),
        events=("event_id", "size"),
        pageviews=("event_name", lambda x: (x == "page_viewed").sum()),
        add_to_cart=("event_name", lambda x: (x == "product_added_to_cart").sum()),
        checkout_started=("event_name", lambda x: (x == "checkout_started").sum()),
        purchases=("event_name", lambda x: (x == "checkout_completed").sum()),
    ).reset_index()
    sess["duration_sec"] = (sess["session_end"] - sess["session_start"]).dt.total_seconds()

    # Acquisition attributes: first event in session (proxy for landing)
    first_rows = df.sort_values(["session_id", "ts", "event_id"]).groupby("session_id").head(1)
    sess = sess.merge(
        first_rows[[
            "session_id", "page_url", "ref_domain", "marketing_source",
            "utm_source", "utm_medium", "utm_campaign", "user_agent"
        ]],
        on="session_id",
        how="left"
    )
    sess["device_type"] = sess["user_agent"].apply(_device_type)

    return stg_out, sess


def build_orders(stg: pd.DataFrame) -> pd.DataFrame:
    """
    Orders come from checkout_completed events with transaction_id + revenue in event_data JSON.
    We dedupe on transaction_id by keeping the latest event (by ts, event_id).
    """
    o = stg[stg["event_name"] == "checkout_completed"].copy()
    o = o[o["transaction_id"].notna()].copy()
    o = o.sort_values(["transaction_id", "ts", "event_id"])

    # Latest record per transaction_id (protects revenue from duplicate events)
    o_dedup = o.groupby("transaction_id", as_index=False).tail(1)

    orders = o_dedup[[
        "transaction_id", "client_id", "ts", "revenue", "items_count", "session_id",
        "marketing_source", "utm_source", "utm_medium", "utm_campaign", "ref_domain", "device_type"
    ]].rename(columns={"ts": "order_ts"}).reset_index(drop=True)

    return orders


def build_attribution(orders: pd.DataFrame, sessions: pd.DataFrame, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> pd.DataFrame:
    """
    Attribution (7-day lookback):
    - Touchpoints are sessions whose marketing_source is NOT direct/internal.
    - For each order, find all touchpoint sessions in [order_ts - lookback_days, order_ts]
    - First-click = earliest touchpoint in window
    - Last-click  = latest touchpoint in window
    """
    touch = sessions.copy()
    touch["is_marketing_touch"] = ~touch["marketing_source"].isin(["direct", "internal"])

    touch = touch[touch["is_marketing_touch"]].sort_values(["client_id", "session_start"])
    # Pre-group touchpoints per client for efficient lookup
    touch_by_client = {cid: g for cid, g in touch.groupby("client_id")}

    rows = []
    for r in orders.itertuples(index=False):
        start = r.order_ts - pd.Timedelta(days=lookback_days)

        g = touch_by_client.get(r.client_id)
        if g is None:
            rows.append({
                "transaction_id": r.transaction_id,
                "model": "first_click",
                "attributed_session_id": None,
                "attributed_source": "direct",
            })
            rows.append({
                "transaction_id": r.transaction_id,
                "model": "last_click",
                "attributed_session_id": None,
                "attributed_source": "direct",
            })
            continue

        inwin = g[(g["session_start"] >= start) & (g["session_start"] <= r.order_ts)]
        if inwin.empty:
            rows.append({
                "transaction_id": r.transaction_id,
                "model": "first_click",
                "attributed_session_id": None,
                "attributed_source": "direct",
            })
            rows.append({
                "transaction_id": r.transaction_id,
                "model": "last_click",
                "attributed_session_id": None,
                "attributed_source": "direct",
            })
            continue

        first = inwin.iloc[0]
        last = inwin.iloc[-1]

        rows.append({
            "transaction_id": r.transaction_id,
            "model": "first_click",
            "attributed_session_id": first["session_id"],
            "attributed_source": first["marketing_source"],
            "utm_source": first["utm_source"],
            "utm_medium": first["utm_medium"],
            "utm_campaign": first["utm_campaign"],
            "ref_domain": first["ref_domain"],
        })
        rows.append({
            "transaction_id": r.transaction_id,
            "model": "last_click",
            "attributed_session_id": last["session_id"],
            "attributed_source": last["marketing_source"],
            "utm_source": last["utm_source"],
            "utm_medium": last["utm_medium"],
            "utm_campaign": last["utm_campaign"],
            "ref_domain": last["ref_domain"],
        })

    attrib = pd.DataFrame(rows)
    return attrib


def reconciliation_report(stg: pd.DataFrame, sessions: pd.DataFrame, orders: pd.DataFrame, attrib: pd.DataFrame) -> dict:
    report = {}

    report["raw_event_rows"] = int(len(stg))
    report["event_types"] = stg["event_name"].value_counts().to_dict()

    # Sessionization coverage
    report["events_with_session_id"] = int(stg["session_id"].notna().sum())
    report["events_missing_client_id"] = int(stg["client_id"].isna().sum())
    report["session_rows"] = int(len(sessions))

    # Orders reconciliation
    raw_purchases = stg[stg["event_name"] == "checkout_completed"].copy()
    report["raw_purchase_events"] = int(len(raw_purchases))
    report["deduped_orders"] = int(len(orders))
    report["duplicate_transaction_ids_in_raw"] = int(raw_purchases["transaction_id"].duplicated().sum())

    report["raw_revenue_sum"] = float(pd.to_numeric(raw_purchases["revenue"], errors="coerce").fillna(0).sum())
    report["deduped_revenue_sum"] = float(pd.to_numeric(orders["revenue"], errors="coerce").fillna(0).sum())

    # Attribution coverage
    a = attrib.pivot_table(index="model", values="transaction_id", aggfunc="count").to_dict()["transaction_id"]
    report["attribution_rows_by_model"] = {k: int(v) for k, v in a.items()}
    direct_share = (attrib["attributed_source"] == "direct").mean()
    report["orders_attributed_to_direct_share"] = float(direct_share)

    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True, help="Folder with date-partitioned CSV files")
    ap.add_argument("--output_dir", required=True, help="Folder to write warehouse tables")
    ap.add_argument("--reports_dir", required=True, help="Folder to write reconciliation reports")
    ap.add_argument("--session_timeout_min", type=int, default=DEFAULT_SESSION_TIMEOUT_MIN)
    ap.add_argument("--lookback_days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    args = ap.parse_args()

    csvs = sorted(glob.glob(os.path.join(args.input_dir, "*.csv")))
    if not csvs:
        raise SystemExit(f"No CSV files found in {args.input_dir}")

    dfs = []
    for p in csvs:
        d = pd.read_csv(p)
        d["__file"] = os.path.basename(p)
        dfs.append(d)
    raw = pd.concat(dfs, ignore_index=True, sort=False)

    stg = build_stg_events(raw)
    stg, sessions = build_sessions(stg, timeout_min=args.session_timeout_min)
    orders = build_orders(stg)
    attrib = build_attribution(orders, sessions, lookback_days=args.lookback_days)

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.reports_dir, exist_ok=True)

    stg.to_csv(os.path.join(args.output_dir, "stg_events.csv"), index=False)
    sessions.to_csv(os.path.join(args.output_dir, "fct_sessions.csv"), index=False)
    orders.to_csv(os.path.join(args.output_dir, "fct_orders.csv"), index=False)
    attrib.to_csv(os.path.join(args.output_dir, "fct_attribution.csv"), index=False)

    rep = reconciliation_report(stg, sessions, orders, attrib)
    with open(os.path.join(args.reports_dir, "reconciliation.json"), "w") as f:
        json.dump(rep, f, indent=2, default=str)

    md_lines = ["# Reconciliation Summary", ""]
    for k, v in rep.items():
        md_lines.append(f"- **{k}**: {v}")
    with open(os.path.join(args.reports_dir, "reconciliation.md"), "w") as f:
        f.write("\n".join(md_lines) + "\n")

    print("✅ Pipeline complete")
    print(f"Tables written to: {args.output_dir}")
    print(f"Reports written to: {args.reports_dir}")


if __name__ == "__main__":
    main()

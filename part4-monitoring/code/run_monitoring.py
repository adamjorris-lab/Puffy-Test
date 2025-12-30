
from __future__ import annotations
import argparse, json, shutil
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import yaml

SEVERITY_ORDER = {"INFO": 0, "WARN": 1, "ERROR": 2}

@dataclass
class Finding:
    severity: str
    check: str
    partition_date: str | None
    message: str
    value: float | None = None
    threshold: float | None = None
    context: dict | None = None

def utc_today() -> datetime:
    return datetime.now(timezone.utc)

def iso(d: datetime) -> str:
    return d.date().isoformat()

def rolling_mean_std(prior: pd.Series, window: int) -> tuple[float, float]:
    s = prior.dropna()
    if len(s) == 0:
        return 0.0, 0.0
    s = s.iloc[-window:]
    return float(s.mean()), float(s.std(ddof=0))

def zscore(curr: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0 if curr == mean else float("inf")
    return (curr - mean) / std

def pct_change(curr: float, base: float) -> float:
    if base == 0:
        return float("inf") if curr != 0 else 0.0
    return (curr - base) / base

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_csv(path)

def ensure_columns(findings: list[Finding], df: pd.DataFrame, required: list[str], table: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        findings.append(Finding("ERROR", f"{table}:required_columns", None, f"Missing required columns: {missing}", context={"missing": missing}))

def add(findings: list[Finding], severity: str, check: str, date: str | None, message: str, value=None, threshold=None, context=None):
    findings.append(Finding(severity, check, date, message, value=value, threshold=threshold, context=context))

def route_alerts(findings: list[Finding], routing: dict, emit_console: bool = True) -> dict:
    """
    Practical routing stub:
    - In production this would send to PagerDuty/Slack/etc.
    - Here we group findings by severity and return a routing plan.
    """
    by_sev = {"ERROR": [], "WARN": [], "INFO": []}
    for f in findings:
        by_sev.setdefault(f.severity, []).append(f)

    plan = {
        "ERROR": routing.get("error_channels", []),
        "WARN": routing.get("warn_channels", []),
        "INFO": routing.get("info_channels", []),
    }

    if emit_console:
        for sev, items in by_sev.items():
            if not items:
                continue
            dest = plan.get(sev, [])
            print(f"[{sev}] {len(items)} finding(s) -> {dest}")
            for f in items[:10]:
                dp = f" ({f.partition_date})" if f.partition_date else ""
                print(f"  - {f.check}{dp}: {f.message}")
            if len(items) > 10:
                print(f"  ... (+{len(items)-10} more)")

    return {"counts": {k: len(v) for k, v in by_sev.items()}, "routing_plan": plan}

def quarantine_snapshot(warehouse_dir: Path, target_dir: Path, reason: str) -> Path:
    ts = utc_today().strftime("%Y%m%dT%H%M%SZ")
    qdir = target_dir / f"warehouse_quarantine_{ts}"
    qdir.mkdir(parents=True, exist_ok=True)
    # Copy key artifacts for investigation/rollback
    for f in warehouse_dir.glob("*.csv"):
        shutil.copy2(f, qdir / f.name)
    (qdir / "REASON.txt").write_text(reason)
    return qdir

def run(warehouse_dir: Path, rules: dict, output_dir: Path, fail_on_error: bool, expected_run_date: str | None) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    findings: list[Finding] = []

    sessions = load_csv(warehouse_dir / "fct_sessions.csv")
    orders = load_csv(warehouse_dir / "fct_orders.csv")
    attrib = load_csv(warehouse_dir / "fct_attribution.csv")

    # Schema guardrails
    ensure_columns(findings, sessions, rules["schemas"]["sessions_required"], "sessions")
    ensure_columns(findings, orders, rules["schemas"]["orders_required"], "orders")
    ensure_columns(findings, attrib, rules["schemas"]["attrib_required"], "attribution")

    sess_ts = rules["schemas"]["sessions_date_col"]
    ord_ts = rules["schemas"]["orders_date_col"]

    # Normalize dates (UTC) from timestamps
    sessions["date"] = pd.to_datetime(sessions[sess_ts], utc=True, errors="coerce").dt.date
    orders["date"] = pd.to_datetime(orders[ord_ts], utc=True, errors="coerce").dt.date

    # -------- A) Freshness & completeness (SLA) --------
    expected_lag = int(rules.get("freshness", {}).get("expected_lag_days", 1))
    if expected_run_date:
        exp = datetime.fromisoformat(expected_run_date).date()
    else:
        exp = (utc_today().date() - timedelta(days=expected_lag))

    sess_dates = sessions["date"].dropna()
    ord_dates = orders["date"].dropna()

    if sess_dates.empty:
        add(findings, "ERROR", "freshness:sessions_missing_dates", None, "No parsable session dates; freshness cannot be evaluated.")
    else:
        max_sess = sess_dates.max()
        if max_sess != exp:
            add(findings, "ERROR", "freshness:sessions_partition", str(max_sess),
                f"Latest session date is {max_sess.isoformat()} but expected {exp.isoformat()} (lag={expected_lag}d).")

    if ord_dates.empty:
        add(findings, "ERROR", "freshness:orders_missing_dates", None, "No parsable order dates; freshness cannot be evaluated.")
    else:
        max_ord = ord_dates.max()
        # orders can legitimately lag sessions by hours, but for daily batch we expect same date
        if max_ord != exp:
            add(findings, "ERROR", "freshness:orders_partition", str(max_ord),
                f"Latest order date is {max_ord.isoformat()} but expected {exp.isoformat()} (lag={expected_lag}d).")

    # Detect missing days in the range (completeness)
    allowed_missing = int(rules.get("freshness", {}).get("allowed_missing_days", 0))
    # Compare sessions and orders completeness independently
    def _missing_days(dates: pd.Series) -> list[str]:
        if dates.empty:
            return []
        dmin, dmax = dates.min(), dates.max()
        full = pd.date_range(dmin, dmax, freq="D").date
        present = set(dates.unique().tolist())
        miss = [d.isoformat() for d in full if d not in present]
        return miss

    miss_sess = _missing_days(sess_dates)
    if len(miss_sess) > allowed_missing:
        add(findings, "ERROR", "completeness:sessions_missing_days", None,
            f"Missing session days in range: {miss_sess}", value=float(len(miss_sess)), threshold=float(allowed_missing))

    miss_ord = _missing_days(ord_dates)
    if len(miss_ord) > allowed_missing:
        add(findings, "ERROR", "completeness:orders_missing_days", None,
            f"Missing order days in range: {miss_ord}", value=float(len(miss_ord)), threshold=float(allowed_missing))

    # -------- B) Hard rules --------
    dup_txn = int(orders["transaction_id"].duplicated().sum()) if "transaction_id" in orders.columns else 0
    if dup_txn > rules["hard_rules"]["max_duplicate_transactions"]:
        add(findings, "ERROR", "orders:duplicate_transactions", None, f"Duplicate transactions detected: {dup_txn}",
            value=float(dup_txn), threshold=float(rules["hard_rules"]["max_duplicate_transactions"]))

    neg_rev = int((pd.to_numeric(orders["revenue"], errors="coerce") < 0).sum())
    if neg_rev > 0:
        add(findings, "ERROR", "orders:negative_revenue", None, f"Negative revenue orders: {neg_rev}", value=float(neg_rev), threshold=0.0)

    zero_rev = int((pd.to_numeric(orders["revenue"], errors="coerce") == 0).sum())
    if zero_rev > rules["hard_rules"]["max_zero_revenue_orders"]:
        add(findings, "WARN", "orders:zero_revenue_orders", None, f"Zero revenue orders: {zero_rev}",
            value=float(zero_rev), threshold=float(rules["hard_rules"]["max_zero_revenue_orders"]))

    # Null-rate rules for critical fields (sessions)
    for col, limit in rules["hard_rules"]["max_null_rate"].items():
        if col in sessions.columns:
            null_rate = float(sessions[col].isna().mean())
            if null_rate > float(limit):
                add(findings, "ERROR", f"sessions:null_rate:{col}", None, f"High null-rate for {col}: {null_rate:.2%}",
                    value=null_rate, threshold=float(limit))

    # -------- C) Volume sanity floors (guards against partial loads) --------
    min_sessions = int(rules.get("freshness", {}).get("min_sessions_per_day", 0))
    min_orders = int(rules.get("freshness", {}).get("min_orders_per_day", 0))

    sessions_by_day = sessions.groupby("date")["session_id"].nunique()
    orders_by_day = orders.groupby("date")["transaction_id"].nunique()

    for d, v in sessions_by_day.items():
        if v < min_sessions:
            add(findings, "ERROR", "volume:sessions_floor", d.isoformat(),
                f"Sessions below floor: {v} < {min_sessions}", value=float(v), threshold=float(min_sessions))
    for d, v in orders_by_day.items():
        if v < min_orders:
            add(findings, "WARN", "volume:orders_floor", d.isoformat(),
                f"Orders below floor: {v} < {min_orders}", value=float(v), threshold=float(min_orders))

    # -------- D) Baseline anomaly detection --------
    # Create daily aggregates
    def has_count(df: pd.DataFrame, col: str) -> pd.Series:
        return (pd.to_numeric(df[col], errors="coerce").fillna(0) > 0).astype(int) if col in df.columns else pd.Series([0]*len(df))

    sessions["_has_purchase"] = has_count(sessions, "purchases")
    daily_sessions = sessions.groupby("date").agg(
        sessions=("session_id", "nunique"),
        purchase_sessions=("_has_purchase", "sum"),
    ).reset_index()
    daily_sessions["conversion_rate"] = np.where(daily_sessions["sessions"] > 0,
                                                 daily_sessions["purchase_sessions"]/daily_sessions["sessions"], 0.0)

    daily_orders = orders.groupby("date").agg(
        orders=("transaction_id", "nunique"),
        revenue=("revenue", "sum"),
        aov=("revenue", "mean"),
    ).reset_index()
    daily_orders["revenue"] = pd.to_numeric(daily_orders["revenue"], errors="coerce").fillna(0.0)
    daily_orders["aov"] = pd.to_numeric(daily_orders["aov"], errors="coerce").fillna(0.0)

    merged = daily_sessions.merge(daily_orders, on="date", how="outer").sort_values("date").reset_index(drop=True)

    baseline_days = int(rules["anomaly_detection"]["baseline_days"])
    z_thr = float(rules["anomaly_detection"]["z_score_threshold"])
    pct_thr = float(rules["anomaly_detection"]["pct_change_threshold"])

    for metric in ["sessions", "orders", "revenue", "aov", "conversion_rate"]:
        series = merged.set_index("date")[metric]
        dates = list(series.index)
        for i, d in enumerate(dates):
            if i < 4:
                continue
            prior = series.iloc[:i].dropna()
            if len(prior) < 3:
                continue
            mean, std = rolling_mean_std(prior, baseline_days)
            curr = float(series.loc[d]) if pd.notna(series.loc[d]) else None
            if curr is None:
                continue
            z = float(zscore(curr, mean, std))
            pc = float(pct_change(curr, mean))
            if (abs(z) >= z_thr) and (abs(pc) >= pct_thr):
                sev = "ERROR" if metric in ("orders", "revenue", "conversion_rate") else "WARN"
                add(findings, sev, f"anomaly:{metric}", d.isoformat(),
                    f"{metric} anomalous vs baseline mean={mean:.2f} std={std:.2f}: curr={curr:.2f} z={z:.2f} pct_change={pc:.2%}",
                    value=curr, threshold=z_thr, context={"baseline_mean": mean, "baseline_std": std, "z": z, "pct_change": pc})

    # -------- E) Marketing mix drift (channel share) --------
    if "transaction_id" in attrib.columns and "attributed_source" in attrib.columns:
        att = attrib.merge(orders[["transaction_id", "date", "revenue"]], on="transaction_id", how="left")
        if "model" in att.columns:
            lc = att["model"].astype(str).str.lower().str.contains("last")
            if lc.any():
                att = att.loc[lc].copy()

        daily_channel = att.groupby(["date", "attributed_source"])["revenue"].sum().reset_index()
        totals = daily_channel.groupby("date")["revenue"].sum().rename("total").reset_index()
        daily_channel = daily_channel.merge(totals, on="date", how="left")
        daily_channel["share"] = np.where(daily_channel["total"] > 0, daily_channel["revenue"]/daily_channel["total"], 0.0)

        top_n = int(rules["anomaly_detection"]["top_channels_to_monitor"])
        top_channels = (daily_channel.groupby("attributed_source")["revenue"].sum()
                        .sort_values(ascending=False).head(top_n).index.tolist())
        share_thr = float(rules["anomaly_detection"]["channel_share_abs_change_threshold"])

        for ch in top_channels:
            s = daily_channel.loc[daily_channel["attributed_source"]==ch].set_index("date")["share"].sort_index()
            dates = list(s.index)
            for i, d in enumerate(dates):
                if i < 4:
                    continue
                prior = s.iloc[:i].dropna()
                if len(prior) < 3:
                    continue
                mean, _ = rolling_mean_std(prior, baseline_days)
                curr = float(s.loc[d])
                if abs(curr - mean) >= share_thr:
                    add(findings, "WARN", "anomaly:channel_share", d.isoformat(),
                        f"Channel '{ch}' share shifted: curr={curr:.1%} vs baseline={mean:.1%}",
                        value=curr, threshold=share_thr, context={"channel": ch, "baseline_share": mean})
    else:
        add(findings, "INFO", "attribution:channel_share", None, "Skipping channel share checks (missing attribution fields).")

    # Sort + counts
    findings_sorted = sorted(findings, key=lambda f: (-SEVERITY_ORDER.get(f.severity, 0), f.check, str(f.partition_date)))
    counts = {"ERROR": 0, "WARN": 0, "INFO": 0}
    for f in findings_sorted:
        counts[f.severity] = counts.get(f.severity, 0) + 1

    # Routing / notifications (stub)
    routing = rules.get("alert_routing", {})
    routing_out = route_alerts(findings_sorted, routing, emit_console=bool(routing.get("emit_console_notifications", True)))

    # Write reports
    out = {
        "generated_utc": iso(utc_today()),
        "expected_partition_date": exp.isoformat(),
        "counts": counts,
        "routing": routing_out,
        "findings": [f.__dict__ for f in findings_sorted],
    }
    (output_dir / "monitoring_report.json").write_text(json.dumps(out, indent=2))

    md = []
    md.append("# Daily Monitoring Report")
    md.append(f"- Generated (UTC): {iso(utc_today())}")
    md.append(f"- Expected partition date: **{exp.isoformat()}**")
    md.append(f"- Warehouse: `{warehouse_dir}`")
    md.append("")
    md.append("## Summary")
    md.append(f"- ERROR: **{counts['ERROR']}** | WARN: **{counts['WARN']}** | INFO: **{counts['INFO']}**")
    md.append("")
    md.append("## Findings")
    if not findings_sorted:
        md.append("No issues detected.")
    else:
        for f in findings_sorted:
            d = f" ({f.partition_date})" if f.partition_date else ""
            thr = f" (threshold={f.threshold})" if f.threshold is not None else ""
            val = f" value={f.value}" if f.value is not None else ""
            md.append(f"- **{f.severity}** `{f.check}`{d}: {f.message}{thr}{val}")
    (output_dir / "monitoring_report.md").write_text("\n".join(md))

    # Quarantine / rollback hook (practical)
    ops = rules.get("operations", {})
    if counts["ERROR"] > 0 and bool(ops.get("quarantine_on_error", False)):
        target = Path(ops.get("quarantine_target_dir", "quarantine"))
        # target is relative to repo root if not absolute
        if not target.is_absolute():
            target = (warehouse_dir.parent / target).resolve()
        reason = f"Quarantined due to monitoring ERROR(s). See {output_dir/'monitoring_report.md'}"
        qdir = quarantine_snapshot(warehouse_dir, target, reason)
        (output_dir / "quarantine_path.txt").write_text(str(qdir))
        add(findings_sorted, "INFO", "operations:quarantine", None, f"Warehouse snapshot quarantined to: {qdir}")

    if fail_on_error and counts["ERROR"] > 0:
        return 2
    return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse_dir", required=True)
    ap.add_argument("--rules", default="config/monitoring_rules.yml")
    ap.add_argument("--output_dir", default="reports")
    ap.add_argument("--fail_on_error", action="store_true")
    ap.add_argument("--expected_run_date", default=None,
                    help="Override expected partition date (YYYY-MM-DD). If not provided, uses UTC today - expected_lag_days.")
    args = ap.parse_args()

    rules = yaml.safe_load(Path(args.rules).read_text())
    code = run(Path(args.warehouse_dir), rules, Path(args.output_dir), args.fail_on_error, args.expected_run_date)
    raise SystemExit(code)

if __name__ == "__main__":
    main()

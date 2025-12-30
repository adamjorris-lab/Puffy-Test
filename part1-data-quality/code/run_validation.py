
"""
Puffy DQ Framework (lightweight, dependency-minimal)

Run:
  python run_validation.py --input_dir data/raw --output_dir reports

Design goals:
- Catch schema drift, missing required fields, parseability, and business-critical validity issues.
- Emit a machine-readable JSON report + a human-readable Markdown summary.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml


ALLOWED_EVENT_NAMES = {
    "page_viewed",
    "email_filled_on_popup",
    "product_added_to_cart",
    "checkout_started",
    # In this dataset, "checkout_completed" behaves like the purchase event.
    "checkout_completed",
    # If upstream sends "purchase" in the future, allow it too (map in downstream models).
    "purchase",
}

REQUIRED_COLUMNS = {
    "page_url",
    "timestamp",
    "event_name",
    "user_agent",
    # client_id is required, but we allow a known alias clientId to handle drift.
    # referrer is nullable but important for marketing attribution.
}

CLIENT_ID_ALIASES = ["client_id", "clientId"]

ISO_TS_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


@dataclass
class Finding:
    severity: str  # ERROR, WARN, INFO
    check: str
    message: str
    sample: Optional[dict] = None



def load_rules(path: str | None) -> dict:
    """Load YAML rules; if None, return defaults matching the original hard-coded checks."""
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _parse_event_data(value) -> Optional[dict]:
    if pd.isna(value):
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def load_partition(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["source_file"] = os.path.basename(path)
    # canonical client id
    df["client_id_canonical"] = None
    for c in CLIENT_ID_ALIASES:
        if c in df.columns:
            df["client_id_canonical"] = df["client_id_canonical"].fillna(df[c])
    return df


def validate_partition(df: pd.DataFrame) -> Tuple[List[Finding], Dict]:
    """
    Validate a single day's partition.
    Returns:
      findings: list of Finding
      metrics: dict for monitoring (row counts, null rates, etc.)
    """
    findings: List[Finding] = []
    metrics: Dict = {}

    # Schema checks
    cols = set(df.columns)
    missing_required = sorted([c for c in REQUIRED_COLUMNS if c not in cols])
    if missing_required:
        findings.append(Finding(
            severity="ERROR",
            check="schema.required_columns",
            message=f"Missing required columns: {missing_required}",
        ))

    if not any(c in cols for c in CLIENT_ID_ALIASES):
        findings.append(Finding(
            severity="ERROR",
            check="schema.client_id_column",
            message=f"Missing client id column. Expected one of {CLIENT_ID_ALIASES}.",
        ))
    else:
        present = [c for c in CLIENT_ID_ALIASES if c in cols]
        if present != ["client_id"]:  # canonical preference
            findings.append(Finding(
                severity="WARN",
                check="schema.drift.client_id_alias",
                message=f"Client ID column drift detected. Present: {present}. Canonical expected: client_id.",
            ))

    if "referrer" not in cols:
        findings.append(Finding(
            severity="WARN",
            check="schema.missing_referrer",
            message="referrer column missing (will break/limit attribution).",
        ))

    # Basic type/format checks
    metrics["rows"] = int(len(df))
    metrics["null_rate.client_id_canonical"] = float(df["client_id_canonical"].isna().mean())
    if metrics["null_rate.client_id_canonical"] > 0.05:
        findings.append(Finding(
            severity="ERROR",
            check="nulls.client_id_canonical",
            message=f"High null rate for client_id_canonical: {metrics['null_rate.client_id_canonical']:.1%}",
        ))

    # timestamp format + parseability
    bad_ts = df["timestamp"].astype(str).apply(lambda s: not bool(ISO_TS_REGEX.match(s)))
    metrics["bad_timestamp_format"] = int(bad_ts.sum())
    if metrics["bad_timestamp_format"] > 0:
        findings.append(Finding(
            severity="ERROR",
            check="format.timestamp_iso8601",
            message=f"Found {metrics['bad_timestamp_format']} timestamps not matching expected ISO8601 '...sssZ' format.",
            sample={"timestamp_examples": df.loc[bad_ts, "timestamp"].head(3).tolist()},
        ))

    # event_name allowed values
    bad_event = ~df["event_name"].isin(ALLOWED_EVENT_NAMES)
    metrics["invalid_event_name"] = int(bad_event.sum())
    if metrics["invalid_event_name"] > 0:
        findings.append(Finding(
            severity="ERROR",
            check="values.event_name",
            message=f"Found {metrics['invalid_event_name']} rows with unexpected event_name values.",
            sample={"event_name_examples": df.loc[bad_event, "event_name"].value_counts().head(5).to_dict()},
        ))

    # event_data JSON validity (for non-null)
    non_null = df["event_data"].dropna()
    parsed_ok = non_null.apply(lambda v: _parse_event_data(v) is not None)
    bad_json = int((~parsed_ok).sum())
    metrics["bad_event_data_json"] = bad_json
    if bad_json > 0:
        findings.append(Finding(
            severity="ERROR",
            check="json.event_data_parse",
            message=f"Found {bad_json} non-null event_data values that are not valid JSON.",
        ))

    # Purchase-specific checks (checkout_completed / purchase)
    purchase_mask = df["event_name"].isin(["checkout_completed", "purchase"])
    purchase_df = df.loc[purchase_mask].copy()
    metrics["purchases"] = int(len(purchase_df))
    if len(purchase_df) > 0:
        purchase_df["event_data_parsed"] = purchase_df["event_data"].apply(_parse_event_data)
        # required purchase keys
        missing_tx = purchase_df["event_data_parsed"].apply(lambda d: d is None or d.get("transaction_id") in [None, ""])
        missing_rev = purchase_df["event_data_parsed"].apply(lambda d: d is None or d.get("revenue") is None)
        if missing_tx.any():
            findings.append(Finding(
                severity="ERROR",
                check="purchase.missing_transaction_id",
                message=f"{int(missing_tx.sum())} purchase rows missing transaction_id.",
            ))
        if missing_rev.any():
            findings.append(Finding(
                severity="ERROR",
                check="purchase.missing_revenue",
                message=f"{int(missing_rev.sum())} purchase rows missing revenue.",
            ))

        # revenue validity
        rev = purchase_df["event_data_parsed"].apply(lambda d: None if d is None else d.get("revenue"))
        rev_numeric = pd.to_numeric(rev, errors="coerce")
        zero_or_neg = (rev_numeric <= 0).fillna(False)
        if zero_or_neg.any():
            findings.append(Finding(
                severity="WARN",
                check="purchase.revenue_non_positive",
                message=f"{int(zero_or_neg.sum())} purchase rows have revenue <= 0.",
                sample={"transaction_ids": purchase_df.loc[zero_or_neg, "event_data_parsed"].head(5).apply(lambda d: d.get("transaction_id")).tolist()},
            ))

        # non-integer revenue (common if cents vs dollars bug)
        non_int = (rev_numeric.dropna() % 1 != 0)
        if non_int.any():
            # map back to tx ids
            txs = purchase_df.loc[rev_numeric.dropna().index[non_int], "event_data_parsed"].apply(lambda d: d.get("transaction_id")).head(5).tolist()
            findings.append(Finding(
                severity="WARN",
                check="purchase.revenue_non_integer",
                message=f"{int(non_int.sum())} purchase rows have non-integer revenue values (possible cents/dollars mismatch).",
                sample={"transaction_ids": txs},
            ))

        # duplicate transaction_id
        tx = purchase_df["event_data_parsed"].apply(lambda d: None if d is None else d.get("transaction_id"))
        dup = tx.duplicated(keep=False) & tx.notna()
        metrics["duplicate_transaction_ids"] = int(dup.sum())
        if metrics["duplicate_transaction_ids"] > 0:
            ex = purchase_df.loc[dup, ["timestamp", "source_file"]].head(5).to_dict(orient="records")
            findings.append(Finding(
                severity="ERROR",
                check="purchase.duplicate_transaction_id",
                message=f"Found {metrics['duplicate_transaction_ids']} purchase rows with duplicate transaction_id (will inflate revenue if summed).",
                sample={"examples": ex},
            ))

    # Attribution sanity (referrer null rate)
    if "referrer" in df.columns:
        metrics["null_rate.referrer"] = float(df["referrer"].isna().mean())
        if metrics["null_rate.referrer"] > 0.95:
            findings.append(Finding(
                severity="WARN",
                check="attribution.referrer_mostly_null",
                message=f"referrer is >95% null ({metrics['null_rate.referrer']:.1%}) — attribution likely broken for this partition.",
            ))

    return findings, metrics


def validate_directory(input_dir: str, rules: dict | None = None) -> Tuple[List[Finding], pd.DataFrame]:
    files = sorted([os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".csv")])
    all_findings: List[Finding] = []
    metrics_rows: List[dict] = []

    for path in files:
        df = load_partition(path)
        # partition date from filename
        m = re.search(r"events_(\d{8})\.csv$", os.path.basename(path))
        part_date = m.group(1) if m else os.path.basename(path)
        findings, metrics = validate_partition(df)
        for f in findings:
            # attach partition context
            f.sample = f.sample or {}
            f.sample["partition"] = part_date
            f.sample["source_file"] = os.path.basename(path)
            all_findings.append(f)
        metrics_rows.append({"partition": part_date, "source_file": os.path.basename(path), **metrics})

    metrics_df = pd.DataFrame(metrics_rows).sort_values("partition")
    return all_findings, metrics_df


def write_reports(findings: List[Finding], metrics_df: pd.DataFrame, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    # JSON
    json_path = os.path.join(output_dir, "dq_report.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "findings": [asdict(x) for x in findings],
            "metrics": metrics_df.to_dict(orient="records"),
        }, f, indent=2)

    # Markdown summary
    md_path = os.path.join(output_dir, "dq_report.md")
    by_sev = {"ERROR": [], "WARN": [], "INFO": []}
    for x in findings:
        by_sev.setdefault(x.severity, []).append(x)

    def _bullet(f: Finding) -> str:
        part = None
        if f.sample and "partition" in f.sample:
            part = f.sample["partition"]
        prefix = f"- **{f.check}**"
        if part:
            prefix += f" (partition {part})"
        msg = f"{prefix}: {f.message}"
        if f.sample:
            # keep samples compact
            sample = {k: v for k, v in f.sample.items() if k not in ["source_file"]}
            if sample:
                msg += f"  \n  _sample_: `{json.dumps(sample)[:220]}...`"
        return msg

    lines = []
    lines.append("# Data Quality Report\n")
    lines.append("## Summary\n")
    lines.append(f"- Partitions checked: **{len(metrics_df)}**")
    lines.append(f"- Total findings: **{len(findings)}** (ERROR: {len(by_sev.get('ERROR',[]))}, WARN: {len(by_sev.get('WARN',[]))})\n")

    lines.append("## Key Metrics by Partition\n")
    keep_cols = [c for c in ["partition","rows","purchases","duplicate_transaction_ids","null_rate.client_id_canonical","null_rate.referrer","bad_event_data_json","invalid_event_name"] if c in metrics_df.columns]
    lines.append(metrics_df[keep_cols].to_markdown(index=False))
    lines.append("\n")

    for sev in ["ERROR","WARN","INFO"]:
        if by_sev.get(sev):
            lines.append(f"## {sev}\n")
            for f in by_sev[sev]:
                lines.append(_bullet(f))
            lines.append("\n")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--output_dir", required=True)
    ap.add_argument("--rules", required=False, help="Path to YAML rules file")
    ap.add_argument("--fail_on_error", action="store_true", help="Exit non-zero if any ERROR findings")
    args = ap.parse_args()

    rules = load_rules(args.rules)
    findings, metrics_df = validate_directory(args.input_dir, rules=rules)
    write_reports(findings, metrics_df, args.output_dir)
    print(f"Wrote reports to: {args.output_dir}")

    if args.fail_on_error and any(f.severity == 'ERROR' for f in findings):
        raise SystemExit(2)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Customer 360 ETL Pipeline
-------------------------

This script builds a unified analytical dataset ("Customer 360") by combining
three raw data sources:

  • crm_leads.csv       → Customer leads and sign-up information
  • web_activity.json   → Web browsing activity (JSON Lines)
  • transactions.txt    → Transaction history (pipe-delimited)

Outputs:
  • customer_360.parquet  → Final, cleaned analytical dataset
  • rejected_transactions.log  → Log of invalid or rejected transactions
  • README.md             → Run summary

"""

import argparse
import json
import re
from pathlib import Path
from typing import List, Tuple
import pandas as pd

# ------------------------------------------------------------
# Utility Helpers
# ------------------------------------------------------------

# Regular expression to validate UUIDs
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

def is_valid_uuid(u: str) -> bool:
    """Check if a value is a valid UUID."""
    if pd.isna(u):
        return False
    return bool(UUID_RE.match(str(u).strip()))

def to_iso_timestamp(s):
    """Convert any date-like value to UTC ISO timestamp."""
    return pd.to_datetime(s, utc=True, errors="coerce")

def proper_case(x: str):
    """Convert text to Proper Case (e.g., 'john doe' → 'John Doe')."""
    return str(x).strip().lower().title() if pd.notna(x) else x

def clean_email(x: str):
    """Standardize email casing and remove spaces."""
    return str(x).strip().lower() if pd.notna(x) else x

def coerce_numeric(x):
    """Convert to numeric, forcing errors to NaN."""
    return pd.to_numeric(x, errors="coerce")

# ------------------------------------------------------------
# CRM: Customer Lead Data
# ------------------------------------------------------------

def read_crm(path: Path) -> pd.DataFrame:
    """
    Read and clean CRM leads data:
      - Normalize email, names, and timestamps
      - Deduplicate by email, keeping the most recent record
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=["", "null", "NaN"])
    df.columns = [c.lower().strip() for c in df.columns]

    # --- Email cleanup
    if "email" not in df.columns:
        df["email"] = pd.NA
    df["email"] = df["email"].apply(clean_email)

    # --- Name cleanup
    if "first_name" in df.columns:
        df["first_name"] = df["first_name"].apply(proper_case)
    if "last_name" in df.columns:
        df["last_name"] = df["last_name"].apply(proper_case)

    # If only "name" column exists, split it
    if "name" in df.columns and ("first_name" not in df.columns and "last_name" not in df.columns):
        parts = df["name"].fillna("").astype(str).str.strip().str.split(r"\s+", n=1, expand=True)
        df["first_name"] = parts[0].apply(proper_case)
        df["last_name"] = parts[1].apply(proper_case) if parts.shape[1] > 1 else pd.NA

    # Ensure name columns exist
    df["first_name"] = df.get("first_name", pd.NA)
    df["last_name"] = df.get("last_name", pd.NA)

    # --- Validate UUIDs
    df["user_uuid"] = df.get("user_uuid", pd.NA)
    df["user_uuid"] = df["user_uuid"].where(df["user_uuid"].apply(is_valid_uuid), pd.NA)

    # --- Convert timestamp columns to datetime
    ts_col = next((c for c in ["created_at", "created", "signup_date", "updated_at", "timestamp"]
                   if c in df.columns), None)
    df["_lead_ts"] = df[ts_col].apply(to_iso_timestamp) if ts_col else pd.NaT

    # --- Deduplicate leads by email (keep latest record)
    df["_row_order"] = range(len(df))
    df = df.sort_values(by=["_lead_ts", "_row_order"], ascending=[False, True])
    df = df.drop_duplicates(subset=["email"], keep="first").drop(columns=["_row_order"])

    keep_cols = [c for c in ["user_uuid", "email", "first_name", "last_name", "_lead_ts"] if c in df.columns]
    return df[keep_cols]

# ------------------------------------------------------------
# Web Activity Data
# ------------------------------------------------------------

def read_web(path: Path) -> pd.DataFrame:
    """
    Read and clean web activity logs:
      - Validate UUIDs
      - Aggregate total page views and last activity timestamp
    """
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not records:
        return pd.DataFrame(columns=["user_uuid", "total_page_views", "last_seen_ts"])

    df = pd.DataFrame(records)
    df.columns = [c.lower().strip() for c in df.columns]

    # --- Validate UUIDs
    df["user_uuid"] = df.get("user_uuid", pd.NA)
    df["user_uuid"] = df["user_uuid"].apply(lambda x: x if is_valid_uuid(str(x)) else pd.NA)
    df = df[df["user_uuid"].notna()]

    # --- Numeric and timestamp conversions
    df["page_view_count"] = coerce_numeric(df.get("page_view_count", 0)).fillna(0).astype("Int64")
    df["last_seen_ts"] = df.get("last_seen_ts", pd.NaT).apply(to_iso_timestamp)

    # --- Aggregate by user
    return (
        df.groupby("user_uuid")
          .agg(total_page_views=("page_view_count", "sum"),
               last_seen_ts=("last_seen_ts", "max"))
          .reset_index()
    )

# ------------------------------------------------------------
# Transactions Data
# ------------------------------------------------------------

def read_transactions(path: Path) -> Tuple[pd.DataFrame, List[str]]:
    """
    Read and validate transaction records:
      - Reject invalid UUIDs, non-positive amounts, or non-completed statuses
      - Log rejection reasons
      - Aggregate totals and last transaction timestamp
    """
    df = pd.read_csv(path, sep="|", dtype=str)
    df.columns = [c.lower().strip() for c in df.columns]
    df["amount"] = coerce_numeric(df.get("amount", pd.NA))
    df["status"] = df.get("status", "").str.lower().str.strip()
    df["user_uuid"] = df.get("user_uuid", pd.NA)

    reasons = []
    bad_uuid = ~df["user_uuid"].apply(is_valid_uuid)
    non_positive = ~(df["amount"] > 0)
    bad_status = df["status"] != "completed"

    for tid in df.loc[bad_uuid, "transaction_id"].fillna("<missing>"):
        reasons.append(f"{tid}\tINVALID_UUID")
    for tid in df.loc[non_positive, "transaction_id"].fillna("<missing>"):
        reasons.append(f"{tid}\tNON_POSITIVE_AMOUNT")
    for tid in df.loc[bad_status, "transaction_id"].fillna("<missing>"):
        reasons.append(f"{tid}\tINVALID_STATUS")

    # --- Keep only valid rows
    df_valid = df[~(bad_uuid | non_positive | bad_status)].copy()

    tx = (
        df_valid.groupby("user_uuid")
        .agg(total_spent=("amount", "sum"),
             transactions_count=("transaction_id", "count"))
        .reset_index()
    )

    # --- Add last transaction timestamp
    ts_col = next((c for c in ["timestamp", "created_at", "tx_ts"] if c in df_valid.columns), None)
    if ts_col:
        df_valid[ts_col] = df_valid[ts_col].apply(to_iso_timestamp)
        last_tx = df_valid.groupby("user_uuid")[ts_col].max().reset_index().rename(columns={ts_col: "last_transaction_ts"})
        tx = tx.merge(last_tx, on="user_uuid", how="left")
    else:
        tx["last_transaction_ts"] = pd.NaT

    return tx, reasons

# ------------------------------------------------------------
# Output Helpers
# ------------------------------------------------------------

def write_reject_log(reasons: List[str], path: Path):
    """Write rejected transaction IDs and reasons to a log file."""
    with open(path, "w") as f:
        f.write("transaction_id\trejection_reason\n")
        for r in reasons:
            f.write(r + "\n")

def write_output(df: pd.DataFrame, outdir: Path):
    """Save merged dataset as Parquet or CSV fallback."""
    parquet = outdir / "customer_360.parquet"
    csv = outdir / "customer_360.csv"
    try:
        df.to_parquet(parquet, index=False)
        print(f"✅ Saved {parquet}")
    except Exception:
        df.to_csv(csv, index=False)
        print(f"⚠️  Parquet not available, saved CSV fallback: {csv}")

# ------------------------------------------------------------
# Main Orchestration
# ------------------------------------------------------------

def main(crm_path: Path, web_path: Path, tx_path: Path, outdir: Path):
    """Main orchestration function to execute the full ETL pipeline."""
    print("🔹 Reading CRM data...")
    crm = read_crm(crm_path)

    print("🔹 Reading Web Activity data...")
    web = read_web(web_path)

    print("🔹 Reading Transactions data...")
    tx, reasons = read_transactions(tx_path)

    print("🔹 Merging all datasets...")
    merged = crm.merge(web, on="user_uuid", how="left").merge(tx, on="user_uuid", how="left")

    # --- Fill missing numeric values
    merged["total_page_views"] = merged["total_page_views"].fillna(0).astype("Int64")
    merged["transactions_count"] = merged["transactions_count"].fillna(0).astype("Int64")
    merged["total_spent"] = merged["total_spent"].fillna(0.0).astype(float)

    # --- Write output files
    write_output(merged, outdir)
    write_reject_log(reasons, outdir / "rejected_transactions.log")

    # --- Write minimal run summary
    with open(outdir / "README.md", "w") as f:
        f.write("Customer 360 ETL executed successfully.\n")

    print("✅ Process completed successfully!")

# ------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Customer 360 ETL pipeline.")
    parser.add_argument("--crm", type=Path, default=Path("crm_leads.csv"), help="Path to CRM leads CSV file")
    parser.add_argument("--web", type=Path, default=Path("web_activity.json"), help="Path to web activity JSON file")
    parser.add_argument("--tx", type=Path, default=Path("transactions.txt"), help="Path to transactions text file")
    parser.add_argument("--outdir", type=Path, default=Path("."), help="Output directory")
    args = parser.parse_args()

    main(args.crm, args.web, args.tx, args.outdir)
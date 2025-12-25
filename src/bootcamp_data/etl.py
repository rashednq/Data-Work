from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any, Tuple
import json

import pandas as pd

from .iio import read_orders_csv, read_users_csv, write_parquet
from .quality import require_columns, assert_non_empty, assert_unique_key, missingness_report,add_missing_flags
from .joins import safe_left_join
from .transforms import enforce_schema, normalize_text, parse_datetime, add_time_parts, add_outlier_flag, winsorize


@dataclass(frozen=True)
class Paths:
    """Data class to hold all project paths."""
    root: Path
    raw: Path
    cache: Path
    processed: Path
    external: Path


@dataclass(frozen=True)
class ETLConfig:
    paths: Paths
    orders_csv: str = "orders.csv"
    users_csv: str = "users.csv"


def load_inputs(cfg: ETLConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.paths.raw / cfg.orders_csv)
    users = read_users_csv(cfg.paths.raw / cfg.users_csv)
    return orders, users


def clean_orders(orders_raw: pd.DataFrame) -> pd.DataFrame:
    df = enforce_schema(orders_raw.copy())

    # Normalize status
    if "status" in df.columns:
        df["status_clean"] = normalize_text(df["status"])

    # Datetime + time parts
    if "created_at" in df.columns:
        df = parse_datetime(df, "created_at", utc=True)
        df = add_time_parts(df, "created_at")

    # Amount winsorize + outlier flag
    if "amount" in df.columns:
        df["amount_w"] = winsorize(df["amount"], lo_q=0.01, hi_q=0.99)
        df = add_outlier_flag(df, "amount", k=1.5)

    # Missing flags (حسب الأعمدة الموجودة)
    cols_for_flags = [c for c in ["amount", "quantity", "created_at", "status"] if c in df.columns]
    if cols_for_flags:
        df = add_missing_flags(df, cols_for_flags)

    return df


def clean_users(users_raw: pd.DataFrame) -> pd.DataFrame:
    df = users_raw.copy()

    if "user_id" in df.columns:
        df["user_id"] = df["user_id"].astype("string")

    # Normalize common text columns if present
    for col in ["country", "city", "segment"]:
        if col in df.columns:
            df[f"{col}_clean"] = normalize_text(df[col])

    return df


def build_analytics_table(orders_clean: pd.DataFrame, users_clean: pd.DataFrame) -> pd.DataFrame:
    # Safe LEFT JOIN on user_id (many orders to one user)
    if "user_id" in orders_clean.columns and "user_id" in users_clean.columns:
        return safe_left_join(orders_clean, users_clean, on="user_id", validate="many_to_one")

    # Fallback (shouldn't happen if data is correct)
    return orders_clean


def transform(cfg: ETLConfig, orders_raw: pd.DataFrame, users_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    # ---- Quality gates (حسب quality.py) ----
    require_columns(orders_raw, ["order_id", "user_id"])
    require_columns(users_raw, ["user_id"])

    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users_raw, "users_raw")

    assert_unique_key(orders_raw, "order_id", allow_na=False)
    assert_unique_key(users_raw, "user_id", allow_na=False)

    # ---- Cleaning ----
    orders_clean = clean_orders(orders_raw)
    users_clean = clean_users(users_raw)

    # ---- Analytics ----
    analytics = build_analytics_table(orders_clean, users_clean)

    # ---- Meta stats (حسب دليل المشروع) ----
    meta: Dict[str, Any] = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users_raw)),
        "rows_out_analytics": int(len(analytics)),
    }

    if "created_at" in orders_raw.columns:
        meta["missing_created_at"] = int(orders_raw["created_at"].isna().sum())

    # Country match rate (لو عندك country_clean من users)
    if "country_clean" in users_clean.columns and "user_id" in analytics.columns:
        # كم صف في analytics قدرنا نطلع له country_clean
        meta["country_match_rate"] = float(analytics["country_clean"].notna().mean())

    return orders_clean, users_clean, analytics, meta


def load_outputs(cfg: ETLConfig, orders_clean: pd.DataFrame, users_clean: pd.DataFrame, analytics: pd.DataFrame, meta: Dict[str, Any]) -> None:
    out = cfg.paths.processed
    out.mkdir(parents=True, exist_ok=True)

    write_parquet(orders_clean, out / "orders_clean.parquet")
    write_parquet(users_clean, out / "users.parquet")
    write_parquet(analytics, out / "analytics_table.parquet")

    payload = {
        **meta,
        "config": {
            "paths": {k: str(v) for k, v in asdict(cfg.paths).items()},
            "orders_csv": cfg.orders_csv,
            "users_csv": cfg.users_csv,
        },
    }
    (out / "_run_meta.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Extra: missingness report (يساعدك في التسليم)
    reports_dir = cfg.paths.root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    miss = missingness_report(orders_clean)
    miss.to_csv(reports_dir / "missingness_orders.csv", index=True)


def run_etl(cfg: ETLConfig) -> None:
    orders_raw, users_raw = load_inputs(cfg)
    orders_clean, users_clean, analytics, meta = transform(cfg, orders_raw, users_raw)
    load_outputs(cfg, orders_clean, users_clean, analytics, meta)

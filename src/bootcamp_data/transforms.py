import pandas as pd
import re


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
        created_at=pd.to_datetime(df["created_at"], errors="coerce", utc=True),
        status=df["status"].astype("string"),
    )

_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: dict) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))

def dedupe_keep_latest(
    df: pd.DataFrame,
    key_cols: list[str],
    ts_col: str,
) -> pd.DataFrame:
    return (
        df.sort_values(ts_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )

    
def parse_datetime(
    df: pd.DataFrame,
    col: str,
    *,
    utc: bool = True,
) -> pd.DataFrame:
    """
    Safely parse a datetime column.
    Invalid values -> NaT
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(
    df: pd.DataFrame,
    ts_col: str,
) -> pd.DataFrame:
    """
    Add common time grouping columns.
    """
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month = ts.dt.month.astype("Int64"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )
    
    
def iqr_bounds(
    s: pd.Series,
    *,
    k: float = 1.5,
) -> tuple[float, float]:
    """
    Return (lower, upper) IQR bounds.
    """
    x = pd.to_numeric(s, errors="coerce").dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """
    Add a boolean IQR outlier flag column for `col`.
    Does NOT modify values.
    """
    # ensure numeric for IQR
    x = pd.to_numeric(df[col], errors="coerce")

    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1

    lo = q1 - k * iqr
    hi = q3 + k * iqr

    return df.assign(**{f"{col}__is_outlier": (x < lo) | (x > hi)})


def winsorize(
    s: pd.Series,
    *,
    lo_q: float = 0.01,
    hi_q: float = 0.99,
) -> pd.Series:
    """
    Cap values to [lo_q, hi_q] quantiles (winsorization).
    Does NOT drop rows.
    Non-numeric values -> NaN (kept as NaN).
    """
    x = pd.to_numeric(s, errors="coerce")
    lo = x.quantile(lo_q)
    hi = x.quantile(hi_q)
    return x.clip(lower=lo, upper=hi)

import sys
from pathlib import Path
import logging

# --------------------------------------------------
# Make src/ importable
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# --------------------------------------------------
# Project imports
# --------------------------------------------------
from bootcamp_data.config import make_paths
from bootcamp_data.iio import read_parquet, write_parquet
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    add_outlier_flag,   # ✅ بدل iqr_outlier_flag
)
from bootcamp_data.joins import safe_left_join

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    paths = make_paths(ROOT)

    # ---------------- Load cleaned data ----------------
    orders = read_parquet(paths.processed / "orders_clean.parquet")
    users = read_parquet(paths.processed / "users.parquet")

    # ---------------- Build analytics table ----------------
    analytics = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
        .pipe(add_outlier_flag, col="amount", k=1.5)   # ✅ يضيف amount__is_outlier
        .pipe(
            safe_left_join,
            right=users,
            on="user_id",
            validate="many_to_one",
            suffixes=("", "_user"),
        )
    )

    # ---------------- Save output ----------------
    out_path = paths.processed / "analytics_table.parquet"
    write_parquet(analytics, out_path)

    log.info("Day 3 analytics table built successfully")
    log.info("Output saved at: %s", out_path)


if __name__ == "__main__":
    main()
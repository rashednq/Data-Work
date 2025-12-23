import sys
from pathlib import Path
from datetime import datetime, timezone
import logging
import json

# --------------------------------------------------
# Make src/ importable (لازم قبل أي import من bootcamp_data)
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# --------------------------------------------------
# Project imports (بعد sys.path فقط)
# --------------------------------------------------
from bootcamp_data.config import make_paths
from bootcamp_data.iio import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    normalize_text,
    apply_mapping,
    dedupe_keep_latest,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
    missingness_report,
)

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


def main() -> None:
    paths = make_paths(ROOT)

    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    # ---------------- Quality checks ----------------
    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"]
    )
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # ---------------- Cleaning ----------------
    orders = enforce_schema(orders)

    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=1, name="quantity")

    orders["status"] = normalize_text(orders["status"])
    orders["status"] = apply_mapping(
        orders["status"],
        {"paid": "paid", "refund": "refund", "refunded": "refund"},
    )

    orders = dedupe_keep_latest(orders, ["order_id"], "created_at")
    assert_unique_key(orders, "order_id")

    # ---------------- Reports ----------------
    report_path = ROOT / "reports" / "missingness_orders.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    missingness_report(orders).to_csv(report_path)

    # ---------------- Outputs ----------------
    out_orders = paths.processed / "orders_clean.parquet"
    out_users = paths.processed / "users.parquet"

    write_parquet(orders, out_orders)
    write_parquet(users, out_users)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {
            "orders": int(len(orders)),
            "users": int(len(users)),
        },
        "outputs": {
            "orders": str(out_orders),
            "users": str(out_users),
        },
    }

    meta_path = paths.processed / "_run_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info("Day 2 pipeline completed successfully")
    log.info("Missingness report saved at: %s", report_path)


if __name__ == "__main__":
    main()
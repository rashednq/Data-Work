import sys
from pathlib import Path
from datetime import datetime, timezone
import logging
import json

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
from bootcamp_data.iio import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema

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

    orders = enforce_schema(orders)

    out_orders = paths.processed / "orders.parquet"
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

    meta_path = paths.processed / "_run_meta_day1.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info("Day 1 pipeline completed successfully")


if __name__ == "__main__":
    main()
from __future__ import annotations

from pathlib import Path

from bootcamp_data.config import make_paths
from bootcamp_data.iio import read_orders_csv, write_parquet


def project_root() -> Path:
    """
    Return the repository root assuming this file is at: <root>/scripts/build_orders_parquet.py
    """
    return Path(__file__).resolve().parents[1]


def main() -> None:
    root = project_root()
    paths = make_paths(root)

    in_path = paths.raw / "orders.csv"
    out_path = paths.processed / "orders.parquet"

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    df = read_orders_csv(in_path)
    write_parquet(df, out_path)

    print("Done")
    print(f"Read : {in_path}")
    print(f"Wrote: {out_path}")
    print(f"Rows : {len(df):,} | Cols: {df.shape[1]}")


if __name__ == "__main__":
    main()

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.etl import Paths, ETLConfig, run_etl

def main() -> None:
    root = ROOT
    data = root / "data"
    paths = Paths(
        root=root,
        raw=data / "raw",
        cache=data / "cache",
        processed=data / "processed",
        external=data / "external",
    )
    run_etl(ETLConfig(paths=paths))

if __name__ == "__main__":
    main()
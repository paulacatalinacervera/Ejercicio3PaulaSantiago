from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from src.config import PROCESSED_DIR, RESULTS_DIR
from src.warehouse.queries import QUERIES


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def get_connection(processed_dir: Path | str = PROCESSED_DIR) -> duckdb.DuckDBPyConnection:
    processed = Path(processed_dir)
    required = {
        "trips": processed / "trips_flat" / "trips.parquet",
        "zones": processed / "zones.parquet",
        "hourly_zone_stats": processed / "hourly_zone_stats.parquet",
        "od_edges": processed / "od_edges.parquet",
    }
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Faltan archivos procesados. Ejecuta `python -m src.batch`. Faltantes: "
            + ", ".join(missing)
        )

    con = duckdb.connect()
    for view, path in required.items():
        con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet('{_sql_path(path)}')")
    return con


def run_queries(
    processed_dir: Path | str = PROCESSED_DIR,
    results_dir: Path | str = RESULTS_DIR,
) -> dict:
    con = get_connection(processed_dir)
    results = Path(results_dir)
    results.mkdir(parents=True, exist_ok=True)
    summary = {}
    try:
        for name, item in QUERIES.items():
            df = con.execute(item["sql"]).df()
            output = results / f"duckdb_{name}.csv"
            df.to_csv(output, index=False)
            summary[name] = {
                "description": item["description"],
                "rows": int(len(df)),
                "output": str(output),
            }
            print(f"\n[{name}] {item['description']}")
            print(df.head(15).to_string(index=False))
    finally:
        con.close()
    (results / "duckdb_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ejecutar consultas analiticas locales con DuckDB")
    parser.add_argument("--processed-dir", default=str(PROCESSED_DIR))
    parser.add_argument("--results-dir", default=str(RESULTS_DIR))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_queries(args.processed_dir, args.results_dir)


if __name__ == "__main__":
    main()

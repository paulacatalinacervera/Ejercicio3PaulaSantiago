from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from src.config import (
    DATA_DIR,
    GCP_DATASET,
    GCP_LOCATION,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    GCS_PREFIX,
    PROCESSED_DIR,
    RESULTS_DIR,
    S3_BUCKET,
    S3_PREFIX,
)
from src.warehouse.queries import QUERIES

TABLE_SOURCES = {
    "zones": "zones.parquet",
    "trips": "trips_flat/trips.parquet",
    "hourly_zone_stats": "hourly_zone_stats.parquet",
    "od_edges": "od_edges.parquet",
}
REFERENCE_FILES = ("taxi_zone_lookup.csv", "zone_centroids.csv")


def _require(value: str, name: str) -> str:
    if not value:
        raise ValueError(
            f"Falta {name}. Completa .env o usa `python -m src.warehouse.bigquery_load --local`."
        )
    return value


def _download_from_s3(target: Path) -> None:
    from src.s3_utils import get_s3_client

    client = get_s3_client()
    bucket = _require(S3_BUCKET, "S3_BUCKET")
    prefix = S3_PREFIX.strip("/")
    for relative in TABLE_SOURCES.values():
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, f"{prefix}/processed/{relative}", str(destination))


def _upload_files(bucket, source_root: Path, prefix: str) -> int:
    uploaded = 0
    for relative in TABLE_SOURCES.values():
        path = source_root / relative
        if not path.exists():
            raise FileNotFoundError(f"Falta el archivo procesado: {path}")
        bucket.blob(f"{prefix}/processed/{relative}").upload_from_filename(str(path))
        uploaded += 1
    for filename in REFERENCE_FILES:
        path = DATA_DIR / filename
        if path.exists():
            bucket.blob(f"{prefix}/reference/{filename}").upload_from_filename(str(path))
    return uploaded


def upload_to_gcs(from_s3: bool = False) -> dict:
    project = _require(GCP_PROJECT_ID, "GCP_PROJECT_ID")
    bucket_name = _require(GCS_BUCKET, "GCS_BUCKET")
    prefix = GCS_PREFIX.strip("/")
    bucket = storage.Client(project=project).bucket(bucket_name)

    if from_s3:
        with tempfile.TemporaryDirectory(prefix="mobility_gcs_") as temporary:
            _download_from_s3(Path(temporary))
            uploaded = _upload_files(bucket, Path(temporary), prefix)
    else:
        uploaded = _upload_files(bucket, Path(PROCESSED_DIR), prefix)

    return {
        "bucket": bucket_name,
        "prefix": f"{prefix}/processed",
        "files": uploaded,
    }


def ensure_dataset(client) -> str:
    project = _require(GCP_PROJECT_ID, "GCP_PROJECT_ID")
    dataset = _require(GCP_DATASET, "GCP_DATASET")
    reference = bigquery.DatasetReference(project, dataset)
    try:
        client.get_dataset(reference)
    except NotFound:
        created = bigquery.Dataset(reference)
        created.location = _require(GCP_LOCATION, "GCP_LOCATION")
        client.create_dataset(created)
    return f"{project}.{dataset}"


def load_tables(client, dataset_id: str) -> list[dict]:
    bucket_name = _require(GCS_BUCKET, "GCS_BUCKET")
    prefix = GCS_PREFIX.strip("/")
    summaries = []
    for table_name, relative in TABLE_SOURCES.items():
        uri = f"gs://{bucket_name}/{prefix}/processed/{relative}"
        table_id = f"{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        summaries.append({"table": table_name, "rows": int(table.num_rows)})
        print(f"[load] {table_name}: {table.num_rows} filas")
    return summaries


def run_queries(client, dataset_id: str) -> dict:
    results = Path(RESULTS_DIR)
    results.mkdir(parents=True, exist_ok=True)
    summary = {"project": GCP_PROJECT_ID, "dataset": dataset_id, "queries": {}}

    for name, item in QUERIES.items():
        job_config = bigquery.QueryJobConfig(default_dataset=dataset_id)
        dataframe = client.query(item["sql"], job_config=job_config).to_dataframe()
        output = results / f"bigquery_{name}.csv"
        dataframe.to_csv(output, index=False)
        summary["queries"][name] = {
            "description": item["description"],
            "rows": int(len(dataframe)),
            "output": str(output),
        }
        print(f"\n[{name}] {item['description']}")
        print(dataframe.head(15).to_string(index=False))

    (results / "bigquery_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cargar GCS en BigQuery y ejecutar consultas")
    parser.add_argument("--local", action="store_true", help="Usar DuckDB local en vez de BigQuery")
    parser.add_argument("--from-s3", action="store_true", help="Descargar Parquet desde S3 antes de subirlos a GCS")
    parser.add_argument("--skip-upload", action="store_true", help="Reutilizar Parquet ya presentes en GCS")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.local:
        from src.warehouse.local_duckdb import run_queries as run_local_queries

        run_local_queries()
        return

    try:
        project = _require(GCP_PROJECT_ID, "GCP_PROJECT_ID")
        if not args.skip_upload:
            upload = upload_to_gcs(from_s3=args.from_s3)
            print(f"[gcs] {upload['bucket']}: {upload['files']} archivos en {upload['prefix']}")

        client = bigquery.Client(project=project)
        dataset_id = ensure_dataset(client)
        tables = load_tables(client, dataset_id)
        summary = run_queries(client, dataset_id)
        summary["tables"] = tables
        (Path(RESULTS_DIR) / "bigquery_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Error BigQuery: {exc}", file=sys.stderr)
        print(
            "Sugerencia: verifica `gcloud auth application-default login` "
            "o ejecuta `python -m src.warehouse.bigquery_load --local`.",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import PROCESSED_DIR, RESULTS_DIR, SYNTHETIC_DIR, ZONE_CENTROIDS_CSV, ensure_directories
from src.zones import build_zone_coordinates, haversine_km, load_zones


def _enrich_trips(trips: pd.DataFrame) -> pd.DataFrame:
    trips = trips.copy()
    trips["pickup_ts"] = pd.to_datetime(trips["pickup_ts"])
    trips["dropoff_ts"] = pd.to_datetime(trips["dropoff_ts"])
    trips["pickup_date"] = trips["pickup_ts"].dt.date
    trips["hour"] = trips["pickup_ts"].dt.hour.astype(int)
    trips["day_of_week"] = trips["pickup_ts"].dt.day_of_week.astype(int)
    trips["day_type"] = np.where(trips["day_of_week"] >= 5, "weekend", "weekday")
    trips["is_peak"] = trips["hour"].isin([7, 8, 9, 17, 18, 19])
    trips["congestion_index"] = np.clip(1.0 - trips["speed_kmh"] / 35.0, 0.0, 1.0).round(4)
    return trips


def _build_od_links(od_edges: pd.DataFrame, coords: pd.DataFrame) -> pd.DataFrame:
    zone_cols = coords[["location_id", "zone", "borough", "lat", "lon"]]
    origin = zone_cols.rename(
        columns={
            "location_id": "origin_zone",
            "zone": "origin_zone_name",
            "borough": "origin_borough",
            "lat": "origin_lat",
            "lon": "origin_lon",
        }
    )
    destination = zone_cols.rename(
        columns={
            "location_id": "destination_zone",
            "zone": "destination_zone_name",
            "borough": "destination_borough",
            "lat": "destination_lat",
            "lon": "destination_lon",
        }
    )
    links = od_edges.merge(origin, on="origin_zone", how="left").merge(
        destination, on="destination_zone", how="left"
    )
    links["haversine_km"] = haversine_km(
        links["origin_lat"].to_numpy(dtype=float),
        links["origin_lon"].to_numpy(dtype=float),
        links["destination_lat"].to_numpy(dtype=float),
        links["destination_lon"].to_numpy(dtype=float),
    ).round(3)
    mean_lat = np.radians((links["origin_lat"] + links["destination_lat"]) / 2.0)
    links["grid_km"] = (
        111.32
        * (
            (links["destination_lat"] - links["origin_lat"]).abs()
            + (links["destination_lon"] - links["origin_lon"]).abs() * np.cos(mean_lat)
        )
    ).round(3)
    links["path"] = [
        [
            [row.origin_lon, row.origin_lat],
            [row.destination_lon, row.origin_lat]
            if (int(row.origin_zone) + int(row.destination_zone)) % 2 == 0
            else [row.origin_lon, row.destination_lat],
            [row.destination_lon, row.destination_lat],
        ]
        for row in links.itertuples(index=False)
    ]
    return links


def build_processed_datasets(
    synthetic_dir: Path | str = SYNTHETIC_DIR,
    out_dir: Path | str = PROCESSED_DIR,
    zones_path: Path | str | None = None,
    centroids_path: Path | str | None = None,
) -> dict:
    ensure_directories()
    synthetic = Path(synthetic_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    trips_file = synthetic / "trips.parquet"
    readings_file = synthetic / "gps_readings.parquet"
    if not trips_file.exists():
        raise FileNotFoundError(f"No existe {trips_file}. Ejecuta primero el generador.")
    if not readings_file.exists():
        raise FileNotFoundError(f"No existe {readings_file}. Ejecuta primero el generador.")

    zones = load_zones(zones_path)
    coords = build_zone_coordinates(zones, centroids_path or ZONE_CENTROIDS_CSV)
    zones_out = zones.merge(coords[["location_id", "lat", "lon"]], on="location_id", how="left")
    zones_out.to_parquet(out / "zones.parquet", index=False)

    trips = _enrich_trips(pd.read_parquet(trips_file))
    gps = pd.read_parquet(readings_file)
    gps["event_time"] = pd.to_datetime(gps["event_time"])
    gps["day_bucket"] = gps["event_time"].dt.strftime("%Y%m%d")

    trips_flat_dir = out / "trips_flat"
    trips_flat_dir.mkdir(parents=True, exist_ok=True)
    trips.to_parquet(trips_flat_dir / "trips.parquet", index=False)
    trips.to_parquet(out / "trips", index=False, partition_cols=["pickup_date"])

    gps.to_parquet(out / "gps_enriched.parquet", index=False)

    hourly = (
        trips.groupby(
            ["pickup_date", "origin_zone", "origin_borough", "origin_zone_name", "hour"],
            as_index=False,
        )
        .agg(
            trips=("trip_id", "count"),
            avg_speed_kmh=("speed_kmh", "mean"),
            avg_duration_min=("duration_min", "mean"),
            avg_distance_km=("distance_km", "mean"),
            avg_occupancy=("occupancy", "mean"),
        )
        .reset_index(drop=True)
    )
    hourly["congestion_index"] = np.clip(1.0 - hourly["avg_speed_kmh"] / 35.0, 0.0, 1.0).round(4)
    for col in ["avg_speed_kmh", "avg_duration_min", "avg_distance_km", "avg_occupancy"]:
        hourly[col] = hourly[col].round(3)
    hourly.to_parquet(out / "hourly_zone_stats.parquet", index=False)

    od_edges = (
        trips.groupby(["origin_zone", "destination_zone"], as_index=False)
        .agg(
            trips=("trip_id", "count"),
            avg_duration_min=("duration_min", "mean"),
            avg_distance_km=("distance_km", "mean"),
            peak_trips=("is_peak", "sum"),
        )
        .reset_index(drop=True)
    )
    od_edges["avg_duration_min"] = od_edges["avg_duration_min"].round(3)
    od_edges["avg_distance_km"] = od_edges["avg_distance_km"].round(3)
    od_edges.to_parquet(out / "od_edges.parquet", index=False)

    od_links = _build_od_links(od_edges, coords)
    od_links.to_parquet(out / "od_links.parquet", index=False)

    summary = {
        "trips": int(len(trips)),
        "gps_readings": int(len(gps)),
        "zones": int(len(zones_out)),
        "hourly_zone_stats": int(len(hourly)),
        "od_edges": int(len(od_edges)),
        "od_links": int(len(od_links)),
        "start_date": str(trips["pickup_ts"].min()),
        "end_date": str(trips["pickup_ts"].max()),
        "outputs": {
            "trips_flat": str(trips_flat_dir / "trips.parquet"),
            "trips_partitioned": str(out / "trips"),
            "gps_enriched": str(out / "gps_enriched.parquet"),
            "hourly_zone_stats": str(out / "hourly_zone_stats.parquet"),
            "od_edges": str(out / "od_edges.parquet"),
            "od_links": str(out / "od_links.parquet"),
            "zones": str(out / "zones.parquet"),
        },
        "volume_ok": bool(len(gps) >= 500_000),
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "batch_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enriquecer datos sinteticos y crear agregados para S3/BigQuery")
    parser.add_argument("--synthetic-dir", default=str(SYNTHETIC_DIR))
    parser.add_argument("--out", default=str(PROCESSED_DIR))
    parser.add_argument("--zones", default=None)
    parser.add_argument("--centroids", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = build_processed_datasets(
        synthetic_dir=args.synthetic_dir,
        out_dir=args.out,
        zones_path=args.zones,
        centroids_path=args.centroids,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

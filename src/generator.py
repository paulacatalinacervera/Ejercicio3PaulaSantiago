from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import SYNTHETIC_DIR, ZONE_CENTROIDS_CSV, ensure_directories
from src.zones import (
    build_zone_coordinates,
    distance_matrix,
    load_zones,
    save_zone_coordinates,
    zone_coordinate_map,
    zone_weights,
)

WEEKDAY_PROFILE = np.array(
    [1.0, 0.8, 0.6, 0.5, 0.7, 1.2, 2.5, 4.5, 5.5, 4.0, 3.0, 3.0,
     3.3, 3.5, 3.2, 3.0, 3.5, 5.0, 5.5, 4.2, 3.2, 2.5, 2.0, 1.5],
    dtype=float,
)
WEEKEND_PROFILE = np.array(
    [1.5, 1.2, 0.8, 0.6, 0.5, 0.7, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5,
     3.8, 4.0, 4.2, 4.0, 3.8, 3.5, 3.2, 3.0, 2.8, 2.5, 2.2, 1.8],
    dtype=float,
)
WEEKDAY_PROFILE = WEEKDAY_PROFILE / WEEKDAY_PROFILE.sum()
WEEKEND_PROFILE = WEEKEND_PROFILE / WEEKEND_PROFILE.sum()

PEAK_HOURS = {7, 8, 9, 17, 18, 19}
FREE_FLOW_SPEED_KMH = 35.0


def _bearing_degrees(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _speed_for_hour(hour: int, borough: str, rng: np.random.Generator) -> float:
    if borough == "Manhattan":
        base = 24.0
    elif borough in {"Brooklyn", "Queens"}:
        base = 30.0
    elif borough == "Bronx":
        base = 32.0
    elif borough == "Staten Island":
        base = 38.0
    else:
        base = 35.0
    if hour in PEAK_HOURS:
        base *= 0.62
    elif hour in {0, 1, 2, 3, 4, 22, 23}:
        base *= 1.18
    return float(np.clip(base + rng.normal(0.0, 3.5), 5.0, 65.0))


def generate_dataset(
    trips: int = 200_000,
    pings_min: int = 4,
    pings_max: int = 7,
    seed: int = 42,
    start_date: str | None = "2026-11-14",
    days: int = 6,
    fleet_size: int = 5_000,
    out_dir: Path | str | None = None,
    zones_path: Path | str | None = None,
    centroids_path: Path | str | None = None,
) -> dict:
    """Generate a reproducible synthetic mobility dataset.

    The generator creates trips using a gravity model over the 265 NYC taxi
    zones and then emits several GPS pings per trip.     The default configuration
    produces more than 2,000,000 readings across six days, above the 500,000 required volume.
    """
    if trips <= 0:
        raise ValueError("trips debe ser mayor que cero")
    if pings_min <= 0 or pings_max < pings_min:
        raise ValueError("pings_min/pings_max invalidos")
    if days <= 0:
        raise ValueError("days debe ser mayor que cero")

    ensure_directories()
    out = Path(out_dir) if out_dir else SYNTHETIC_DIR
    out.mkdir(parents=True, exist_ok=True)

    zones = load_zones(zones_path)
    coords = build_zone_coordinates(zones, centroids_path)
    save_zone_coordinates(coords, centroids_path or ZONE_CENTROIDS_CSV)
    lat, lon = zone_coordinate_map(zones, coords)
    n = len(zones)

    rng = np.random.default_rng(seed)
    start = pd.Timestamp(start_date).normalize() if start_date else pd.Timestamp(date.today())
    start_ns = int(start.value)
    start_weekday = start.weekday()

    weights = zone_weights(zones)
    dist = distance_matrix(coords)
    gravity = weights[None, :] / np.power(dist + 1.0, 1.55)
    np.fill_diagonal(gravity, 0.0)
    od_prob = gravity / gravity.sum(axis=1, keepdims=True)

    boroughs = zones["borough"].to_numpy()
    zone_names = zones["zone"].to_numpy()
    location_ids = zones["location_id"].to_numpy(dtype=int)
    zone_index = {int(z): i for i, z in enumerate(location_ids)}

    origins = rng.choice(n, size=trips, p=weights)
    destinations = np.empty(trips, dtype=int)
    for i, origin in enumerate(origins):
        destinations[i] = int(rng.choice(n, p=od_prob[origin]))

    day_offsets = rng.integers(0, days, size=trips)
    is_weekend = ((start_weekday + day_offsets) % 7) >= 5
    hours = np.empty(trips, dtype=int)
    for weekend in (False, True):
        mask = is_weekend == weekend
        if mask.any():
            profile = WEEKEND_PROFILE if weekend else WEEKDAY_PROFILE
            hours[mask] = rng.choice(24, size=int(mask.sum()), p=profile)
    seconds = rng.integers(0, 3600, size=trips)
    pickup_ns = start_ns + ((day_offsets * 24 + hours) * 3600 + seconds) * 1_000_000_000

    road_distance = dist[origins, destinations] * 1.28 + rng.uniform(0.2, 1.0, trips)
    speeds = np.empty(trips, dtype=float)
    for i in range(trips):
        speeds[i] = _speed_for_hour(int(hours[i]), str(boroughs[origins[i]]), rng)
    durations = np.clip(road_distance / speeds * 60.0, 2.0, 240.0)
    dropoff_ns = pickup_ns + (durations * 60.0 * 1_000_000_000).astype(np.int64)

    fleet = np.array([f"veh-{i:05d}" for i in range(fleet_size)])
    vehicle_ids = rng.choice(fleet, size=trips)
    occupancies = rng.integers(0, 5, size=trips)
    trip_ids = np.array([f"t{i:08d}" for i in range(trips)])
    is_peak = np.isin(hours, list(PEAK_HOURS))
    congestion_index = np.clip(1.0 - speeds / FREE_FLOW_SPEED_KMH, 0.0, 1.0)

    trips_df = pd.DataFrame(
        {
            "trip_id": trip_ids,
            "vehicle_id": vehicle_ids,
            "origin_zone": location_ids[origins],
            "destination_zone": location_ids[destinations],
            "pickup_ts": pd.to_datetime(pickup_ns, unit="ns"),
            "dropoff_ts": pd.to_datetime(dropoff_ns, unit="ns"),
            "distance_km": np.round(road_distance, 3),
            "duration_min": np.round(durations, 3),
            "speed_kmh": np.round(speeds, 3),
            "occupancy": occupancies.astype(int),
            "hour": hours.astype(int),
            "day_of_week": ((start_weekday + day_offsets) % 7).astype(int),
            "day_type": np.where(is_weekend, "weekend", "weekday"),
            "is_peak": is_peak,
            "origin_borough": boroughs[origins],
            "origin_zone_name": zone_names[origins],
            "destination_borough": boroughs[destinations],
            "destination_zone_name": zone_names[destinations],
            "congestion_index": np.round(congestion_index, 4),
        }
    )

    pings_per_trip = rng.integers(pings_min, pings_max + 1, size=trips)
    total_readings = int(pings_per_trip.sum())
    reading_ids = []
    vehicles = []
    reading_zones = []
    reading_times = []
    reading_lats = []
    reading_lons = []
    reading_speeds = []
    headings = []
    reading_occupancy = []
    reading_trip_ids = []

    headings_by_trip = np.array(
        [
            _bearing_degrees(lat[origins[i]], lon[origins[i]], lat[destinations[i]], lon[destinations[i]])
            for i in range(trips)
        ]
    )

    for i in range(trips):
        count = int(pings_per_trip[i])
        fractions = np.sort(rng.uniform(0.03, 0.97, size=count))
        if count == 1:
            fractions = np.array([0.5])
        times = pickup_ns[i] + (fractions * (dropoff_ns[i] - pickup_ns[i])).astype(np.int64)
        o_idx, d_idx = int(origins[i]), int(destinations[i])
        base_speed = float(trips_df.at[i, "speed_kmh"])
        heading = float(headings_by_trip[i])
        for k in range(count):
            f = float(fractions[k])
            reading_ids.append(f"{trip_ids[i]}-{k:02d}")
            vehicles.append(str(vehicle_ids[i]))
            reading_zones.append(int(location_ids[o_idx] if f < 0.5 else location_ids[d_idx]))
            reading_times.append(int(times[k]))
            reading_lats.append(float(lat[o_idx] + (lat[d_idx] - lat[o_idx]) * f + rng.normal(0, 0.0004)))
            reading_lons.append(float(lon[o_idx] + (lon[d_idx] - lon[o_idx]) * f + rng.normal(0, 0.0004)))
            reading_speeds.append(float(np.clip(base_speed * (0.85 + 0.30 * rng.random()), 3.0, 70.0)))
            headings.append(heading)
            reading_occupancy.append(int(occupancies[i]))
            reading_trip_ids.append(str(trip_ids[i]))

    readings_df = pd.DataFrame(
        {
            "reading_id": reading_ids,
            "vehicle_id": vehicles,
            "zone_id": np.array(reading_zones, dtype=int),
            "event_time": pd.to_datetime(np.array(reading_times, dtype=np.int64), unit="ns"),
            "lat": np.round(np.array(reading_lats, dtype=float), 6),
            "lon": np.round(np.array(reading_lons, dtype=float), 6),
            "speed_kmh": np.round(np.array(reading_speeds, dtype=float), 3),
            "heading": np.round(np.array(headings, dtype=float), 2),
            "occupancy": np.array(reading_occupancy, dtype=int),
            "trip_id": reading_trip_ids,
        }
    )

    trips_path = out / "trips.parquet"
    readings_path = out / "gps_readings.parquet"
    trips_df.to_parquet(trips_path, index=False)
    readings_df.to_parquet(readings_path, index=False)
    readings_df.head(2_000).to_csv(out / "gps_readings_sample.csv", index=False)

    metadata = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "seed": seed,
        "start_date": start.strftime("%Y-%m-%d"),
        "days": days,
        "trips": int(len(trips_df)),
        "readings": int(len(readings_df)),
        "pings_min": pings_min,
        "pings_max": pings_max,
        "fleet_size": fleet_size,
        "zones": int(n),
        "borough_trip_counts": trips_df["origin_borough"].value_counts().to_dict(),
        "readings_by_zone": int(readings_df["zone_id"].nunique()),
        "volume_ok": bool(len(readings_df) >= 500_000),
        "files": {
            "trips": str(trips_path),
            "gps_readings": str(readings_path),
            "zone_centroids": str(ZONE_CENTROIDS_CSV),
        },
    }
    (out / "generator_metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return metadata


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generador sintetico de movilidad urbana NYC")
    parser.add_argument("--trips", type=int, default=500_000, help="Numero de viajes")
    parser.add_argument("--pings-min", type=int, default=4, help="Pings minimos por viaje")
    parser.add_argument("--pings-max", type=int, default=7, help="Pings maximos por viaje")
    parser.add_argument("--seed", type=int, default=42, help="Semilla reproducible")
    parser.add_argument("--start-date", default="2026-11-14", help="Fecha inicial YYYY-MM-DD (default: 2026-11-14)")
    parser.add_argument("--days", type=int, default=6, help="Dias simulados")
    parser.add_argument("--fleet-size", type=int, default=5_000, help="Vehiculos simulados")
    parser.add_argument("--out", default=str(SYNTHETIC_DIR), help="Directorio de salida")
    parser.add_argument("--zones", default=None, help="CSV de zonas alternativo")
    parser.add_argument("--centroids", default=None, help="CSV de centroides alternativo")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    metadata = generate_dataset(
        trips=args.trips,
        pings_min=args.pings_min,
        pings_max=args.pings_max,
        seed=args.seed,
        start_date=args.start_date,
        days=args.days,
        fleet_size=args.fleet_size,
        out_dir=args.out,
        zones_path=args.zones,
        centroids_path=args.centroids,
    )
    print(json.dumps(metadata, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import DATA_DIR, ZONE_CENTROIDS_CSV, ZONE_LOOKUP_CSV

BOROUGH_CENTERS = {
    "Manhattan": (40.7831, -73.9712),
    "Brooklyn": (40.6782, -73.9442),
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Staten Island": (40.5795, -74.1502),
    "EWR": (40.6895, -74.1745),
    "Unknown": (40.7500, -73.9900),
}

BOROUGH_DEMAND = {
    "Manhattan": 5.0,
    "Brooklyn": 3.0,
    "Queens": 2.5,
    "Bronx": 1.5,
    "Staten Island": 0.8,
    "EWR": 0.5,
    "Unknown": 1.0,
}


def _stable_unit(value: str) -> float:
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def load_zones(path: Path | str | None = None) -> pd.DataFrame:
    """Load and normalize the NYC taxi zone lookup table."""
    csv_path = Path(path) if path else ZONE_LOOKUP_CSV
    if not csv_path.exists() and ZONE_LOOKUP_CSV.exists():
        csv_path = ZONE_LOOKUP_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"No se encontro {csv_path}. Copia taxi_zone_lookup.csv a la carpeta data/."
        )
    zones = pd.read_csv(csv_path)
    zones.columns = [c.strip().lower() for c in zones.columns]
    rename = {
        "locationid": "location_id",
        "location_id": "location_id",
        "zone": "zone",
        "borough": "borough",
        "service_zone": "service_zone",
    }
    zones = zones.rename(columns={k: v for k, v in rename.items() if k in zones.columns})
    required = {"location_id", "borough", "zone", "service_zone"}
    missing = required.difference(zones.columns)
    if missing:
        raise ValueError(f"Faltan columnas en el CSV de zonas: {sorted(missing)}")
    zones["location_id"] = zones["location_id"].astype(int)
    zones["borough"] = zones["borough"].fillna("Unknown").astype(str)
    zones["zone"] = zones["zone"].fillna("Unknown").astype(str)
    zones["service_zone"] = zones["service_zone"].fillna("Unknown").astype(str)
    return zones.sort_values("location_id").reset_index(drop=True)


def build_zone_coordinates(zones: pd.DataFrame, centroids_path: Path | str | None = None) -> pd.DataFrame:
    """Return one coordinate per zone.

    If data/zone_centroids.csv exists it is used as the source of truth (the
    optional scripts/fetch_zone_geometry.py can build it from the official TLC
    shapefile). Otherwise a deterministic synthetic coordinate is generated
    inside the borough bounding box so the demo remains reproducible offline.
    """
    path = Path(centroids_path) if centroids_path else ZONE_CENTROIDS_CSV
    if path.exists():
        coords = pd.read_csv(path)
        coords.columns = [c.strip().lower() for c in coords.columns]
        if {"location_id", "lat", "lon"}.issubset(coords.columns):
            merged = zones.merge(
                coords[["location_id", "lat", "lon"]], on="location_id", how="left"
            )
            if merged["lat"].notna().all() and merged["lon"].notna().all():
                return merged[["location_id", "borough", "zone", "lat", "lon"]]

    rows = []
    for row in zones.itertuples(index=False):
        base_lat, base_lon = BOROUGH_CENTERS.get(row.borough, BOROUGH_CENTERS["Unknown"])
        u = _stable_unit(f"lat-{row.location_id}")
        v = _stable_unit(f"lon-{row.location_id}")
        lat = base_lat + (u - 0.5) * 0.09
        lon = base_lon + (v - 0.5) * 0.11
        rows.append(
            {
                "location_id": int(row.location_id),
                "borough": row.borough,
                "zone": row.zone,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
            }
        )
    return pd.DataFrame(rows)


def save_zone_coordinates(coords: pd.DataFrame, path: Path | str | None = None) -> Path:
    path = Path(path) if path else ZONE_CENTROIDS_CSV
    path.parent.mkdir(parents=True, exist_ok=True)
    coords.to_csv(path, index=False)
    return path


def haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    radius = 6371.0088
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return radius * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def distance_matrix(coords: pd.DataFrame) -> np.ndarray:
    lat = coords["lat"].to_numpy(dtype=float)
    lon = coords["lon"].to_numpy(dtype=float)
    return haversine_km(lat[:, None], lon[:, None], lat[None, :], lon[None, :])


def zone_weights(zones: pd.DataFrame) -> np.ndarray:
    weights = zones["borough"].map(BOROUGH_DEMAND).fillna(1.0).to_numpy(dtype=float)
    variation = np.array([0.65 + 0.70 * _stable_unit(f"demand-{z}") for z in zones["location_id"]])
    weights = weights * variation
    return weights / weights.sum()


def zone_coordinate_map(zones: pd.DataFrame, coords: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    merged = zones[["location_id"]].merge(coords[["location_id", "lat", "lon"]], on="location_id", how="left")
    if merged["lat"].isna().any():
        raise ValueError("No hay coordenadas para todas las zonas.")
    return merged["lat"].to_numpy(dtype=float), merged["lon"].to_numpy(dtype=float)


def default_zone_csv() -> Path:
    return ZONE_LOOKUP_CSV if ZONE_LOOKUP_CSV.exists() else DATA_DIR / "taxi_zone_lookup.csv"

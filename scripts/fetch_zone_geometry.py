from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

import requests

from src.config import ZONE_CENTROIDS_CSV

TLC_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


def fetch_centroids(output: Path = ZONE_CENTROIDS_CSV) -> Path:
    try:
        import geopandas as gpd
    except ImportError:
        print(
            "GeoPandas no esta instalado. Instala con `pip install geopandas` "
            "o usa el generador con coordenadas sinteticas por borough.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    response = requests.get(TLC_ZONES_URL, timeout=60)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        shapefile = next(name for name in archive.namelist() if name.endswith(".shp"))
        archive.extractall(Path(output).parent / "_taxi_zones")

    shp_path = Path(output).parent / "_taxi_zones" / shapefile
    zones = gpd.read_file(shp_path)
    if zones.crs is None:
        zones = zones.set_crs(4326, allow_override=True)
    zones = zones.to_crs(4326)
    id_column = next(
        (col for col in zones.columns if col.lower() == "locationid"),
        None,
    )
    if id_column is None:
        raise ValueError("No se encontro la columna LocationID en el shapefile")

    centroids = zones.geometry.centroid
    result = zones[[id_column]].rename(columns={id_column: "location_id"})
    result["lat"] = centroids.y
    result["lon"] = centroids.x
    result["location_id"] = result["location_id"].astype(int)
    result = result.sort_values("location_id")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False)
    print(f"Centroides guardados en {output} ({len(result)} zonas)")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Descargar centroides oficiales de zonas TLC")
    parser.add_argument("--output", default=str(ZONE_CENTROIDS_CSV))
    args = parser.parse_args()
    fetch_centroids(Path(args.output))


if __name__ == "__main__":
    main()

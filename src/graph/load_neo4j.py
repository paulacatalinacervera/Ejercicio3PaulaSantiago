from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase

from src.config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER, PROCESSED_DIR, ZONE_CENTROIDS_CSV
from src.zones import build_zone_coordinates, load_zones


def _zone_rows(processed_dir: Path) -> list[dict]:
    zones_file = processed_dir / "zones.parquet"
    if zones_file.exists():
        zones = pd.read_parquet(zones_file)
    else:
        zones = build_zone_coordinates(load_zones(), ZONE_CENTROIDS_CSV)
    return zones[["location_id", "borough", "zone", "service_zone", "lat", "lon"]].to_dict("records")


def _edge_rows(processed_dir: Path) -> list[dict]:
    edges_file = processed_dir / "od_edges.parquet"
    if not edges_file.exists():
        raise FileNotFoundError(f"No existe {edges_file}. Ejecuta `python -m src.batch`.")
    edges = pd.read_parquet(edges_file)
    return edges[
        ["origin_zone", "destination_zone", "trips", "avg_duration_min", "avg_distance_km", "peak_trips"]
    ].to_dict("records")


def load_graph(processed_dir: Path | str = PROCESSED_DIR, clear: bool = False) -> dict:
    processed = Path(processed_dir)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    zones = _zone_rows(processed)
    edges = _edge_rows(processed)

    try:
        with driver.session() as session:
            session.run(
                "CREATE CONSTRAINT zone_id IF NOT EXISTS "
                "FOR (z:Zone) REQUIRE z.location_id IS UNIQUE"
            )
            if clear:
                session.run("MATCH (n:Zone) DETACH DELETE n")
                print("Nodos Zone eliminados")

            session.run(
                """
                UNWIND $rows AS row
                MERGE (z:Zone {location_id: row.location_id})
                SET z.borough = row.borough,
                    z.zone = row.zone,
                    z.service_zone = row.service_zone,
                    z.lat = row.lat,
                    z.lon = row.lon
                """,
                rows=zones,
            )
            print(f"Zonas cargadas: {len(zones)}")

            for start in range(0, len(edges), 1_000):
                batch = edges[start:start + 1_000]
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (o:Zone {location_id: row.origin_zone})
                    MATCH (d:Zone {location_id: row.destination_zone})
                    MERGE (o)-[r:TRIP]->(d)
                    SET r.trips = row.trips,
                        r.avg_duration_min = row.avg_duration_min,
                        r.avg_distance_km = row.avg_distance_km,
                        r.peak_trips = row.peak_trips
                    """,
                    rows=batch,
                )
                print(f"Relaciones cargadas: {min(start + 1_000, len(edges)):,}/{len(edges):,}")

            counts = session.run(
                "MATCH (z:Zone) WITH count(z) AS zones "
                "MATCH ()-[r:TRIP]->() RETURN zones, count(r) AS edges"
            ).single()
            return {"zones": counts["zones"], "edges": counts["edges"]}
    finally:
        driver.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cargar estaciones y rutas en Neo4j")
    parser.add_argument("--processed-dir", default=str(PROCESSED_DIR))
    parser.add_argument("--clear", action="store_true", help="Borrar nodos Zone antes de cargar")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    print(load_graph(args.processed_dir, clear=args.clear))


if __name__ == "__main__":
    main()

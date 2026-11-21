from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase

from src.config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER, RESULTS_DIR


def _run(session, query: str, **params):
    return session.run(query, **params).data()


def run_centrality(
    origin: int = 161,
    destination: int = 132,
    graph_name: str = "zoneGraph",
) -> dict:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    results = Path(RESULTS_DIR)
    results.mkdir(parents=True, exist_ok=True)
    summary: dict = {}

    try:
        with driver.session() as session:
            exists = session.run(
                "CALL gds.graph.exists($name) YIELD exists RETURN exists", name=graph_name
            ).single()["exists"]
            if exists:
                session.run("CALL gds.graph.drop($name, false)", name=graph_name)

            session.run(
                """
                CALL gds.graph.project(
                  $name,
                  'Zone',
                  {TRIP: {orientation: 'NATURAL', properties: ['trips', 'avg_duration_min']}}
                )
                """,
                name=graph_name,
            )
            print(f"Grafo proyectado: {graph_name}")

            pagerank = session.run(
                """
                CALL gds.pageRank.write($name, {
                  writeProperty: 'pagerank',
                  relationshipWeightProperty: 'trips',
                  maxIterations: 20,
                  dampingFactor: 0.85
                })
                YIELD nodePropertiesWritten, ranIterations
                RETURN nodePropertiesWritten, ranIterations
                """,
                name=graph_name,
            ).single()
            print(f"PageRank: {pagerank}")

            try:
                betweenness = session.run(
                    """
                    CALL gds.betweenness.write($name, {
                      writeProperty: 'betweenness',
                      relationshipWeightProperty: 'avg_duration_min'
                    })
                    YIELD nodePropertiesWritten
                    RETURN nodePropertiesWritten
                    """,
                    name=graph_name,
                ).single()
            except Exception as exc:  # noqa: BLE001
                print(f"Intermediacion ponderada no disponible ({exc}); usando no ponderada")
                betweenness = session.run(
                    """
                    CALL gds.betweenness.write($name, {writeProperty: 'betweenness'})
                    YIELD nodePropertiesWritten
                    RETURN nodePropertiesWritten
                    """,
                    name=graph_name,
                ).single()
            print(f"Betweenness: {betweenness}")

            top_pagerank = _run(
                session,
                """
                MATCH (z:Zone)
                WHERE z.pagerank IS NOT NULL
                RETURN z.location_id AS location_id, z.zone AS zone, z.borough AS borough,
                       z.pagerank AS pagerank
                ORDER BY pagerank DESC
                LIMIT 10
                """,
            )
            top_betweenness = _run(
                session,
                """
                MATCH (z:Zone)
                WHERE z.betweenness IS NOT NULL
                RETURN z.location_id AS location_id, z.zone AS zone, z.borough AS borough,
                       z.betweenness AS betweenness
                ORDER BY betweenness DESC
                LIMIT 10
                """,
            )

            pr_df = pd.DataFrame(top_pagerank)
            bt_df = pd.DataFrame(top_betweenness)
            metrics = pr_df.merge(bt_df, on=["location_id", "zone", "borough"], how="outer")
            metrics.to_csv(results / "graph_metrics.csv", index=False)

            path = _run(
                session,
                """
                MATCH (s:Zone {location_id: $origin}), (t:Zone {location_id: $destination})
                CALL gds.shortestPath.dijkstra.stream($name, {
                  sourceNode: id(s),
                  targetNode: id(t),
                  relationshipWeightProperty: 'avg_duration_min'
                })
                YIELD path, totalCost
                RETURN [n IN nodes(path) | n.zone] AS route,
                       [n IN nodes(path) | n.location_id] AS ids,
                       totalCost
                """,
                name=graph_name,
                origin=origin,
                destination=destination,
            )
            shortest = path[0] if path else {}
            (results / "shortest_path.json").write_text(
                json.dumps(shortest, indent=2, ensure_ascii=False), encoding="utf-8"
            )

            summary = {
                "pagerank": pagerank,
                "betweenness": betweenness,
                "top_pagerank": top_pagerank,
                "top_betweenness": top_betweenness,
                "shortest_path": shortest,
            }
            print("\nTop PageRank")
            print(pr_df.to_string(index=False))
            print("\nTop intermediacion")
            print(bt_df.to_string(index=False))
            print("\nRuta mas corta")
            print(json.dumps(shortest, indent=2, ensure_ascii=False))
    finally:
        driver.close()
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calcular PageRank, intermediacion y ruta mas corta con GDS")
    parser.add_argument("--origin", type=int, default=161, help="Zona origen (default 161 Midtown Center)")
    parser.add_argument("--destination", type=int, default=132, help="Zona destino (default 132 JFK)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_centrality(args.origin, args.destination)


if __name__ == "__main__":
    main()

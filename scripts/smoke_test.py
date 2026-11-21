from __future__ import annotations

import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import (
    CASSANDRA_HOST,
    CASSANDRA_PORT,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    PROCESSED_DIR,
    S3_BUCKET,
    SYNTHETIC_DIR,
)

STATUS: list[tuple[str, bool, str]] = []


def check(name: str, fn) -> None:
    try:
        detail = fn()
        STATUS.append((name, True, str(detail or "OK")))
    except Exception as exc:  # noqa: BLE001
        STATUS.append((name, False, str(exc)))


def port_check(host: str, port: int):
    with socket.create_connection((host, port), timeout=3):
        return "puerto abierto"


def check_files():
    required = [
        SYNTHETIC_DIR / "trips.parquet",
        SYNTHETIC_DIR / "gps_readings.parquet",
        PROCESSED_DIR / "trips_flat" / "trips.parquet",
        PROCESSED_DIR / "od_edges.parquet",
        PROCESSED_DIR / "hourly_zone_stats.parquet",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(", ".join(missing))
    return "archivos generados"


def check_kafka():
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="smoke")
    try:
        topics = admin.list_topics()
        if KAFKA_TOPIC not in topics:
            raise RuntimeError(f"Falta el topic {KAFKA_TOPIC}")
        return f"{len(topics)} topics"
    finally:
        admin.close()


def check_cassandra():
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect()
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name='mobility'"
        )
        tables = {row.table_name for row in rows}
        expected = {
            "sensor_readings_by_vehicle",
            "congestion_by_zone_window",
            "city_congestion_by_window",
        }
        missing = expected - tables
        if missing:
            raise RuntimeError(f"Faltan tablas: {sorted(missing)}")
        return f"{len(tables)} tablas"
    finally:
        cluster.shutdown()


def check_neo4j():
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            row = session.run("MATCH (z:Zone) RETURN count(z) AS zones").single()
            edges = session.run("MATCH ()-[r:TRIP]->() RETURN count(r) AS edges").single()
            return f"{row['zones']} zonas, {edges['edges']} relaciones"
    finally:
        driver.close()


def check_s3():
    from src.s3_utils import ensure_bucket, get_s3_client

    client = get_s3_client()
    ensure_bucket(bucket=S3_BUCKET, create=False, client=client)
    response = client.list_objects_v2(Bucket=S3_BUCKET, Prefix="", MaxKeys=1)
    return f"bucket {S3_BUCKET} accesible ({response.get('KeyCount', 0)} objetos en muestra)"


def main() -> None:
    kafka_host, kafka_port = KAFKA_BOOTSTRAP.split(":")[0], int(KAFKA_BOOTSTRAP.split(":")[1])
    neo4j_host = NEO4J_URI.replace("bolt://", "").split(":")[0]
    neo4j_port = int(NEO4J_URI.rsplit(":", 1)[1])

    check("Archivos de datos", check_files)
    check("Kafka", lambda: port_check(kafka_host, kafka_port))
    check("Cassandra", lambda: port_check(CASSANDRA_HOST, CASSANDRA_PORT))
    check("Neo4j", lambda: port_check(neo4j_host, neo4j_port))
    check("Topic Kafka", check_kafka)
    check("Esquema Cassandra", check_cassandra)
    check("Grafo Neo4j", check_neo4j)
    check("Bucket S3", check_s3)

    failures = 0
    for name, ok, detail in STATUS:
        print(f"[{'OK' if ok else 'FAIL'}] {name}: {detail}")
        failures += 0 if ok else 1
    print(f"\n{len(STATUS) - failures}/{len(STATUS)} comprobaciones OK")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()

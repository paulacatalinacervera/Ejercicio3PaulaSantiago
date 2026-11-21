from __future__ import annotations

import argparse
import socket
import sys
import time
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
    S3_ENDPOINT_URL,
    SQL_DIR,
)


def wait_port(host: str, port: int, timeout: int = 120, label: str = "") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"OK {label or host}:{port}")
                return
        except OSError:
            time.sleep(2)
    raise TimeoutError(f"No responde {label or host}:{port} despues de {timeout}s")


def _statements(path: Path, comment_prefix: str = "--") -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if not line.strip().startswith(comment_prefix)]
    return [part.strip() for part in "\n".join(lines).split(";") if part.strip()]


def init_kafka() -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="mobility-bootstrap")
    try:
        admin.create_topics(
            [NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)],
            validate_only=False,
        )
        print(f"Topic creado: {KAFKA_TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Topic existente: {KAFKA_TOPIC}")
    finally:
        admin.close()


def init_cassandra() -> None:
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    try:
        session = cluster.connect()
        for statement in _statements(SQL_DIR / "cassandra_schema.cql"):
            session.execute(statement)
        print("Esquema Cassandra aplicado")
    finally:
        cluster.shutdown()


def init_neo4j() -> None:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            for statement in _statements(SQL_DIR / "neo4j_constraints.cypher"):
                session.run(statement)
        print("Restricciones Neo4j aplicadas")
    finally:
        driver.close()


def init_s3(create_bucket: bool = False) -> None:
    from src.s3_utils import ensure_bucket, get_s3_client

    client = get_s3_client()
    try:
        bucket = ensure_bucket(create=create_bucket or bool(S3_ENDPOINT_URL), client=client)
        print(f"Bucket disponible: {bucket}")
    except Exception as exc:  # noqa: BLE001
        print(f"AVISO S3: {exc}. Crea el bucket o usa --create-bucket.", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inicializar topic Kafka, esquema Cassandra, Neo4j y bucket S3")
    parser.add_argument("--create-bucket", action="store_true")
    parser.add_argument("--skip-s3", action="store_true")
    parser.add_argument("--skip-cassandra", action="store_true")
    parser.add_argument("--skip-neo4j", action="store_true")
    parser.add_argument("--skip-kafka", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    kafka_host, kafka_port = KAFKA_BOOTSTRAP.split(":")[0], int(KAFKA_BOOTSTRAP.split(":")[1])
    neo4j_host = NEO4J_URI.replace("bolt://", "").split(":")[0]
    neo4j_port = int(NEO4J_URI.rsplit(":", 1)[1])

    if not args.skip_kafka:
        wait_port(kafka_host, kafka_port, label="Kafka")
        init_kafka()
    if not args.skip_cassandra:
        wait_port(CASSANDRA_HOST, CASSANDRA_PORT, label="Cassandra")
        init_cassandra()
    if not args.skip_neo4j:
        wait_port(neo4j_host, neo4j_port, label="Neo4j")
        init_neo4j()
    if not args.skip_s3:
        init_s3(args.create_bucket)
    print("Bootstrap completado")


if __name__ == "__main__":
    main()

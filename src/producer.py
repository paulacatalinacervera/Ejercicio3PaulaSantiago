from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, SYNTHETIC_DIR


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="mobility-admin")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)],
            validate_only=False,
        )
        print(f"Topic creado: {topic}")
    except TopicAlreadyExistsError:
        print(f"Topic existente: {topic}")
    finally:
        admin.close()


def serialize_event(row: dict) -> bytes:
    payload = {
        "reading_id": str(row["reading_id"]),
        "vehicle_id": str(row["vehicle_id"]),
        "zone_id": int(row["zone_id"]),
        "event_time": pd.Timestamp(row["event_time"]).strftime("%Y-%m-%dT%H:%M:%S"),
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "speed_kmh": float(row["speed_kmh"]),
        "heading": float(row["heading"]),
        "occupancy": int(row["occupancy"]),
        "trip_id": str(row["trip_id"]),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def replay(
    readings_path: Path | str | None = None,
    limit: int | None = 200_000,
    rate: float = 2_000.0,
    bootstrap: str = KAFKA_BOOTSTRAP,
    topic: str = KAFKA_TOPIC,
    create_topic: bool = True,
) -> int:
    path = Path(readings_path) if readings_path else SYNTHETIC_DIR / "gps_readings.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}. Ejecuta primero el generador.")

    df = pd.read_parquet(path, columns=[
        "reading_id", "vehicle_id", "zone_id", "event_time",
        "lat", "lon", "speed_kmh", "heading", "occupancy", "trip_id",
    ]).sort_values("event_time")
    if limit and limit > 0:
        df = df.head(limit)

    if create_topic:
        ensure_topic(bootstrap, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks=1,
        linger_ms=5,
        key_serializer=lambda v: str(v).encode("utf-8"),
        value_serializer=lambda v: v,
    )

    started = time.perf_counter()
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    sent = 0
    for _, row in df.iterrows():
        producer.send(topic, key=str(row["vehicle_id"]), value=serialize_event(row))
        sent += 1
        if interval:
            elapsed = time.perf_counter() - started
            expected = sent * interval
            if expected > elapsed:
                time.sleep(expected - elapsed)
        if sent % 10_000 == 0:
            elapsed = max(time.perf_counter() - started, 1e-6)
            print(f"Enviados {sent:,} eventos ({sent / elapsed:,.0f} eventos/s)")
    producer.flush()
    producer.close()
    elapsed = time.perf_counter() - started
    print(f"Total enviado: {sent:,} eventos en {elapsed:.1f}s")
    return sent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reproducir lecturas GPS sinteticas en Kafka")
    parser.add_argument("--readings", default=str(SYNTHETIC_DIR / "gps_readings.parquet"))
    parser.add_argument("--limit", type=int, default=200_000, help="0 = todas las lecturas")
    parser.add_argument("--rate", type=float, default=2_000.0, help="Eventos por segundo; 0 = sin limite")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--no-create-topic", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    replay(
        readings_path=args.readings,
        limit=args.limit,
        rate=args.rate,
        bootstrap=args.bootstrap,
        topic=args.topic,
        create_topic=not args.no_create_topic,
    )


if __name__ == "__main__":
    main()

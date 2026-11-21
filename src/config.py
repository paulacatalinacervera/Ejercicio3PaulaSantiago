from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
SYNTHETIC_DIR = DATA_DIR / "synthetic"
PROCESSED_DIR = DATA_DIR / "processed"
RESULTS_DIR = ROOT_DIR / "results"
SQL_DIR = ROOT_DIR / "sql"

ZONE_LOOKUP_CSV = DATA_DIR / "taxi_zone_lookup.csv"
ZONE_CENTROIDS_CSV = DATA_DIR / "zone_centroids.csv"

load_dotenv(ROOT_DIR / ".env")


def env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def ensure_directories() -> None:
    for path in (DATA_DIR, SYNTHETIC_DIR, PROCESSED_DIR, RESULTS_DIR):
        path.mkdir(parents=True, exist_ok=True)


AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
AWS_REGION = env("AWS_REGION", "us-east-1")
S3_BUCKET = env("S3_BUCKET", "mobility-local")
S3_PREFIX = env("S3_PREFIX", "mobility")
S3_ENDPOINT_URL = env("S3_ENDPOINT_URL")
S3_ENDPOINT_URL_DOCKER = env("S3_ENDPOINT_URL_DOCKER")
LOCAL_MODE = env_bool("LOCAL_MODE", True)

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", "mobility.gps")

CASSANDRA_HOST = env("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = env_int("CASSANDRA_PORT", 9042)
CASSANDRA_KEYSPACE = env("CASSANDRA_KEYSPACE", "mobility")

NEO4J_URI = env("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = env("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = env("NEO4J_PASSWORD", "mobility123")

GCP_PROJECT_ID = env("GCP_PROJECT_ID")
GCP_DATASET = env("GCP_DATASET", "mobility")
GCP_LOCATION = env("GCP_LOCATION", "US")
GCS_BUCKET = env("GCS_BUCKET")
GCS_PREFIX = env("GCS_PREFIX", "mobility")

SIM_DAY = env("SIM_DAY")

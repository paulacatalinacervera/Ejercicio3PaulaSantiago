from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from src.config import (  # noqa: E402
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    CASSANDRA_HOST,
    CASSANDRA_KEYSPACE,
    CASSANDRA_PORT,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    LOCAL_MODE,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_PREFIX,
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("reading_id", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("zone_id", T.IntegerType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("speed_kmh", T.DoubleType()),
        T.StructField("heading", T.DoubleType()),
        T.StructField("occupancy", T.IntegerType()),
        T.StructField("trip_id", T.StringType()),
    ]
)


def spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("MobilityStreaming")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.checkpointLocation", args.checkpoint_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.cassandra.connection.host", args.cassandra_host)
        .config("spark.cassandra.connection.port", str(args.cassandra_port))
        .config("spark.cassandra.output.batch.size.rows", "100")
        .config("spark.cassandra.output.concurrent.writes", "5")
    )

    endpoint = args.s3_endpoint or S3_ENDPOINT_URL
    if endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
    if AWS_REGION:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config("spark.hadoop.fs.s3a.region", AWS_REGION)
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_cassandra(df: DataFrame, table: str, keyspace: str) -> None:
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .options(keyspace=keyspace, table=table)
        .mode("append")
        .save()
    )


def build_window_output(parsed: DataFrame) -> DataFrame:
    windowed = (
        parsed.withWatermark("event_ts", "2 minutes")
        .groupBy(F.window(F.col("event_ts"), "1 minute"), F.col("zone_id"))
        .agg(
            F.count("*").alias("readings"),
            F.approx_count_distinct("vehicle_id").alias("vehicles"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.avg("occupancy").alias("avg_occupancy"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "readings",
            "vehicles",
            F.round("avg_speed_kmh", 3).alias("avg_speed_kmh"),
            F.round("avg_occupancy", 3).alias("avg_occupancy"),
        )
    )
    return (
        windowed.withColumn(
            "congestion_index",
            F.round(
                F.greatest(
                    F.lit(0.0),
                    F.least(F.lit(1.0), F.lit(1.0) - F.col("avg_speed_kmh") / F.lit(35.0)),
                ),
                4,
            ),
        )
        .withColumn("day_bucket", F.date_format("window_start", "yyyyMMdd"))
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming de movilidad urbana")
    parser.add_argument("--kafka", default=os.getenv("KAFKA_BOOTSTRAP", KAFKA_BOOTSTRAP))
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--cassandra-host", default=os.getenv("CASSANDRA_HOST", CASSANDRA_HOST))
    parser.add_argument("--cassandra-port", type=int, default=CASSANDRA_PORT)
    parser.add_argument("--cassandra-keyspace", default=CASSANDRA_KEYSPACE)
    parser.add_argument("--checkpoint-dir", default=str(ROOT / "checkpoints"))
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--window-output", default=None)
    parser.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT_URL", S3_ENDPOINT_URL))
    parser.add_argument("--write-raw-s3", action="store_true")
    return parser


def default_window_output() -> str:
    if S3_ENDPOINT_URL or not LOCAL_MODE:
        return f"s3a://{S3_BUCKET}/{S3_PREFIX}/processed/streaming_windows"
    return str(ROOT / "data" / "processed" / "streaming_windows")


def main() -> None:
    args = build_parser().parse_args()
    output_path = args.window_output or default_window_output()
    raw_output = f"s3a://{S3_BUCKET}/{S3_PREFIX}/streaming/raw"
    Path(args.checkpoint_dir).mkdir(parents=True, exist_ok=True)

    spark = spark_session(args)
    print(f"Kafka: {args.kafka} | topic: {args.topic}")
    print(f"Cassandra: {args.cassandra_host}:{args.cassandra_port}/{args.cassandra_keyspace}")
    print(f"Salida de ventanas: {output_path}")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_df.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .filter(F.col("event_ts").isNotNull() & F.col("zone_id").between(1, 265))
        .withColumn("day_bucket", F.date_format("event_ts", "yyyyMMdd"))
    )

    raw_columns = [
        "reading_id", "vehicle_id", "day_bucket", "event_time", "zone_id",
        "lat", "lon", "speed_kmh", "occupancy", "trip_id",
    ]
    raw_df = parsed.select(
        "reading_id", "vehicle_id", "day_bucket",
        F.col("event_ts").alias("event_time"), "zone_id",
        "lat", "lon", "speed_kmh", "occupancy", "trip_id",
    )

    window_df = build_window_output(parsed)

    def write_raw(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        write_cassandra(batch_df.select(*raw_columns), "sensor_readings_by_vehicle", args.cassandra_keyspace)
        if args.write_raw_s3:
            (
                batch_df.write.mode("append")
                .partitionBy("day_bucket")
                .parquet(f"{raw_output}/batch_id={batch_id}")
            )
        if batch_id % 5 == 0:
            count = batch_df.count()
            print(f"[raw] batch={batch_id} filas={count:,}")

    def write_windows(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        zone_df = batch_df.select(
            "zone_id", "day_bucket", "window_start", "window_end",
            "readings", "vehicles", "avg_speed_kmh", "avg_occupancy", "congestion_index",
        )
        city_df = batch_df.select(
            "day_bucket", "window_start", "zone_id",
            "readings", "avg_speed_kmh", "congestion_index",
        )
        write_cassandra(zone_df, "congestion_by_zone_window", args.cassandra_keyspace)
        write_cassandra(city_df, "city_congestion_by_window", args.cassandra_keyspace)
        (
            batch_df.write.mode("append")
            .partitionBy("day_bucket")
            .parquet(output_path)
        )
        print(f"\n[ventanas] batch={batch_id}")
        (
            batch_df.orderBy(F.desc("congestion_index"))
            .select("window_start", "zone_id", "readings", "avg_speed_kmh", "congestion_index")
            .show(5, truncate=False)
        )

    raw_query = (
        raw_df.writeStream.outputMode("append")
        .foreachBatch(write_raw)
        .option("checkpointLocation", str(Path(args.checkpoint_dir) / "raw"))
        .trigger(processingTime=args.trigger)
        .start()
    )
    window_query = (
        window_df.writeStream.outputMode("append")
        .foreachBatch(write_windows)
        .option("checkpointLocation", str(Path(args.checkpoint_dir) / "windows"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    print("Streaming iniciado. Ctrl+C para detener.")
    try:
        spark.streams.awaitAnyTermination()
    finally:
        raw_query.stop()
        window_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

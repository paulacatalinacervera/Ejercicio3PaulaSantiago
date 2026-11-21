from __future__ import annotations

import pandas as pd

from src.generator import generate_dataset


def test_generator_volume_schema_and_zones(tmp_path):
    metadata = generate_dataset(
        trips=80,
        pings_min=3,
        pings_max=5,
        seed=7,
        start_date="2026-01-05",
        days=2,
        fleet_size=20,
        out_dir=tmp_path,
    )

    trips = pd.read_parquet(tmp_path / "trips.parquet")
    readings = pd.read_parquet(tmp_path / "gps_readings.parquet")

    assert len(trips) == 80
    assert len(readings) == metadata["readings"]
    assert len(readings) >= 80 * 3
    assert len(readings) <= 80 * 5
    assert readings["zone_id"].between(1, 265).all()
    assert trips["origin_zone"].between(1, 265).all()
    assert trips["destination_zone"].between(1, 265).all()
    assert (trips["duration_min"] > 0).all()
    assert (trips["distance_km"] > 0).all()
    assert readings["event_time"].min() >= pd.Timestamp("2026-01-05")
    assert readings["event_time"].max() <= pd.Timestamp("2026-01-07")
    assert metadata["zones"] == 265


def test_generator_is_reproducible(tmp_path):
    kwargs = dict(
        trips=30,
        pings_min=2,
        pings_max=3,
        seed=123,
        start_date="2026-02-10",
        days=1,
        fleet_size=10,
    )
    generate_dataset(out_dir=tmp_path / "a", **kwargs)
    generate_dataset(out_dir=tmp_path / "b", **kwargs)

    trips_a = pd.read_parquet(tmp_path / "a" / "trips.parquet")
    trips_b = pd.read_parquet(tmp_path / "b" / "trips.parquet")
    readings_a = pd.read_parquet(tmp_path / "a" / "gps_readings.parquet")
    readings_b = pd.read_parquet(tmp_path / "b" / "gps_readings.parquet")

    pd.testing.assert_frame_equal(trips_a, trips_b)
    pd.testing.assert_frame_equal(readings_a, readings_b)

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.cassandra_utils import day_bucket
from src.generator import generate_dataset


def test_day_bucket_formats_dates():
    assert day_bucket("2026-11-13T08:30:00") == "20261113"
    assert day_bucket(datetime(2026, 1, 2, 23, 59)) == "20260102"
    assert day_bucket(pd.Timestamp("2026-07-09")) == "20260709"


def test_vehicle_day_partitions_are_bounded(tmp_path):
    generate_dataset(
        trips=120,
        pings_min=3,
        pings_max=6,
        seed=11,
        start_date="2026-03-02",
        days=1,
        fleet_size=25,
        out_dir=tmp_path,
    )
    readings = pd.read_parquet(tmp_path / "gps_readings.parquet")
    readings["day_bucket"] = readings["event_time"].apply(day_bucket)
    sizes = readings.groupby(["vehicle_id", "day_bucket"]).size()

    assert len(sizes) > 0
    assert sizes.max() < 500
    assert readings["day_bucket"].nunique() == 1
    assert (readings["day_bucket"] == "20260302").all()

from __future__ import annotations

from datetime import date, datetime

import pandas as pd


def day_bucket(value: datetime | date | pd.Timestamp | str) -> str:
    """Partition bucket used to keep Cassandra partitions bounded.

    One partition per (vehicle, day) or (zone, day) is small enough for the
    expected volume and still supports daily time-range queries.
    """
    return pd.Timestamp(value).strftime("%Y%m%d")


def current_day_bucket() -> str:
    return datetime.utcnow().strftime("%Y%m%d")

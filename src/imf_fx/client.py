from __future__ import annotations
from typing import Optional
import polars as pl

from .config import (
    BASE_URL,
    DATAFLOW_AGENCY,
    DATAFLOW_ID,
    FREQUENCY,
    TRANSFORMATION,
    INDICATOR,
)
from .http import get_json_session
from .sdmx import sdmx3_to_tidy

def make_er_key(country_iso3: str) -> str:
    # DSD order: COUNTRY.INDICATOR.TYPE_OF_TRANSFORMATION.FREQUENCY
    return f"{country_iso3}.{INDICATOR}.{TRANSFORMATION}.{FREQUENCY}"

def get_data_key(
    key: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 90,
) -> dict:
    url = f"{BASE_URL}/data/dataflow/{DATAFLOW_AGENCY}/{DATAFLOW_ID}/+/{key}"
    params = {
        "dimensionAtObservation": "TIME_PERIOD",
        "includeHistory": "false",
    }
    if start:
        params["startPeriod"] = start
    if end:
        params["endPeriod"] = end
    return get_json_session(url, params=params, timeout=timeout)

def fetch_country_usd_series(country_iso3: str, start: str, end: str) -> pl.DataFrame:
    key = make_er_key(country_iso3)
    j = get_data_key(key, start=start, end=end, timeout=120)
    return sdmx3_to_tidy(j)
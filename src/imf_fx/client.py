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
    """
    Build the ER series key.

    DSD order: COUNTRY.INDICATOR.TYPE_OF_TRANSFORMATION.FREQUENCY
    Example: USA.XDC_USD.PA_RT.M
    """
    return f"{country_iso3}.{INDICATOR}.{TRANSFORMATION}.{FREQUENCY}"


def get_data_key(
    key: str,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 90,
) -> dict:
    """
    Fetch SDMX 3.0 data for a specific series key.

    start/end are IMF period strings (e.g. '1957-M01'). If omitted, IMF returns available history.
    """
    url = f"{BASE_URL}/data/dataflow/{DATAFLOW_AGENCY}/{DATAFLOW_ID}/+/{key}"
    params: dict[str, str] = {
        "dimensionAtObservation": "TIME_PERIOD",
        "includeHistory": "false",
    }
    if start:
        params["startPeriod"] = start
    if end:
        params["endPeriod"] = end

    return get_json_session(url, params=params, timeout=timeout)


def fetch_country_usd_series(
    country_iso3: str,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    timeout: int = 120,
) -> pl.DataFrame:
    """
    Fetch USD-only monthly ER series for a single IMF COUNTRY code (ISO3-like),
    returning a tidy Polars DataFrame from the SDMX JSON response.

    If start/end are omitted, IMF returns available history.
    """
    key = make_er_key(country_iso3)
    j = get_data_key(key, start=start, end=end, timeout=timeout)
    return sdmx3_to_tidy(j)
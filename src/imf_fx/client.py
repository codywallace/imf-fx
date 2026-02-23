from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from .config import (
    BASE_URL,
    DATAFLOW_AGENCY,
    DATAFLOW_ID,
    FREQUENCY,
    INDICATOR,
    TRANSFORMATION,
)
from .http import get_json_session
from .sdmx import sdmx3_to_tidy


def _is_iso3_like(code: object) -> bool:
    return isinstance(code, str) and len(code) == 3 and code.isalpha()


def _normalize_iso3_list(codes: Sequence[str]) -> list[str]:
    """
    Normalize ISO3-like codes:
    - strip + upper
    - filter to AAA alpha codes
    - de-dupe
    - sort for determinism
    """
    norm = []
    seen = set()
    for c in codes:
        if not isinstance(c, str):
            continue
        c2 = c.strip().upper()
        if not _is_iso3_like(c2):
            continue
        if c2 in seen:
            continue
        seen.add(c2)
        norm.append(c2)
    norm.sort()
    return norm


def make_er_key(country_iso3: str) -> str:
    country_iso3 = country_iso3.strip().upper()
    return f"{country_iso3}.{INDICATOR}.{TRANSFORMATION}.{FREQUENCY}"


def get_data_key(
    key: str,
    start: str | None = None,
    end: str | None = None,
    timeout: int = 90,
) -> dict:
    """
    Fetch SDMX data for the given key via the IMF SDMX 3.0 endpoint.

    Note:
    The IMF endpoint may ignore start/end filtering server-side for some requests.
    We still pass startPeriod/endPeriod, but enforce the window client-side in sdmx parsing.
    """
    url = f"{BASE_URL}/data/dataflow/{DATAFLOW_AGENCY}/{DATAFLOW_ID}/+/{key}"
    params = {"dimensionAtObservation": "TIME_PERIOD", "includeHistory": "false"}
    if start:
        params["startPeriod"] = start
    if end:
        params["endPeriod"] = end
    return get_json_session(url, params=params, timeout=timeout)


def fetch_country_usd_series(
    country_iso3: str,
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
) -> pl.DataFrame:
    """
    Fetch a single COUNTRY series (USD-only, monthly) and return a tidy Polars DataFrame.

    start/end are IMF period strings like '2020-M01'. Filtering is enforced client-side.
    """
    key = make_er_key(country_iso3)
    j = get_data_key(key, start=start, end=end, timeout=timeout)
    return sdmx3_to_tidy(j, min_period=start, max_period=end)


def fetch_countries_usd_series(
    countries_iso3: Sequence[str],
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
    *,
    max_countries_per_request: int = 60,
) -> pl.DataFrame:
    """
    Fetch multiple COUNTRY members in a single SDMX request using '+' in the key.
    Example key:
      USA+JPN+GBR.XDC_USD.PA_RT.M

    - Countries are normalized, de-duped, and sorted for deterministic URLs.
    - If more than max_countries_per_request are provided, raises ValueError.
      (Your datasets.py batching should keep this under control.)
    """
    normalized = _normalize_iso3_list(countries_iso3)
    if not normalized:
        return pl.DataFrame()

    if len(normalized) > max_countries_per_request:
        raise ValueError(
            f"Too many countries for one request ({len(normalized)} > {max_countries_per_request}). "
            "Batch the request (e.g., use datasets.monthly_usd_only with batch_size)."
        )

    country_part = "+".join(normalized)
    key = f"{country_part}.{INDICATOR}.{TRANSFORMATION}.{FREQUENCY}"
    j = get_data_key(key, start=start, end=end, timeout=timeout)
    return sdmx3_to_tidy(j, min_period=start, max_period=end)

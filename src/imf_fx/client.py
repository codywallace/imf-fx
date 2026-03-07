from __future__ import annotations

from collections.abc import Sequence
from typing import Any

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
    norm: list[str] = []
    seen: set[str] = set()
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


def make_er_key(
    country_iso3: str,
    *,
    indicator: str = INDICATOR,
    transformation: str = TRANSFORMATION,
    frequency: str = FREQUENCY,
) -> str:
    country_iso3 = country_iso3.strip().upper()
    return f"{country_iso3}.{indicator}.{transformation}.{frequency}"


def get_data_key(
    key: str,
    start: str | None = None,
    end: str | None = None,
    timeout: int = 90,
    *,
    include_history: bool = False,
) -> dict[str, Any]:
    url = f"{BASE_URL}/data/dataflow/{DATAFLOW_AGENCY}/{DATAFLOW_ID}/+/{key}"
    params: dict[str, Any] = {
        "dimensionAtObservation": "TIME_PERIOD",
        "includeHistory": str(include_history).lower(),
    }
    if start:
        params["startPeriod"] = start
    if end:
        params["endPeriod"] = end
    return get_json_session(url, params=params, timeout=timeout)


def fetch_country_series(
    country_iso3: str,
    *,
    indicator: str = INDICATOR,
    transformation: str = TRANSFORMATION,
    frequency: str = FREQUENCY,
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
    include_history: bool = False,
) -> pl.DataFrame:
    key = make_er_key(
        country_iso3,
        indicator=indicator,
        transformation=transformation,
        frequency=frequency,
    )
    j = get_data_key(key, start=start, end=end, timeout=timeout, include_history=include_history)
    return sdmx3_to_tidy(j, min_period=start, max_period=end)


def fetch_countries_series(
    countries_iso3: Sequence[str],
    *,
    indicator: str = INDICATOR,
    transformation: str = TRANSFORMATION,
    frequency: str = FREQUENCY,
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
    include_history: bool = False,
    max_countries_per_request: int = 60,
) -> pl.DataFrame:
    normalized = _normalize_iso3_list(countries_iso3)
    if not normalized:
        return pl.DataFrame()

    if len(normalized) > max_countries_per_request:
        raise ValueError(
            f"Too many countries for one request ({len(normalized)} > {max_countries_per_request}). "
            "Use batching in fx.exchange_rates(batch_size=...)."
        )

    country_part = "+".join(normalized)
    key = f"{country_part}.{indicator}.{transformation}.{frequency}"
    j = get_data_key(key, start=start, end=end, timeout=timeout, include_history=include_history)
    return sdmx3_to_tidy(j, min_period=start, max_period=end)


# ---- optional compatibility wrappers (can remove in a later major version) ----


def fetch_country_usd_series(
    country_iso3: str, start: str | None = None, end: str | None = None, timeout: int = 120
) -> pl.DataFrame:
    return fetch_country_series(country_iso3, start=start, end=end, timeout=timeout)


def fetch_countries_usd_series(
    countries_iso3: Sequence[str],
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
    *,
    max_countries_per_request: int = 60,
) -> pl.DataFrame:
    return fetch_countries_series(
        countries_iso3,
        start=start,
        end=end,
        timeout=timeout,
        max_countries_per_request=max_countries_per_request,
    )

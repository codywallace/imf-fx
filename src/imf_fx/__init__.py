"""
imf-fx

Lightweight utilities for fetching and transforming IMF SDMX 3.0
monthly USD exchange rate data.

Only the functions imported below are considered part of the
public, stable API.
"""

from .client import fetch_countries_usd_series
from .datasets import monthly_usd_only

__all__ = [
    "monthly_usd_only",
    "fetch_countries_usd_series",
]

__version__ = "0.1.0"

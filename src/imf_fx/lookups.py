from __future__ import annotations

import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path

import requests

from .config import CACHE_DIR, CLDR_URL

CACHE_DIR_PATH = Path(CACHE_DIR)
CACHE_DIR_PATH.mkdir(parents=True, exist_ok=True)
CLDR_CACHE = CACHE_DIR_PATH / "cldr_supplementalData.xml"


def load_cldr_xml(cache_path: Path = CLDR_CACHE, url: str = CLDR_URL) -> str:
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_text(encoding="utf-8")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    cache_path.write_text(r.text, encoding="utf-8")
    return r.text


def build_iso2_to_currency_map_from_cldr(xml_text: str) -> dict[str, str]:
    root = ET.fromstring(xml_text)
    iso2_to_ccy: dict[str, str] = {}

    currency_data = root.find(".//currencyData")
    if currency_data is None:
        raise ValueError("Could not find currencyData in CLDR XML")

    for region in currency_data.findall(".//region"):
        iso2 = region.attrib.get("iso3166")
        if not iso2:
            continue

        current = None
        for cur in region.findall("currency"):
            if "to" not in cur.attrib:  # current currency has no "to" attr
                current = cur
                break

        if current is None:
            continue

        ccy = current.attrib.get("iso4217")
        if ccy:
            iso2_to_ccy[iso2] = ccy

    return iso2_to_ccy


@lru_cache(maxsize=1)
def _iso2_to_currency_map() -> dict[str, str]:
    # Lazy + cached: only downloads/parses CLDR on first actual use
    xml_text = load_cldr_xml()
    return build_iso2_to_currency_map_from_cldr(xml_text)


def iso2_to_currency(iso2: str | None) -> str | None:
    if not isinstance(iso2, str):
        return None
    return _iso2_to_currency_map().get(iso2)

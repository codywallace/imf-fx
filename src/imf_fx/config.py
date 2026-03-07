from __future__ import annotations

import os

BASE_URL = "https://api.imf.org/external/SDMX/3.0"

# Optional:
IMF_API_KEY = os.getenv("IMF_API_KEY")

# Default dataflow
DATAFLOW_AGENCY = "IMF.STA"
DATAFLOW_ID = "ER"

# Default series parameters (fallbacks; fx.exchange_rates overrides these)
FREQUENCY = "M"  # M / Q / A
TRANSFORMATION = "PA_RT"  # PA_RT (period average) / EOP_RT (end-of-period)
INDICATOR = "XDC_USD"  # e.g., XDC_USD, XDC_EUR, XDC_XDR, USD_XDC, etc.

# CLDR for ISO2 -> currency (used for metadata/enrichment, not for rate series selection)
CLDR_URL = "https://raw.githubusercontent.com/unicode-org/cldr/main/common/supplemental/supplementalData.xml"

# Cache directory
CACHE_DIR = os.getenv("IMF_FX_CACHE_DIR", "./data/cache")

# Structure cache defaults (fx.py)
STRUCTURE_CACHE_FILE = os.getenv(
    "IMF_FX_STRUCTURE_CACHE_FILE",
    f"{CACHE_DIR}/imf_fx_structure_ER.json",
)
STRUCTURE_CACHE_TTL_SECONDS = int(
    os.getenv("IMF_FX_STRUCTURE_CACHE_TTL_SECONDS", str(24 * 60 * 60))
)

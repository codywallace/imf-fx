from __future__ import annotations
import os

BASE_URL = "https://api.imf.org/external/SDMX/3.0"

# Optional:
IMF_API_KEY = os.getenv("IMF_API_KEY")

# Default dataset: USD-only, monthly, ER/PA_RT, domestic per USD
DATAFLOW_AGENCY = "IMF.STA"
DATAFLOW_ID = "ER"
FREQUENCY = "M"
TRANSFORMATION = "PA_RT"
INDICATOR = "XDC_USD"  # domestic per USD

# CLDR for ISO2 -> currency
CLDR_URL = "https://raw.githubusercontent.com/unicode-org/cldr/main/common/supplemental/supplementalData.xml"

# Cache directory (used by lookups.py)
CACHE_DIR = os.getenv("IMF_FX_CACHE_DIR", "./data/cache")

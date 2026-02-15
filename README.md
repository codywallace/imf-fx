imf-fx

Lightweight Python utilities for fetching, parsing, and transforming IMF SDMX 3.0 exchange rate data.

imf-fx makes it easy to:

Fetch official IMF exchange rate data via SDMX 3.0

Convert SDMX JSON into tidy tabular form

Generate clean monthly USD exchange rate datasets

Export results to CSV or Parquet

Use exchange rates directly in analytics workflows

The package focuses on monthly USD-only exchange rates (domestic currency per USD), which are widely used in:

Macroeconomic research

Development finance normalization

Aid and humanitarian reporting

Cross-country financial comparison

Historical FX modeling

Installation
pip install imf-fx

Python 3.11 or newer is required.

Optional: If you have an IMF API key:

export IMF_API_KEY="your_api_key_here"
Quick Start
Download monthly USD exchange rates
from imf_fx.datasets import monthly_usd_only

df, meta = monthly_usd_only(
    start="2010-M01",
    end="2024-M12"
)

print(meta)
print(df.head())

This returns:

A Polars DataFrame

A metadata dictionary describing the fetch

Output Schema

The returned DataFrame includes:

Column	Description
date	Month-end date
country_iso3	IMF country code (ISO3-like)
country_iso2	ISO2 code (best effort)
country_name	Country name from IMF codelist
currency	Domestic ISO4217 currency (best effort via CLDR)
against	Always "USD"
rate_domestic_per_usd	Domestic currency per 1 USD
usd_per_domestic	Inverse exchange rate
log_rate	Natural log of rate
source	"IMF"
Real-World Usage Examples
1. Export to CSV (Excel / BI workflows)
df, meta = monthly_usd_only(
    start="2000-M01",
    end="2024-M12"
)

df.write_csv("imf_fx_monthly_usd.csv")

Use this file in:

Excel

Power BI

Tableau

2. Export to Parquet (analytics pipelines, etc.)
df, meta = monthly_usd_only(
    start="1990-M01",
    end="2024-M12"
)

df.write_parquet("imf_fx.parquet")

Parquet is ideal for:

Cloud analytics

DuckDB workflows

Spark pipelines

Large-scale modeling

3. Filter a specific country
df, _ = monthly_usd_only(
    start="2015-M01",
    end="2024-M12"
)

kenya = df.filter(df["country_iso3"] == "KEN")
print(kenya.tail())
4. Convert to Pandas

If your workflow relies on Pandas:

import pandas as pd

df, _ = monthly_usd_only(
    start="2015-M01",
    end="2024-M12"
)

pdf = df.to_pandas()
print(pdf.head())
5. Compute rolling averages
df, _ = monthly_usd_only(
    start="2010-M01",
    end="2024-M12"
)

kenya = (
    df
    .filter(df["country_iso3"] == "KEN")
    .sort("date")
    .with_columns(
        df["rate_domestic_per_usd"]
        .rolling_mean(window_size=12)
        .alias("rolling_12m_avg")
    )
)

print(kenya.tail())
6. Normalize historical financial data

Convert domestic currency to USD using the inverse rate:

df, _ = monthly_usd_only(
    start="2005-M01",
    end="2024-M12"
)

ethiopia = df.filter(df["country_iso3"] == "ETH")

# Example: Convert 1000 ETB in 2010 to USD
rate_row = ethiopia.filter(df["date"].dt.year() == 2010).head(1)
usd_value = 1000 * rate_row["usd_per_domestic"][0]

print("USD value:", usd_value)
Advanced Usage

You can access lower-level components if needed:

imf_fx.client — fetch individual country series

imf_fx.sdmx — convert SDMX JSON to tidy data

imf_fx.transform — apply exchange rate transformations

Example:

from imf_fx.client import fetch_country_usd_series

raw_df = fetch_country_usd_series(
    country_iso3="USA",
    start="2010-M01",
    end="2024-M12"
)

print(raw_df.head())
Performance Notes

Series are fetched sequentially (stable and polite to IMF servers)

Data is processed using Polars (fast and memory-efficient)

Suitable for pulling full historical data (1957 → present)

Caching

Currency mappings from CLDR are cached locally at:

./data/cache/

Override the cache location with:

export IMF_FX_CACHE_DIR="/custom/path"
Limitations

Focused on monthly USD-only exchange rates

Not intended as a full SDMX framework

Sequential fetching (no parallelism by default)
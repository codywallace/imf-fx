# imf-fx

High-performance Python client for IMF SDMX 3.0 exchange-rate data, with a monthly USD dataset helper.

`imf-fx` provides reliable access to the IMF’s official exchange rate data using the SDMX 3.0 API contract. It is designed for financial modeling workflows, development finance systems, and public information management platforms that require consistent historical currency normalization.

---

## Overview

The International Monetary Fund (IMF) publishes official exchange rate data through its SDMX 3.0 API.

`imf-fx` provides:

- Direct integration with the official SDMX 3.0 exchange-rate endpoint
- Batched country requests for efficient network usage
- Fast parsing using Polars
- Client-side time-window enforcement
- ISO2 and ISO3 country compatibility
- Log exchange rate output for modeling workflows

The package relies solely on the official IMF API contract, ensuring stability and alignment with IMF data structures. Some earlier community approaches relied on scraping spreadsheets or HTML representations of data. By using the official SDMX 3.0 endpoint directly, this package provides a more robust and production-ready integration path.

---

## Why This Is Useful

Exchange rate normalization is essential for:

- Development finance analytics
- Aid Information Management Systems (AIMS)
- Development Finance Information Management Systems (DFIMS)
- IATI data processing
- OECD CRS normalization
- Public financial management dashboards
- Cross-country budget comparisons
- Financial modeling pipelines

Public finance and development systems frequently ingest transactions denominated in multiple currencies. Consistent historical FX normalization ensures:

- Accurate aggregation across currencies
- Time-consistent financial comparisons
- Reproducible modeling
- Transparent auditability of conversions

`imf-fx` provides a fast and structured way to obtain authoritative exchange rate data for these workflows.

---

## Installation

```bash
pip install imf-fx

```

Python 3.11 or newer is required.

---

## Quick Start

Download the full monthly USD dataset

```Python
from imf_fx import monthly_usd_only

df = monthly_usd_only()
print(df.head())

```
---

## Bulk exports

The dataset can be written directly to CSV, or export to Parquet for use in other systems

```Python
from imf_fx import monthly_usd_only

df = monthly_usd_only()

df.write_csv("imf_monthly_usd.csv")
df.write_parquet("imf_monthly_usd.parquet")

```

---

Download a specific time-window

```Python
from imf_fx import monthly_usd_only

df = monthly_usd_only(
    start="2020-M01",
    end="2020-M12"
)

```

---

Retreive dataset with metadata

```Python
from imf_fx import monthly_usd_only

df, meta = monthly_usd_only(return_meta=True)

print(meta)

```

---

Example metadata output:

```Python
{
  "countries_requested": 260,
  "countries_with_data": 222,
  "rows_final": 154333,
  "min_period": "1924-M06",
  "max_period": "2026-M01",
  "elapsed_s": 2.7
}

```


## Output Schema

The normalized dataset includes

| Column                 | Description                   |
| ---------------------- | ----------------------------- |
| `country_iso3`         | ISO 3-letter country code     |
| `country_iso2`         | ISO 2-letter country code     |
| `country_name`         | Country name                  |
| `date`                 | End-of-month date             |
| `ym`                   | Year-month string (`YYYY-MM`) |
| `usd_per_domestic`     | Domestic currency per 1 USD   |
| `log_usd_per_domestic` | Natural log of exchange rate  |


Both ISO2 and ISO3 codes are included to simplify integration with:

- IATI datasets
- OECD CRS datasets
- National aid platforms
- Government finance systems
- Custom financial tools and pipelines

Users do not need to perform separate country code mapping.

---

## Performance

The dataset helper uses:

- Batched SDMX country keys (reducing HTTP calls)
- Parallel batch fetching
- Polars-based parsing for speed and memory efficiency
- Columnar construction rather than row-by-row dictionary assembly

Typical performance:

- Full historical dataset (1924–present)
- 150k rows
- ~2–3 seconds on a standard machine

Time-windowed requests are significantly faster.

---

## Minimal Dependencies

External dependencies are intentionally minimal:

- requests
- polars
- pycountry

This keeps the package lightweight and suitable for integration into larger systems.

---

## Public API

Stable public interface:

```Python
from imf_fx import monthly_usd_only

```

---

## License

MIT License
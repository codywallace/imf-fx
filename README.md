# imf-fx

![CI](https://github.com/codywallace/imf-fx/actions/workflows/ci.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/v/imf-fx)
![Python](https://img.shields.io/pypi/pyversions/imf-fx)
![License](https://img.shields.io/pypi/l/imf-fx)


---


High-performance Python client for IMF SDMX 3.0 exchange-rate data, with a production-ready monthly USD dataset helper.

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

Unlike spreadsheet scraping or HTML extraction approaches, `imf-fx` uses the official IMF SDMX 3.0 API contract directly. This ensures structural stability and alignment with IMF data standards.

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

Public finance systems frequently ingest transactions denominated in multiple currencies. Consistent historical FX normalization ensures:

- Accurate aggregation across currencies
- Time-consistent financial comparisons
- Reproducible modeling
- Transparent auditability of conversions

`imf-fx` provides a fast, structured, and authoritative way to obtain exchange rate data for these workflows.

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

---

## IMF Data License

The source of the data is extracted from the IMF's Exchange Data.

Below is an excerpt on the IMF Copyright and Usage page, effective 11 October, 2024; accessed 2026-02-23:

> You may download, extract, copy, create derivative works, publish, distribute, and use Data obtained from IMF Sites, subject to the following conditions:
>
> Whether obtained directly from the IMF or another party, when Data is distributed or reproduced in any manner, it must appear accurately with attribution to the IMF as the source, e.g. “Source: International Monetary Fund, Database Name, <<link to the dataset>>.”
>
> Users shall not infringe upon the integrity of the Data and in particular shall refrain from any act of alteration of the Data that intentionally affects its nature or accuracy. If the Data is materially transformed by the User, this must be stated explicitly along with the required source citation.
>
> Users who make IMF Data available to other Users through any type of distribution or download environment agree to take reasonable efforts to communicate and promote compliance by their users with these terms.
>
> If IMF Data is sold by Users as a standalone product, sellers must inform purchasers that the Data is available free of charge from the IMF.
>
> The Data is provided to Users “as is” and without warranty of any kind, either express or implied, including, without limitation, warranties of merchantability, fitness for a particular purpose, and noninfringement.
>
> The policy of free access and free reuse of IMF Data does not imply a right to obtain confidential or any unpublished data, over which the IMF reserves all rights.
>
> Except as stated in this Section on Data Usage, all other terms set forth in the general terms and conditions shall continue to apply to use of IMF Data.
>
> For any potential commercial reuse of IMF Data, please email copyright@imf.org to request permission.

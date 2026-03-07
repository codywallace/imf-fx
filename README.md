# imf-fx

[![CI](https://github.com/codywallace/imf-fx/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/codywallace/imf-fx/actions/workflows/ci.yml)
![PyPI](https://img.shields.io/pypi/v/imf-fx)
![Python versions](https://img.shields.io/pypi/pyversions/imf-fx?logo=python&logoColor=white)
![License](https://img.shields.io/pypi/l/imf-fx?cacheSeconds=300)


---

High-performance Python client for IMF SDMX 3.0 exchange-rate data, with consistent monthly, quarterly, and annual support for USD, EUR, and SDR (XDR).

---

## Overview

The International Monetary Fund (IMF) publishes official exchange rate data through its SDMX 3.0 API.

`imf-fx` provides:

- Direct integration with the official IMF SDMX 3.0 Exchange Rate (ER) dataflow
- Batched and parallel country requests for efficient network usage
- Fast parsing using Polars
- Client-side time-window enforcement
- Consistent normalized outputs across monthly, quarterly, and annual data
- Support for USD, EUR, and SDR (XDR) exchange series
- Optional metadata reporting
- Official API access only, with no spreadsheet or HTML scraping

Unlike scraping-based approaches, `imf-fx` uses the IMF’s structured SDMX 3.0 API contract directly. This provides a stable and reproducible way to work with exchange rate data in analytics, modeling, and public finance workflows.

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

Public finance and development data systems frequently ingest transactions denominated in multiple currencies. Consistent historical FX normalization helps ensure:

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

### Monthly USD exchange rates (Domestic Currency per USD)

```Python
from imf_fx import monthly_usd_avg

df = monthly_usd_avg(normalize=True)
print(df.head())

```

This is the most common entry point for USD exchange rates.

---

### Monthly EUR exchange rates

```Python
from imf_fx import monthly_eur_avg

df = monthly_eur_avg(normalize=True)

```

---

### Monthly SDR (XDR) exchange rates

```Python
from imf_fx import monthly_xdr_avg

df = monthly_xdr_avg(normalize=True)

```

---

### Quarterly or Annual averages

```Python
from imf_fx import quarterly_usd_avg, annual_eur_avg

df_q = quarterly_usd_avg(normalize=True)
df_a = annual_eur_avg(normalize=True)

```

---

Date semantics:

| Frequency | Date returned |
| --------- | ------------- |
| Monthly   | Month end     |
| Quarterly | Quarter end   |
| Annual    | December 31   |

---

## Core API

The core primitive of the library is:

```Python
from imf_fx import exchange_rates

```

All wrapper functions call this underlying API

Example:

```Python
from imf_fx import exchange_rates

df, meta = exchange_rates(
    ref_areas=["USA", "JPN", "BRA"],
    base="XDC",
    quote="USD",
    frequency="monthly",
    transformation="average",
    start="2020-M01",
    end="2020-M12",
    normalize=True,
    return_meta=True,
)

print(df.head())
print(meta)
```

---

## Indicator Semantics

The IMF ER dataset uses indicators structured as:

BASE_QUOTE

Examples:

| Indicator | Meaning                   |
| --------- | ------------------------- |
| `XDC_USD` | Domestic currency per USD |
| `USD_XDC` | USD per domestic currency |
| `XDC_EUR` | Domestic currency per EUR |
| `EUR_XDC` | EUR per domestic currency |
| `XDC_XDR` | Domestic currency per SDR |
| `XDR_XDC` | SDR per domestic currency |

Because the IMF provides both directions for key pairs, no recipocral calculations are required.

---

## Supported Frequencies

| Frequency | Code        |
| --------- | ----------- |
| Monthly   | `monthly`   |
| Quarterly | `quarterly` |
| Annual    | `annual`    |

---

## Supported Transformations

| Transformation | IMF Code |
| -------------- | -------- |
| Average        | `PA_RT`  |
| End-of-period  | `EOP_RT` |

Example:

```Python
exchange_rates(
    ref_areas=["GBR"],
    base="XDC",
    quote="EUR",
    frequency="quarterly",
    transformation="eop",
)
```

---

## Time Windows

Time windows follow IMF period formatting.

| Frequency | Format     |
| --------- | ---------- |
| Monthly   | `YYYY-MMM` |
| Quarterly | `YYYY-Q#`  |
| Annual    | `YYYY`     |

Example:

```Python
from imf_fx import monthly_usd_avg

df = monthly_usd_avg(
    start="2020-M01",
    end="2020-M06",
    normalize=True,
)
```

---

## Normalized Output Schema

When normalize=True, output columns are standardized.

| Column         | Description             |
| -------------- | ----------------------- |
| `date`         | Period-end date         |
| `period`       | Original IMF period     |
| `country_iso3` | ISO-3 country code      |
| `country_iso2` | ISO-2 country code      |
| `country_name` | Country name (optional) |
| `base`         | Base currency           |
| `quote`        | Quote currency          |
| `rate`         | Exchange rate           |
| `source`       | Data source (`IMF`)     |

This schema is identical across:

- monthly
- quarterly
- annual
- USD
- EUR
- SDR

---

### Metadata

Set return_meta=True to retrieve diagnostics.

```Python
from imf_fx import monthly_usd_avg

df, meta = monthly_usd_avg(
    start="2020-M01",
    end="2020-M03",
    normalize=True,
    return_meta=True,
)

print(meta)
```

Example:

```Python
from imf_fx import monthly_usd_avg

df, meta = monthly_usd_avg(
    start="2020-M01",
    end="2020-M03",
    normalize=True,
    return_meta=True,
)

print(meta)
```

Example:

```Python
{
  "indicator": "XDC_USD",
  "frequency": "M",
  "requested_ref_areas": ["BRA","GBR","JPN"],
  "returned_ref_areas": ["BRA","GBR","JPN"],
  "missing_ref_areas": [],
  "rows_raw_total": 9,
  "rows_out": 9
}
```

---

Bulk Export

```
from imf_fx import monthly_usd_avg

df = monthly_usd_avg(normalize=True)

df.write_csv("imf_monthly_usd.csv")
df.write_parquet("imf_monthly_usd.parquet")
```

Parquet would be recommended for analytics pipelines.

---

## Performance

imf-fx uses:

- Batched SDMX country keys
- Parallel requests
- Polars-based parsing
- Columnar dataframe construction
- Structure caching

Typical performance:

| Dataset                                      | Runtime      |
| -------------------------------------------- | ------------ |
| Full historical monthly dataset (~150k rows) | ~2–3 seconds |
| Time-windowed requests                       | faster       |

---

## Stable Public Interface

```Python
from imf_fx import (
    exchange_rates,
    fetch_countries_usd_series,
    monthly_usd_avg,
    quarterly_usd_avg,
    annual_usd_avg,
    monthly_eur_avg,
    quarterly_eur_avg,
    annual_eur_avg,
    monthly_xdr_avg,
    quarterly_xdr_avg,
    annual_xdr_avg,
)
```

---

## Backward Compatibility

monthly_usd_only() is retained for compatibility with earlier versions of the package.

New code should prefer the consistent naming convention:

- monthly_usd_avg
- quarterly_usd_avg
- annual_usd_avg

---

## Dependencies

Minimal dependencies on external libraries.

- requests
- polars
- pycountry

This keeps the package lightweight and suitable for integration into larger systems.

---

## License

MIT License

---

## IMF Data License

The source of the data is extracted from the IMF's Exchange Rate database.

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

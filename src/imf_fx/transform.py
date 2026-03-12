from __future__ import annotations

import polars as pl
import pycountry

from .lookups import iso2_to_currency


def iso3_to_iso2(code: str | None) -> str | None:
    if not isinstance(code, str):
        return None
    code = code.strip().upper()
    c = pycountry.countries.get(alpha_3=code)
    return c.alpha_2 if c else None


def add_imf_date_col(df: pl.DataFrame, time_col: str = "TIME_PERIOD") -> pl.DataFrame:
    """
    Parse monthly IMF period strings like 'YYYY-M01' into a DATE column at month-end.
    (Kept for backwards compatibility with your earlier USD-only pipeline.)
    """
    s = pl.col(time_col).cast(pl.Utf8, strict=False)
    year = s.str.extract(r"^(\d{4})-M(\d{2})$", 1).cast(pl.Int32, strict=False)
    month = s.str.extract(r"^(\d{4})-M(\d{2})$", 2).cast(pl.Int32, strict=False)

    date_expr = (
        pl.when(s.str.contains(r"^\d{4}-M\d{2}$"))
        .then(pl.date(year, month, pl.lit(1)).dt.month_end())
        .otherwise(None)
        .alias("DATE")
    )
    return df.with_columns(date_expr)


def add_imf_date_col_any(df: pl.DataFrame, time_col: str = "TIME_PERIOD") -> pl.DataFrame:
    """
    Convert IMF TIME_PERIOD strings to a consistent Polars Date column (period end).

    Supported:
      - Monthly:   YYYY-MMM  (e.g. 2020-M01)  -> last day of month
      - Quarterly: YYYY-Q#   (e.g. 2020-Q1)   -> last day of quarter
      - Annual:    YYYY      (e.g. 2020)      -> 2020-12-31
    """
    s = pl.col(time_col).cast(pl.Utf8, strict=False)

    # ---- Monthly: YYYY-M01 ----
    year_m = s.str.extract(r"^(\d{4})-M(\d{2})$", 1).cast(pl.Int32, strict=False)
    mon_m = s.str.extract(r"^(\d{4})-M(\d{2})$", 2).cast(pl.Int32, strict=False)
    monthly_date = pl.date(year_m, mon_m, pl.lit(1)).dt.month_end()

    # ---- Quarterly: YYYY-Q1 ----
    year_q = s.str.extract(r"^(\d{4})-Q([1-4])$", 1).cast(pl.Int32, strict=False)
    qtr = s.str.extract(r"^(\d{4})-Q([1-4])$", 2).cast(pl.Int32, strict=False)
    q_end_month = (qtr * 3).cast(pl.Int32, strict=False)
    quarterly_date = pl.date(year_q, q_end_month, pl.lit(1)).dt.month_end()

    # ---- Annual: YYYY ----
    year_a = s.str.extract(r"^(\d{4})$", 1).cast(pl.Int32, strict=False)
    annual_date = pl.date(year_a, pl.lit(12), pl.lit(31))

    date_expr = (
        pl.when(s.str.contains(r"^\d{4}-M\d{2}$"))
        .then(monthly_date)
        .when(s.str.contains(r"^\d{4}-Q[1-4]$"))
        .then(quarterly_date)
        .when(s.str.contains(r"^\d{4}$"))
        .then(annual_date)
        .otherwise(None)
        .alias("DATE")
    )

    return df.with_columns(date_expr)


def add_against_currency(df: pl.DataFrame) -> pl.DataFrame:
    """
    Legacy helper: extracts Against currency from INDICATOR strings like 'XDC_USD' -> 'USD'
    """
    return df.with_columns(
        pl.col("INDICATOR")
        .cast(pl.Utf8, strict=False)
        .str.extract(r"XDC_([A-Z]{3})$", 1)
        .alias("Against")
    )


def enrich_rates_usd_only(df: pl.DataFrame) -> pl.DataFrame:
    """
    Legacy USD-only enrichments (kept for compatibility).
    """
    df = df.with_columns(
        [
            pl.col("OBS_VALUE").cast(pl.Float64, strict=False).alias("rate_domestic_per_usd"),
            pl.when(pl.col("OBS_VALUE").is_not_null() & (pl.col("OBS_VALUE") > 0))
            .then(pl.col("OBS_VALUE").log())
            .otherwise(None)
            .alias("log_rate"),
        ]
    )

    df = df.with_columns(
        [
            pl.when(
                pl.col("rate_domestic_per_usd").is_not_null()
                & (pl.col("rate_domestic_per_usd") > 0)
            )
            .then(1.0 / pl.col("rate_domestic_per_usd"))
            .otherwise(None)
            .alias("usd_per_domestic"),
        ]
    )
    return df


def finalize_usd_only(
    df: pl.DataFrame,
    area_lu: pl.DataFrame,
    *,
    include_country_name: bool = True,
    categorical_dims: bool = True,
    country_name_categorical: bool = False,
) -> pl.DataFrame:
    """
    Legacy USD-only finalizer (kept for compatibility).
    Prefer normalize_fx_rates() for a generic schema.
    """
    if df.height == 0:
        return df

    label_col = "label_en" if "label_en" in area_lu.columns else "label"

    df = (
        df.pipe(add_imf_date_col)
        .pipe(add_against_currency)
        .pipe(enrich_rates_usd_only)
        .with_columns(pl.col("COUNTRY").cast(pl.Utf8, strict=False).alias("country_iso3"))
        .filter(pl.col("country_iso3").str.len_chars() == 3)
        .with_columns(
            pl.col("country_iso3")
            .map_elements(iso3_to_iso2, return_dtype=pl.Utf8)
            .alias("country_iso2")
        )
        .with_columns(
            pl.col("country_iso2")
            .map_elements(iso2_to_currency, return_dtype=pl.Utf8)
            .alias("currency")
        )
        .with_columns(
            [
                pl.lit("IMF").alias("source"),
                pl.lit("USD").alias("against"),
            ]
        )
        .with_columns(
            [
                pl.col("country_iso3").str.slice(0, 3),
                pl.col("country_iso2").str.slice(0, 2),
                pl.col("currency").str.slice(0, 3),
                pl.col("against").str.slice(0, 3),
            ]
        )
    )

    if include_country_name:
        area_small = area_lu.select(["code", label_col]).rename({label_col: "country_name"})
        df = df.join(area_small, left_on="COUNTRY", right_on="code", how="left")

    cols = [
        pl.col("DATE").alias("date"),
        "country_iso3",
        "country_iso2",
    ]
    if include_country_name:
        cols.append("country_name")
    cols += [
        "currency",
        "against",
        "rate_domestic_per_usd",
        "usd_per_domestic",
        "log_rate",
        "source",
    ]

    df = df.select(cols).sort(["country_iso3", "date"])

    if categorical_dims:
        cat_cols = ["country_iso3", "country_iso2", "currency", "against", "source"]
        df = df.with_columns([pl.col(c).cast(pl.Categorical) for c in cat_cols if c in df.columns])

        if include_country_name and country_name_categorical and "country_name" in df.columns:
            df = df.with_columns(pl.col("country_name").cast(pl.Categorical))

    return df


def _pick_area_col(df: pl.DataFrame) -> str:
    for c in ["COUNTRY", "REF_AREA"]:
        if c in df.columns:
            return c
    raise ValueError("Could not find a country/ref area column (expected COUNTRY or REF_AREA).")


def normalize_fx_rates(
    df: pl.DataFrame,
    *,
    indicator: str,
    frequency: str,
    area_lu: pl.DataFrame | None = None,
    include_country_name: bool = False,
    include_period: bool = True,
    categorical_dims: bool = True,
) -> pl.DataFrame:
    """
    Normalize raw ER tidy output into a consistent schema for ANY base_quote indicator.

    Output columns:
      date
      period (optional; original TIME_PERIOD)
      frequency
      country_iso3
      country_iso2
      country_name (optional)
      base
      quote
      rate
      source

    rate follows the indicator name:
      XDC_USD => domestic per USD
      USD_XDC => USD per domestic
    """
    if df.height == 0:
        return df

    area_col = _pick_area_col(df)

    if "_" not in indicator:
        raise ValueError(f"indicator must look like BASE_QUOTE, got: {indicator}")

    base, quote = indicator.split("_", 1)

    out = (
        df.pipe(add_imf_date_col_any)
        .with_columns(pl.col(area_col).cast(pl.Utf8, strict=False).alias("country_iso3"))
        .filter(pl.col("country_iso3").str.len_chars() == 3)
        .with_columns(
            pl.col("country_iso3")
            .map_elements(iso3_to_iso2, return_dtype=pl.Utf8)
            .alias("country_iso2")
        )
        .with_columns(
            [
                pl.lit(base).alias("base"),
                pl.lit(quote).alias("quote"),
                pl.lit(frequency).alias("frequency"),
                pl.col("OBS_VALUE").cast(pl.Float64, strict=False).alias("rate"),
                pl.lit("IMF").alias("source"),
            ]
        )
    )

    if include_period and "TIME_PERIOD" in out.columns:
        out = out.with_columns(pl.col("TIME_PERIOD").cast(pl.Utf8, strict=False).alias("period"))

    if include_country_name and area_lu is not None and area_lu.height > 0:
        lab = "label_en" if "label_en" in area_lu.columns else "label"
        out = out.join(
            area_lu.select(["code", lab]).rename({lab: "country_name"}),
            left_on=area_col,
            right_on="code",
            how="left",
        )

    cols = [pl.col("DATE").alias("date")]
    if include_period:
        cols.append("period")
    cols.append("frequency")
    cols += ["country_iso3", "country_iso2"]
    if include_country_name and "country_name" in out.columns:
        cols.append("country_name")
    cols += ["base", "quote", "rate", "source"]

    out = out.select(cols).sort(["country_iso3", "date"])

    if categorical_dims:
        cat_cols = ["frequency", "country_iso3", "country_iso2", "base", "quote", "source"]
        if include_period:
            cat_cols.append("period")
        out = out.with_columns(
            [pl.col(c).cast(pl.Categorical) for c in cat_cols if c in out.columns]
        )

        if include_country_name and "country_name" in out.columns:
            # optional; Parquet compression is usually fine either way
            pass

    return out

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


def add_against_currency(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("INDICATOR")
        .cast(pl.Utf8, strict=False)
        .str.extract(r"XDC_([A-Z]{3})$", 1)
        .alias("Against")
    )


def enrich_rates_usd_only(df: pl.DataFrame) -> pl.DataFrame:
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
            pl.when(pl.col("rate_domestic_per_usd").is_not_null() & (pl.col("rate_domestic_per_usd") > 0))
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
    Normalize IMF ER USD-only output to a tidy table matching the library's schema.

    include_country_name:
        If False, skip the IMF codelist join entirely (faster + less memory).
    categorical_dims:
        Cast repeated string dims to Polars Categorical for memory/speed.
    country_name_categorical:
        If include_country_name=True, optionally cast country_name to Categorical.
        (Often leaving it as Utf8 is fine; Parquet compresses it well.)
    """
    if df.height == 0:
        return df

    label_col = "label_en" if "label_en" in area_lu.columns else "label"

    df = (
        df.pipe(add_imf_date_col)
        .pipe(add_against_currency)
        .pipe(enrich_rates_usd_only)
        .with_columns(
            pl.col("COUNTRY").cast(pl.Utf8, strict=False).alias("country_iso3")
        )
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
        # safety clamps
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
        # (Optional) ensure join keys are consistent types
        area_small = (
            area_lu.select(["code", label_col])
            .rename({label_col: "country_name"})
        )
        df = df.join(
            area_small,
            left_on="COUNTRY",
            right_on="code",
            how="left",
        )

    # final shape
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
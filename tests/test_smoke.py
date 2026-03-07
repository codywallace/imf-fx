# tests/test_smoke.py

import polars as pl

from imf_fx import fetch_countries_usd_series, monthly_usd_only


def test_monthly_usd_only_small_window():
    df, meta = monthly_usd_only(
        start="2020-M01",
        end="2020-M01",
        return_meta=True,
        batch_parallel=False,
    )

    assert isinstance(df, pl.DataFrame)
    assert isinstance(meta, dict)

    # tolerate either old or new meta schema
    assert any(k in meta for k in ["rows_final", "rows_raw_total", "rows_out"])


def test_fetch_countries_usd_series_basic():
    df = fetch_countries_usd_series(
        ["USA", "JPN"],
        start="2020-M01",
        end="2020-M01",
    )

    assert isinstance(df, pl.DataFrame)

    if df.height > 0:
        assert "COUNTRY" in df.columns
        assert df.select(pl.col("COUNTRY").n_unique()).item() >= 1

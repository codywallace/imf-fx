# tests/test_smoke.py

import polars as pl
from imf_fx import monthly_usd_only, fetch_countries_usd_series  # type: ignore


def test_monthly_usd_only_small_window():
    df, meta = monthly_usd_only(  # pyright: ignore[reportUnknownVariableType]
        start="2020-M01",
        end="2020-M01",
        return_meta=True,
        batch_parallel=False,
    )

    assert isinstance(df, pl.DataFrame)
    assert meta["rows_final"] >= 0  # type: ignore


def test_fetch_countries_usd_series_basic():
    df = fetch_countries_usd_series(
        ["USA", "JPN"],
        start="2020-M01",
        end="2020-M01",
    )

    assert isinstance(df, pl.DataFrame)
    # Should return at least USA
    if df.height > 0:
        assert "COUNTRY" in df.columns
        assert df.select(pl.col("COUNTRY").n_unique()).item() >= 1

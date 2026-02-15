import polars as pl
from imf_fx.transform import finalize_usd_only

def test_finalize_usd_only_shapes_columns():
    raw = pl.DataFrame([
        {"COUNTRY": "USA", "INDICATOR": "XDC_USD", "TIME_PERIOD": "2024-M01", "OBS_VALUE": 1.0},
        {"COUNTRY": "USA", "INDICATOR": "XDC_USD", "TIME_PERIOD": "2024-M02", "OBS_VALUE": 2.0},
    ])

    area_lu = pl.DataFrame([
        {"code": "USA", "label_en": "United States"},
    ])

    out = finalize_usd_only(raw, area_lu=area_lu)
    assert "date" in out.columns
    assert "country_iso3" in out.columns
    assert "against" in out.columns
    assert "rate_domestic_per_usd" in out.columns
    assert out.height == 2
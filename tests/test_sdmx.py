import polars as pl
from imf_fx.sdmx import sdmx3_to_tidy

def test_sdmx3_to_tidy_empty_series_returns_empty_df():
    j = {
        "data": {
            "structures": [{
                "dimensions": {"series": [], "observation": [{"id": "TIME_PERIOD", "values": []}]}
            }],
            "dataSets": [{"series": {}}]
        }
    }
    df = sdmx3_to_tidy(j)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
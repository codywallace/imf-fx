from __future__ import annotations
from typing import Any, Dict, List
import polars as pl

from .exceptions import SdmxParseError

def _first(x: Any) -> Any:
    return x[0] if isinstance(x, list) else x

def _code(v: Any) -> Any:
    if isinstance(v, dict):
        return v.get("id", v.get("value", v.get("code")))
    return v

def sdmx3_to_tidy(j: Dict[str, Any], debug: bool = False) -> pl.DataFrame:
    """
    Convert IMF SDMX 3.0 JSON response into a "tidy" Polars DataFrame with columns:
      - series dimensions (e.g. COUNTRY, INDICATOR, ...)
      - TIME_PERIOD
      - OBS_VALUE
    """
    try:
        data = j["data"]
        structures0 = _first(data["structures"])
        ds0 = _first(data["dataSets"])
    except Exception as e:
        raise SdmxParseError("Unexpected SDMX JSON shape (missing data/structures/dataSets)") from e

    dims = structures0.get("dimensions", {})
    series_dim_list = dims.get("series", []) or []
    obs_dim_list = dims.get("observation", []) or []

    # TIME_PERIOD dimension
    time_dim = None
    for d in obs_dim_list:
        if isinstance(d, dict) and d.get("id") == "TIME_PERIOD":
            time_dim = d
            break
    if time_dim is None:
        time_dim = obs_dim_list[0] if obs_dim_list else {"values": []}

    time_values = [_code(v) for v in (time_dim.get("values") or [])]

    series_ids = [d.get("id") for d in series_dim_list]
    series_vals_per_dim = [d.get("values") or [] for d in series_dim_list]

    series_block = ds0.get("series", {})
    if not isinstance(series_block, dict) or len(series_block) == 0:
        if debug:
            print("No series in ds0. ds0 keys:", list(ds0.keys()))
        return pl.DataFrame()

    out_rows: List[Dict[str, Any]] = []

    for skey, sobj in series_block.items():
        idx = [int(x) for x in skey.split(":")]
        if len(idx) != len(series_ids):
            continue

        decoded: Dict[str, Any] = {}
        bad = False
        for pos, dim_id in enumerate(series_ids):
            codes_list = series_vals_per_dim[pos]
            i = idx[pos]
            if i < 0 or i >= len(codes_list):
                bad = True
                break
            decoded[dim_id] = _code(codes_list[i])
        if bad:
            continue

        observations = sobj.get("observations", {})
        if not isinstance(observations, dict) or len(observations) == 0:
            continue

        for tpos_str, obs_payload in observations.items():
            tpos = int(tpos_str)
            tp = time_values[tpos] if 0 <= tpos < len(time_values) else None

            val = None
            if isinstance(obs_payload, list) and len(obs_payload) > 0:
                val = obs_payload[0]

            out_rows.append({**decoded, "TIME_PERIOD": tp, "OBS_VALUE": val})

    df = pl.from_dicts(out_rows) if out_rows else pl.DataFrame()
    if df.height and "OBS_VALUE" in df.columns:
        df = df.with_columns(pl.col("OBS_VALUE").cast(pl.Float64, strict=False))
    return df
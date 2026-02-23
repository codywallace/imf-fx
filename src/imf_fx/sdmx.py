# Internal module. Not part of the public API.
# Subject to change without notice.

from __future__ import annotations

from typing import Any

import polars as pl

from .exceptions import SdmxParseError


def _first(x: Any) -> Any:
    return x[0] if isinstance(x, list) else x


def _code(v: Any) -> Any:
    if isinstance(v, dict):
        return v.get("id", v.get("value", v.get("code")))
    return v


def sdmx3_to_tidy(
    j: dict[str, Any],
    *,
    min_period: str | None = None,
    max_period: str | None = None,
    debug: bool = False,
) -> pl.DataFrame:
    """
    Convert IMF SDMX 3.0 JSON response into a tidy Polars DataFrame with columns:
      - series dimensions (e.g. COUNTRY, INDICATOR, ...)
      - TIME_PERIOD
      - OBS_VALUE

    Optional:
      min_period / max_period filter observations by TIME_PERIOD lexicographically.
      Works for IMF monthly formats like 'YYYY-M01'.
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
    if not isinstance(series_block, dict) or not series_block:
        if debug:
            print("No series in ds0. ds0 keys:", list(ds0.keys()))
        return pl.DataFrame()

    # ---- output columns (lists) ----
    # One list per series dimension, plus TIME_PERIOD and OBS_VALUE.
    cols: dict[str, list[Any]] = {sid: [] for sid in series_ids}
    time_col: list[Any] = []
    obs_col: list[Any] = []

    # localize for speed
    tv = time_values
    sv = series_vals_per_dim
    sids = series_ids
    mn = min_period
    mx = max_period

    cols_append = {k: v.append for k, v in cols.items()}
    time_append = time_col.append
    obs_append = obs_col.append

    for skey, sobj in series_block.items():
        try:
            idx = [int(x) for x in skey.split(":")]
        except Exception:
            continue

        if len(idx) != len(sids):
            continue

        # decode this series' dimension values once
        decoded_vals: list[Any] = []
        bad = False
        for pos, dim_id in enumerate(sids):
            codes_list = sv[pos]
            i = idx[pos]
            if i < 0 or i >= len(codes_list):
                bad = True
                break
            decoded_vals.append(_code(codes_list[i]))
        if bad:
            continue

        observations = sobj.get("observations", {})
        if not isinstance(observations, dict) or not observations:
            continue

        # Now iterate observations; only append when inside window
        for tpos_str, obs_payload in observations.items():
            try:
                tpos = int(tpos_str)
            except Exception:
                continue

            tp = tv[tpos] if 0 <= tpos < len(tv) else None
            if tp is None:
                continue

            if mn and tp < mn:
                continue
            if mx and tp > mx:
                continue

            val = None
            if isinstance(obs_payload, list) and obs_payload:
                val = obs_payload[0]

            # append decoded series dims
            for dim_id, dim_val in zip(sids, decoded_vals):
                cols_append[dim_id](dim_val)

            time_append(tp)
            obs_append(val)

    if not time_col:
        return pl.DataFrame()

    data_out: dict[str, list[Any]] = {**cols, "TIME_PERIOD": time_col, "OBS_VALUE": obs_col}
    df = pl.DataFrame(data_out)

    # OBS_VALUE cast
    if "OBS_VALUE" in df.columns and df.height:
        df = df.with_columns(pl.col("OBS_VALUE").cast(pl.Float64, strict=False))

    return df

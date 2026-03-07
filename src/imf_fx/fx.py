from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import polars as pl

from .client import fetch_countries_series
from .structure import codelist_to_df, get_dataflow_structure
from .transform import normalize_fx_rates


def _is_iso3_like(code: object) -> bool:
    return isinstance(code, str) and len(code) == 3 and code.isalpha()


def _chunked(xs: list[str], n: int) -> list[list[str]]:
    return [xs[i : i + n] for i in range(0, len(xs), n)]


def _default_structure_cache_path() -> Path:
    return Path("./data/cache/imf_fx_structure_ER.json")


def _load_cached_structure(
    cache_path: Path, ttl_seconds: int
) -> tuple[dict[str, Any] | None, bool]:
    import json
    import time

    if not cache_path.exists():
        return None, False
    try:
        age = time.time() - cache_path.stat().st_mtime
        if age > ttl_seconds:
            return None, False
        obj = json.loads(cache_path.read_text(encoding="utf-8"))
        return (obj, True) if isinstance(obj, dict) else (None, False)
    except Exception:
        return None, False


def _write_cached_structure(cache_path: Path, struct: dict[str, Any]) -> None:
    import json

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(struct), encoding="utf-8")


def exchange_rates(
    ref_areas: str | Sequence[str] | None = None,
    *,
    base: str,
    quote: str,
    transformation: str = "average",  # "average" | "eop"
    frequency: str = "monthly",  # "annual" | "monthly" | "quarterly"
    start: str | None = None,
    end: str | None = None,
    timeout: int = 120,
    batch_size: int = 25,
    batch_parallel: bool = True,
    batch_max_workers: int = 3,
    batch_max_splits: int = 6,
    cache_structure: bool = True,
    structure_cache_path: str | None = None,
    structure_cache_ttl_seconds: int = 24 * 60 * 60,
    include_country_labels: bool = False,
    normalize: bool = False,
    return_meta: bool = False,
    debug: bool = False,
) -> pl.DataFrame | tuple[pl.DataFrame, dict]:
    """
    Fetch ER series and return a tidy Polars DataFrame.

    indicator is formed as '{base}_{quote}' (e.g. XDC_USD, USD_XDC, XDC_EUR,
    EUR_XDC, XDC_XDR, XDR_XDC).

    If normalize=True, returns a standardized schema via transform.normalize_fx_rates().

    If ref_areas is None, all valid IMF ER country codes from CL_ER_COUNTRY_PUB
    are requested.
    """
    tr_map = {"average": "PA_RT", "eop": "EOP_RT"}
    freq_map = {"annual": "A", "monthly": "M", "quarterly": "Q"}

    if transformation not in tr_map:
        raise ValueError(f"transformation must be one of {sorted(tr_map)}")
    if frequency not in freq_map:
        raise ValueError(f"frequency must be one of {sorted(freq_map)}")

    tr_code = tr_map[transformation]
    freq_code = freq_map[frequency]

    base = str(base).strip().upper()
    quote = str(quote).strip().upper()
    indicator = f"{base}_{quote}"

    # structure (cached)
    struct: dict[str, Any] | None = None
    cache_hit = False
    if cache_structure:
        p = Path(structure_cache_path) if structure_cache_path else _default_structure_cache_path()
        struct, cache_hit = _load_cached_structure(p, structure_cache_ttl_seconds)

    if struct is None:
        struct = get_dataflow_structure()
        if cache_structure:
            p = (
                Path(structure_cache_path)
                if structure_cache_path
                else _default_structure_cache_path()
            )
            _write_cached_structure(p, struct)

    ind_lu = codelist_to_df(struct, "CL_ER_INDICATOR_PUB")
    valid_indicators = set(ind_lu["code"].to_list())
    if indicator not in valid_indicators:
        common = ["XDC_USD", "USD_XDC", "XDC_EUR", "EUR_XDC", "XDC_XDR", "XDR_XDC"]
        examples = [c for c in common if c in valid_indicators]
        raise ValueError(
            f"Invalid ER indicator '{indicator}'. "
            f"Examples: {', '.join(examples) if examples else 'see CL_ER_INDICATOR_PUB'}"
        )

    area_lu = codelist_to_df(struct, "CL_ER_COUNTRY_PUB")

    # Optional area labels for joins later
    label_lu = area_lu if include_country_labels else None

    # normalize ref_areas
    if ref_areas is None:
        refs = [
            c.strip().upper()
            for c in area_lu["code"].to_list()
            if isinstance(c, str) and _is_iso3_like(c.strip().upper())
        ]
    elif isinstance(ref_areas, str):
        refs = [ref_areas]
    else:
        refs = list(ref_areas)

    refs = [r.strip().upper() for r in refs if isinstance(r, str)]
    refs = [r for r in refs if _is_iso3_like(r)]
    refs = sorted(set(refs))

    if not refs:
        empty = pl.DataFrame()
        meta = {
            "indicator": indicator,
            "transformation": tr_code,
            "frequency": freq_code,
            "start": start,
            "end": end,
            "requested_ref_areas": [],
            "returned_ref_areas": [],
            "missing_ref_areas": [],
            "structure_cache_hit": cache_hit,
            "rows_raw_total": 0,
            "rows_out": 0,
        }
        return (empty, meta) if return_meta else empty

    # batching + resilience
    batches = _chunked(refs, batch_size)
    dfs: list[pl.DataFrame] = []
    errors = 0

    def _fetch_batch(batch: list[str], splits_left: int) -> tuple[pl.DataFrame, int]:
        try:
            df = fetch_countries_series(
                batch,
                indicator=indicator,
                transformation=tr_code,
                frequency=freq_code,
                start=start,
                end=end,
                timeout=timeout,
            )
            return df, 0
        except Exception as e:
            if debug:
                print(f"[imf-fx] batch ERROR ({len(batch)}): {type(e).__name__}: {e}")
            if splits_left <= 0 or len(batch) <= 1:
                return pl.DataFrame(), 1
            mid = len(batch) // 2
            df_l, fail_l = _fetch_batch(batch[:mid], splits_left - 1)
            df_r, fail_r = _fetch_batch(batch[mid:], splits_left - 1)
            parts = [d for d in (df_l, df_r) if getattr(d, "height", 0) > 0]
            if parts:
                return pl.concat(parts, how="vertical"), (fail_l + fail_r)
            return pl.DataFrame(), (fail_l + fail_r)

    if batch_parallel:
        with ThreadPoolExecutor(max_workers=batch_max_workers) as ex:
            futs = {ex.submit(_fetch_batch, b, batch_max_splits): b for b in batches}
            for fut in as_completed(futs):
                df, fail = fut.result()
                errors += fail
                if df.height:
                    dfs.append(df)
    else:
        for b in batches:
            df, fail = _fetch_batch(b, batch_max_splits)
            errors += fail
            if df.height:
                dfs.append(df)

    if not dfs:
        empty = pl.DataFrame()
        meta = {
            "indicator": indicator,
            "transformation": tr_code,
            "frequency": freq_code,
            "start": start,
            "end": end,
            "requested_ref_areas": refs,
            "returned_ref_areas": [],
            "missing_ref_areas": refs,
            "errors": errors,
            "structure_cache_hit": cache_hit,
            "rows_raw_total": 0,
            "rows_out": 0,
        }
        return (empty, meta) if return_meta else empty

    df_raw = pl.concat(dfs, how="vertical")

    # normalize output optionally
    df_out = df_raw
    if normalize:
        df_out = normalize_fx_rates(
            df_raw,
            indicator=indicator,
            area_lu=label_lu,
            include_country_name=include_country_labels,
        )

    # meta
    returned: list[str] = []
    for col in ["COUNTRY", "REF_AREA"]:
        if col in df_raw.columns:
            returned = sorted(df_raw.select(pl.col(col).unique()).to_series().to_list())
            break

    meta = {
        "indicator": indicator,
        "transformation": tr_code,
        "frequency": freq_code,
        "start": start,
        "end": end,
        "requested_ref_areas": refs,
        "returned_ref_areas": returned,
        "missing_ref_areas": sorted(set(refs) - set(returned)),
        "errors": errors,
        "structure_cache_hit": cache_hit,
        "rows_raw_total": int(df_raw.height),
        "rows_out": int(df_out.height),
    }

    return (df_out, meta) if return_meta else df_out


# Consistent wrappers (A/Q/M averages)


def monthly_usd_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="USD",
        frequency="monthly",
        transformation="average",
        **kwargs,
    )


def quarterly_usd_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="USD",
        frequency="quarterly",
        transformation="average",
        **kwargs,
    )


def annual_usd_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="USD",
        frequency="annual",
        transformation="average",
        **kwargs,
    )


def monthly_eur_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="EUR",
        frequency="monthly",
        transformation="average",
        **kwargs,
    )


def quarterly_eur_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="EUR",
        frequency="quarterly",
        transformation="average",
        **kwargs,
    )


def annual_eur_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="EUR",
        frequency="annual",
        transformation="average",
        **kwargs,
    )


def monthly_xdr_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="XDR",
        frequency="monthly",
        transformation="average",
        **kwargs,
    )


def quarterly_xdr_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="XDR",
        frequency="quarterly",
        transformation="average",
        **kwargs,
    )


def annual_xdr_avg(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return exchange_rates(
        ref_areas=ref_areas,
        base="XDC",
        quote="XDR",
        frequency="annual",
        transformation="average",
        **kwargs,
    )


def monthly_usd_only(ref_areas: str | Sequence[str] | None = None, **kwargs):
    return monthly_usd_avg(ref_areas=ref_areas, **kwargs)

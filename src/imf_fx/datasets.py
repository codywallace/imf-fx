from __future__ import annotations

from typing import Optional, Union
from time import perf_counter, time
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl

from .structure import get_dataflow_structure, codelist_to_df
from .client import fetch_country_usd_series
from .transform import finalize_usd_only


def _is_iso3_like(code: object) -> bool:
    """
    IMF codelists can include special area/aggregate codes (e.g., TX###).
    For the default dataset helper, we only want ISO3-like alpha codes (AAA).
    """
    return isinstance(code, str) and len(code) == 3 and code.isalpha()


def _default_cache_path() -> Path:
    return Path.home() / ".cache" / "imf_fx" / "er_structure.json"


def _load_cached_structure(cache_path: Path, ttl_seconds: int, debug: bool) -> tuple[Optional[dict], bool]:
    """
    Return (struct, hit) where hit=True if we used cache.
    """
    try:
        if not cache_path.exists():
            return None, False
        if cache_path.stat().st_size <= 0:
            return None, False

        age = time() - cache_path.stat().st_mtime
        if age > ttl_seconds:
            return None, False

        txt = cache_path.read_text(encoding="utf-8")
        return json.loads(txt), True
    except Exception as e:
        if debug:
            print(f"[imf-fx] structure cache read failed ({type(e).__name__}: {e}); refetching...")
        return None, False


def _write_cached_structure(cache_path: Path, struct: dict, debug: bool) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(struct), encoding="utf-8")
    except Exception as e:
        if debug:
            print(f"[imf-fx] structure cache write failed ({type(e).__name__}: {e}); continuing...")


def monthly_usd_only(
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit_countries: Optional[int] = None,
    *,
    include_country_name: bool = True,
    categorical_dims: bool = True,
    return_meta: bool = False,
    debug: bool = False,
    timeout: int = 90,
    # NEW:
    parallel: bool = True,
    max_workers: int = 16,
    # Structure cache (NEW):
    cache_structure: bool = True,
    structure_cache_path: Optional[str] = None,
    structure_cache_ttl_seconds: int = 24 * 60 * 60,
) -> Union[pl.DataFrame, tuple[pl.DataFrame, dict]]:
    """
    Fetch IMF ER USD-only monthly exchange rates (domestic per USD) for published countries,
    transform to a normalized table.

    Default return:
        df (Polars DataFrame)

    If return_meta=True:
        (df, meta) where meta includes fetch/transform diagnostics.

    By default (start/end omitted), this helper fetches all available history returned by IMF.

    start/end: optional IMF SDMX period strings like '1957-M01', '2026-M02'

    Performance options:
    - parallel=True uses a thread pool to fetch multiple countries concurrently (I/O-bound).
      Default max_workers=8 is polite to IMF and still much faster than sequential.
    - cache_structure=True caches the ER structure response (~24h) in ~/.cache/imf_fx/er_structure.json

    Notes:
    - Filters the IMF codelist to ISO3-like codes (A–Z, length 3), excluding special codes
      that often return empty series.
    - `timeout` is included for future pass-through; fetch_country_usd_series currently uses its own timeout.
    """
    t0 = perf_counter()

    if debug:
        print("[imf-fx] fetching structure for ER (to discover countries)...")

    # ---- structure fetch with caching ----
    struct = None
    cache_hit = False

    if cache_structure:
        cache_path = Path(structure_cache_path) if structure_cache_path else _default_cache_path()
        struct, cache_hit = _load_cached_structure(cache_path, structure_cache_ttl_seconds, debug=debug)

        if cache_hit and debug:
            print(f"[imf-fx] structure cache hit: {cache_path}")

    if struct is None:
        struct = get_dataflow_structure()
        if cache_structure:
            cache_path = Path(structure_cache_path) if structure_cache_path else _default_cache_path()
            _write_cached_structure(cache_path, struct, debug=debug)

    area_lu = codelist_to_df(struct, "CL_ER_COUNTRY_PUB")

    all_codes = area_lu.select("code").to_series().to_list()
    total_available = len(all_codes)

    countries = [c for c in all_codes if _is_iso3_like(c)]
    filtered_out = total_available - len(countries)

    if limit_countries:
        countries = countries[:limit_countries]

    if debug:
        if filtered_out:
            print(
                f"[imf-fx] countries: {len(countries)} (ISO3-like; filtered out {filtered_out} "
                f"non-ISO3 codes from {total_available})"
            )
        else:
            print(f"[imf-fx] countries: {len(countries)} (from {total_available})")

        if start and end:
            print(f"[imf-fx] fetching monthly USD series for IMF periods {start} to {end}...")
        elif start and not end:
            print(f"[imf-fx] fetching monthly USD series from {start} to latest available...")
        elif end and not start:
            print(f"[imf-fx] fetching monthly USD series up to {end}...")
        else:
            print("[imf-fx] fetching monthly USD series for all available history...")

        if parallel:
            print(f"[imf-fx] parallel fetch enabled: max_workers={max_workers}")

    errors = 0
    dfs: list[pl.DataFrame] = []
    countries_with_data = 0
    rows_raw_total = 0

    # --- fetch helpers ---
    def _fetch_one(code: str) -> tuple[str, pl.DataFrame]:
        raw = fetch_country_usd_series(code, start=start, end=end, timeout=timeout)
        return code, raw

    if not parallel:
        # Sequential (your original behavior)
        for i, c in enumerate(countries, start=1):
            if debug:
                print(f"[imf-fx] ({i}/{len(countries)}) fetching {c} ...", end="", flush=True)

            try:
                raw = fetch_country_usd_series(c, start=start, end=end, timeout=timeout)
                n = int(raw.height) if hasattr(raw, "height") else 0
                rows_raw_total += n

                if n:
                    dfs.append(raw)
                    countries_with_data += 1
                    if debug:
                        print(f" ok ({n} rows)")
                else:
                    if debug:
                        print(" ok (0 rows)")
            except Exception as e:
                errors += 1
                if debug:
                    print(f" ERROR: {type(e).__name__}: {e}")
                # keep going

    else:
        # Parallel (I/O-bound)
        # Note: debug logging from threads can get messy; we only print completion lines here.
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_fetch_one, c): c for c in countries}

            done = 0
            total = len(countries)

            for fut in as_completed(futs):
                c = futs[fut]
                done += 1
                try:
                    code, raw = fut.result()
                    n = int(raw.height) if hasattr(raw, "height") else 0
                    rows_raw_total += n

                    if n:
                        dfs.append(raw)
                        countries_with_data += 1

                    if debug:
                        # single-line progress, stable output
                        print(f"[imf-fx] ({done}/{total}) fetched {code}: {n} rows")

                except Exception as e:
                    errors += 1
                    if debug:
                        print(f"[imf-fx] ({done}/{total}) ERROR {c}: {type(e).__name__}: {e}")

    meta = {
        "countries_requested": len(countries),
        "countries_with_data": countries_with_data,
        "errors": errors,
        "start": start,
        "end": end,
        "rows_raw_total": rows_raw_total,
        "parallel": parallel,
        "max_workers": max_workers if parallel else 1,
        "structure_cache_hit": cache_hit,
    }

    if not dfs:
        elapsed = perf_counter() - t0
        if debug:
            print(f"[imf-fx] finished: no data returned. errors={errors}. elapsed={elapsed:.2f}s")

        empty_df = pl.DataFrame()
        out_meta = {**meta, "rows_final": 0, "elapsed_s": elapsed}

        return (empty_df, out_meta) if return_meta else empty_df

    if debug:
        print(f"[imf-fx] concatenating {len(dfs)} country frames...")

    df_raw = pl.concat(dfs, how="vertical")

    # Optional: report actual period coverage returned
    min_period = None
    max_period = None
    if "TIME_PERIOD" in df_raw.columns and df_raw.height:
        try:
            min_period = df_raw.select(pl.col("TIME_PERIOD").min()).item()
            max_period = df_raw.select(pl.col("TIME_PERIOD").max()).item()
        except Exception:
            min_period = None
            max_period = None

    if debug:
        if min_period and max_period:
            print(f"[imf-fx] raw TIME_PERIOD coverage: {min_period} → {max_period}")
        print("[imf-fx] transforming to normalized schema...")

    df_final = finalize_usd_only(
        df_raw,
        area_lu=area_lu,
        include_country_name=include_country_name,
        categorical_dims=categorical_dims,
    )
    elapsed = perf_counter() - t0

    out_meta = {
        **meta,
        "rows_final": int(df_final.height),
        "elapsed_s": elapsed,
        "min_period": min_period,
        "max_period": max_period,
    }

    if debug:
        print(
            f"[imf-fx] finished: rows_final={out_meta['rows_final']} "
            f"errors={errors} elapsed={elapsed:.2f}s"
        )

    return (df_final, out_meta) if return_meta else df_final
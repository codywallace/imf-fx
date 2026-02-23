from __future__ import annotations

from typing import Optional, Union
from time import perf_counter, time
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl

from .structure import get_dataflow_structure, codelist_to_df
from .client import fetch_countries_usd_series
from .transform import finalize_usd_only


def _is_iso3_like(code: object) -> bool:
    """
    IMF codelists can include special area/aggregate codes (e.g., TX###).
    For the default dataset helper, we only want ISO3-like alpha codes (AAA).
    """
    return isinstance(code, str) and len(code) == 3 and code.isalpha()


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _default_cache_path() -> Path:
    return Path.home() / ".cache" / "imf_fx" / "er_structure.json"


def _load_cached_structure(
    cache_path: Path, ttl_seconds: int, debug: bool
) -> tuple[Optional[dict], bool]:
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


def _fetch_batch_resilient(
    batch: list[str],
    *,
    start: Optional[str],
    end: Optional[str],
    timeout: int,
    debug: bool,
    max_splits: int = 6,
) -> tuple[pl.DataFrame, int]:
    """
    Try fetching a batch. If it fails, split into smaller batches and retry.

    Returns: (df, failures)
      - df: concatenated results (may be empty)
      - failures: number of leaf batches that still failed after splitting

    max_splits controls recursion depth:
      0 => no splitting (single attempt)
      6 => at most 2^6 = 64 leaves (worst case)
    """
    try:
        df = fetch_countries_usd_series(batch, start=start, end=end, timeout=timeout)
        return df, 0
    except Exception as e:
        if debug:
            print(f"[imf-fx] batch ERROR ({len(batch)}): {type(e).__name__}: {e}")

        if max_splits <= 0 or len(batch) <= 1:
            # Give up on this leaf
            return pl.DataFrame(), 1

        mid = len(batch) // 2
        left = batch[:mid]
        right = batch[mid:]

        df_l, fail_l = _fetch_batch_resilient(
            left, start=start, end=end, timeout=timeout, debug=debug, max_splits=max_splits - 1
        )
        df_r, fail_r = _fetch_batch_resilient(
            right, start=start, end=end, timeout=timeout, debug=debug, max_splits=max_splits - 1
        )

        dfs = [d for d in (df_l, df_r) if getattr(d, "height", 0) > 0]
        if dfs:
            return pl.concat(dfs, how="vertical"), (fail_l + fail_r)
        return pl.DataFrame(), (fail_l + fail_r)


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
    # Structure cache:
    cache_structure: bool = True,
    structure_cache_path: Optional[str] = None,
    structure_cache_ttl_seconds: int = 24 * 60 * 60,
    # Batch fetch:
    batch_size: int = 25,
    batch_parallel: bool = True,
    batch_max_workers: int = 3,
    # Resilience:
    batch_max_splits: int = 6,
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

    Performance strategy:
    - Uses batched SDMX keys (COUNTRY joined by '+') to reduce HTTP calls dramatically.
      Example key: USA+JPN+GBR.XDC_USD.PA_RT.M
    - Optionally fetches batches in parallel (few workers; big requests).

    Resilience:
    - If a batch request fails, it is split into smaller batches and retried (binary split).
      `batch_max_splits` limits recursion depth.

    Notes:
    - Filters the IMF country codelist to ISO3-like codes (A–Z, length 3), excluding special codes
      that often return empty series.
    - IMF endpoint may ignore start/end server-side; client enforces time window at parse-time.
    """
    t0 = perf_counter()

    if debug:
        print("[imf-fx] fetching structure for ER (to discover countries)...")

    # ---- structure fetch with caching ----
    struct: Optional[dict] = None
    cache_hit = False

    if cache_structure:
        cache_path = Path(structure_cache_path) if structure_cache_path else _default_cache_path()
        struct, cache_hit = _load_cached_structure(
            cache_path, structure_cache_ttl_seconds, debug=debug
        )
        if cache_hit and debug:
            print(f"[imf-fx] structure cache hit: {cache_path}")

    if struct is None:
        struct = get_dataflow_structure()
        if cache_structure:
            cache_path = (
                Path(structure_cache_path) if structure_cache_path else _default_cache_path()
            )
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

    errors = 0
    dfs: list[pl.DataFrame] = []
    rows_raw_total = 0

    # ---- batched fetch ----
    batches = _chunked(countries, batch_size)

    if debug:
        print(f"[imf-fx] batch fetch: {len(batches)} batches (batch_size={batch_size})")
        if batch_parallel:
            print(f"[imf-fx] batch parallel enabled: batch_max_workers={batch_max_workers}")

    def _fetch_batch(batch: list[str]) -> tuple[pl.DataFrame, int]:
        return _fetch_batch_resilient(
            batch,
            start=start,
            end=end,
            timeout=timeout,
            debug=debug,
            max_splits=batch_max_splits,
        )

    if not batch_parallel:
        for i, batch in enumerate(batches, start=1):
            if debug:
                print(f"[imf-fx] (batch {i}/{len(batches)}) fetching {len(batch)} countries...")
            raw, failures = _fetch_batch(batch)
            errors += failures

            if raw.height:
                dfs.append(raw)
                rows_raw_total += int(raw.height)

    else:
        with ThreadPoolExecutor(max_workers=batch_max_workers) as ex:
            futs = {ex.submit(_fetch_batch, b): b for b in batches}
            done = 0
            total = len(batches)

            for fut in as_completed(futs):
                done += 1
                batch = futs[fut]
                try:
                    raw, failures = fut.result()
                    errors += failures

                    n = int(raw.height) if hasattr(raw, "height") else 0
                    rows_raw_total += n
                    if n:
                        dfs.append(raw)

                    if debug:
                        print(
                            f"[imf-fx] ({done}/{total}) fetched batch({len(batch)}): {n} rows "
                            f"(failures={failures})"
                        )
                except Exception as e:
                    errors += 1
                    if debug:
                        print(
                            f"[imf-fx] ({done}/{total}) ERROR batch({len(batch)}): {type(e).__name__}: {e}"
                        )

    meta_base = {
        "countries_requested": len(countries),
        "errors": errors,
        "start": start,
        "end": end,
        "rows_raw_total": rows_raw_total,
        "structure_cache_hit": cache_hit,
        "batch_size": batch_size,
        "batch_parallel": batch_parallel,
        "batch_max_workers": batch_max_workers if batch_parallel else 1,
        "batch_max_splits": batch_max_splits,
    }

    if not dfs:
        elapsed = perf_counter() - t0
        if debug:
            print(f"[imf-fx] finished: no data returned. errors={errors}. elapsed={elapsed:.2f}s")

        empty_df = pl.DataFrame()
        out_meta = {**meta_base, "countries_with_data": 0, "rows_final": 0, "elapsed_s": elapsed}
        return (empty_df, out_meta) if return_meta else empty_df

    if debug:
        print(f"[imf-fx] concatenating {len(dfs)} batch frames...")

    df_raw = pl.concat(dfs, how="vertical")

    # countries_with_data derived from raw (unique COUNTRY)
    countries_with_data = 0
    if df_raw.height and "COUNTRY" in df_raw.columns:
        try:
            countries_with_data = df_raw.select(pl.col("COUNTRY").n_unique()).item()
        except Exception:
            countries_with_data = 0

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
        **meta_base,
        "countries_with_data": countries_with_data,
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

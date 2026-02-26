# Internal module. Not part of the public API.
# Subject to change without notice.

from __future__ import annotations

from typing import Any

import polars as pl

from .config import BASE_URL, DATAFLOW_AGENCY, DATAFLOW_ID
from .exceptions import StructureNotFound
from .http import get_json_session


def get_dataflow_structure(
    dataflow_agency: str = DATAFLOW_AGENCY,
    dataflow_id: str = DATAFLOW_ID,
) -> dict[str, Any]:
    url = f"{BASE_URL}/structure/dataflow/{dataflow_agency}/{dataflow_id}/+"
    params = {"detail": "full", "references": "descendants"}
    return get_json_session(url, params=params, timeout=90)


def codelist_to_df(struct: dict[str, Any], cl_id: str) -> pl.DataFrame:
    try:
        codelists = struct["data"]["codelists"]
    except Exception as e:
        raise StructureNotFound("No codelists in structure payload") from e

    for cl in codelists:
        if cl.get("id") == cl_id:
            return pl.from_dicts(
                [
                    {"code": c["id"], "label_en": c.get("names", {}).get("en")}
                    for c in cl.get("codes", [])
                ]
            )

    raise StructureNotFound(f"Missing codelist: {cl_id}")


def er_indicator_codelist(struct: dict[str, Any]) -> pl.DataFrame:
    return codelist_to_df(struct, "CL_ER_INDICATOR_PUB")


def er_country_codelist(struct: dict[str, Any]) -> pl.DataFrame:
    return codelist_to_df(struct, "CL_ER_COUNTRY_PUB")

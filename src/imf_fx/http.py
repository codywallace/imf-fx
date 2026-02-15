from __future__ import annotations
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import IMF_API_KEY

def imf_headers() -> Dict[str, str]:
    if not IMF_API_KEY:
        return {"Accept": "application/json"}
    return {"Accept": "application/json", "X-API-KEY": IMF_API_KEY}

SESSION = requests.Session()
retries = Retry(
    total=6,
    backoff_factor=0.7,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

def get_json_session(url: str, params: Optional[dict] = None, timeout: int = 90) -> Dict[str, Any]:
    r = SESSION.get(url, params=params, headers=imf_headers(), timeout=timeout)
    r.raise_for_status()
    return r.json()
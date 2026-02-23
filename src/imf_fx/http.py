# Internal module. Not part of the public API.
# Subject to change without notice.

from __future__ import annotations

from typing import Optional, Dict, Any
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import IMF_API_KEY

_thread_local = threading.local()


def imf_headers() -> Dict[str, str]:
    if not IMF_API_KEY:
        return {"Accept": "application/json"}
    return {"Accept": "application/json", "X-API-KEY": IMF_API_KEY}


def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = _build_session()
        _thread_local.session = s
    return s


def get_json_session(url: str, params: Optional[dict] = None, timeout: int = 90) -> Dict[str, Any]:
    sess = _get_session()
    r = sess.get(url, params=params, headers=imf_headers(), timeout=timeout)
    r.raise_for_status()
    return r.json()

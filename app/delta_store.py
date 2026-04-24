"""Delta access via databricks-sql-connector.

Works on:
  - Databricks Apps (auto SP OAuth via SDK Config)
  - Render / external (DATABRICKS_TOKEN env var as PAT)
"""
from __future__ import annotations

import contextlib
import logging
from typing import Any, Iterable, List, Optional

from databricks import sql

import config as cfg

log = logging.getLogger(__name__)


def _server_hostname() -> str:
    host = cfg.DATABRICKS_HOST
    if host.startswith("https://"):
        host = host[len("https://"):]
    if host.startswith("http://"):
        host = host[len("http://"):]
    return host


def _token_provider():
    """Return an access token. Prefers PAT from env, falls back to SDK OAuth."""
    # Option 1: explicit PAT (Render / external)
    if cfg.DATABRICKS_TOKEN:
        return cfg.DATABRICKS_TOKEN

    # Option 2: SDK credential chain (Databricks Apps — SP OAuth)
    try:
        from databricks.sdk.core import Config
        c = Config()
        hdr = c.authenticate()
        auth = hdr.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:]
    except Exception as e:
        log.warning("SDK auth failed: %s", e)

    raise RuntimeError("No auth configured. Set DATABRICKS_TOKEN or run inside Databricks Apps.")


@contextlib.contextmanager
def _conn():
    host = _server_hostname()
    if not host or not cfg.DATABRICKS_WAREHOUSE_HTTP_PATH:
        raise RuntimeError(
            "DATABRICKS_HOST and DATABRICKS_WAREHOUSE_ID (or _HTTP_PATH) must be set"
        )
    token = _token_provider()
    conn = sql.connect(
        server_hostname=host,
        http_path=cfg.DATABRICKS_WAREHOUSE_HTTP_PATH,
        access_token=token,
    )
    try:
        yield conn
    finally:
        conn.close()


def _safe_value(v: Any) -> Any:
    """Convert connector types to JSON-safe Python primitives."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (list, tuple)):
        return [_safe_value(x) for x in v]
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    try:
        return [_safe_value(x) for x in v]
    except TypeError:
        pass
    return str(v)


def query(statement: str, params: Optional[dict] = None) -> List[dict]:
    """Run a SELECT; return rows as dicts with JSON-safe values."""
    with _conn() as c, c.cursor() as cur:
        cur.execute(statement, params or {})
        cols = [d[0] for d in cur.description] if cur.description else []
        return [
            {k: _safe_value(v) for k, v in zip(cols, row)}
            for row in cur.fetchall()
        ]


def execute(statement: str, params: Optional[dict] = None) -> int:
    """Run a non-SELECT statement; return rowcount (may be -1 on Databricks)."""
    with _conn() as c, c.cursor() as cur:
        cur.execute(statement, params or {})
        return cur.rowcount


def execute_many(statement: str, seq_of_params: Iterable[dict]) -> int:
    """Run the same statement multiple times. Returns count of executions."""
    n = 0
    with _conn() as c, c.cursor() as cur:
        for p in seq_of_params:
            cur.execute(statement, p)
            n += 1
    return n

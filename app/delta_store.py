"""Delta access via databricks-sql-connector, using the App's service-principal OAuth.

All queries target the SQL Warehouse configured via DATABRICKS_WAREHOUSE_ID (or
DATABRICKS_WAREHOUSE_HTTP_PATH). Auth falls back to the SDK's default credential
provider, which inside a Databricks App resolves to the app's service principal.
"""
from __future__ import annotations

import contextlib
import logging
from typing import Any, Iterable, List, Optional

from databricks import sql
from databricks.sdk.core import Config

from app import config as cfg

log = logging.getLogger(__name__)


def _server_hostname() -> str:
    host = cfg.DATABRICKS_HOST
    if host.startswith("https://"):
        host = host[len("https://") :]
    if host.startswith("http://"):
        host = host[len("http://") :]
    return host


def _token_provider():
    """Return a fresh OAuth access token using the default credential chain."""
    c = Config()
    # The SDK's token is callable; we fetch a string each time.
    hdr = c.authenticate()
    auth = hdr.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:]
    # Fall back to env token (personal access token) if present.
    import os

    return os.getenv("DATABRICKS_TOKEN", "")


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


def query(statement: str, params: Optional[dict] = None) -> List[dict]:
    """Run a SELECT; return rows as dicts."""
    with _conn() as c, c.cursor() as cur:
        cur.execute(statement, params or {})
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]


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

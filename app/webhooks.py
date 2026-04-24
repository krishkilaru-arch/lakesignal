"""Webhook subscription management + outbound dispatch, backed by Delta."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import httpx

import config as cfg
from delta_store import execute, query

log = logging.getLogger(__name__)


def create_subscription(url: str, filters: Optional[dict] = None) -> dict:
    sub_id = str(uuid.uuid4())
    sec = secrets.token_urlsafe(24)
    now = datetime.now(timezone.utc).isoformat()
    execute(
        f"""
        INSERT INTO {cfg.T_WEBHOOKS}
          (id, url, secret, filters, active, created_at)
        VALUES (%(id)s, %(url)s, %(secret)s, %(filters)s, true, %(created_at)s)
        """,
        {
            "id": sub_id,
            "url": url,
            "secret": sec,
            "filters": json.dumps(filters or {}),
            "created_at": now,
        },
    )
    return {
        "id": sub_id,
        "url": url,
        "secret": sec,
        "filters": filters or {},
        "active": True,
        "created_at": now,
    }


def list_subscriptions(include_secret: bool = False) -> List[dict]:
    cols = "id, url, filters, active, created_at" + (", secret" if include_secret else "")
    rows = query(f"SELECT {cols} FROM {cfg.T_WEBHOOKS} ORDER BY created_at DESC")
    for r in rows:
        try:
            r["filters"] = json.loads(r.get("filters") or "{}")
        except Exception:  # noqa: BLE001
            r["filters"] = {}
    return rows


def delete_subscription(sub_id: str) -> bool:
    execute(f"DELETE FROM {cfg.T_WEBHOOKS} WHERE id = %(id)s", {"id": sub_id})
    remaining = query(
        f"SELECT id FROM {cfg.T_WEBHOOKS} WHERE id = %(id)s", {"id": sub_id}
    )
    return len(remaining) == 0


def _matches_filters(payload: dict, filters: dict) -> bool:
    if not filters:
        return True
    if "ticker" in filters and payload.get("ticker") != filters["ticker"]:
        return False
    if "direction" in filters and payload.get("direction") != filters["direction"]:
        return False
    if "min_magnitude" in filters:
        try:
            if int(payload.get("magnitude", 0)) < int(filters["min_magnitude"]):
                return False
        except (TypeError, ValueError):
            return False
    if "min_confidence" in filters:
        try:
            if float(payload.get("confidence", 0)) < float(filters["min_confidence"]):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def dispatch(impacts: Iterable[dict]) -> None:
    subs = [s for s in list_subscriptions(include_secret=True) if s.get("active")]
    if not subs:
        return

    with httpx.Client(timeout=10.0, trust_env=False) as client:
        for imp in impacts:
            payload = {"type": "impact.created", **imp}
            body = json.dumps(payload, separators=(",", ":"), default=str).encode()
            for sub in subs:
                if not _matches_filters(payload, sub["filters"]):
                    continue
                headers = {
                    "content-type": "application/json",
                    "x-lakesignal-signature": _sign(body, sub["secret"]),
                    "x-lakesignal-event": "impact.created",
                }
                try:
                    resp = client.post(sub["url"], content=body, headers=headers)
                    log.info(
                        "Webhook -> %s [%s] status=%s", sub["url"], sub["id"], resp.status_code
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("Webhook -> %s [%s] FAILED: %s", sub["url"], sub["id"], e)

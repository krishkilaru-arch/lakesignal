"""Ticker resolution — same logic as the SQLite prototype, backed by Delta."""
from __future__ import annotations

import re
import threading
import time
from typing import Dict, List, Tuple

from app import config as cfg
from app.delta_store import query

AMBIGUOUS_WHEN_BARE = {"A", "T", "V", "C", "F", "GE", "GM", "MS", "UA", "USA", "US", "NY", "AI", "IT"}

SYMBOL_DOLLAR_RE = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)\b")
SYMBOL_PAREN_RE = re.compile(r"\(([A-Z]{1,5}(?:\.[A-Z])?)\)")
SYMBOL_COLON_RE = re.compile(r"\b(?:NYSE|NASDAQ|AMEX)\s*:\s*([A-Z]{1,5}(?:\.[A-Z])?)\b")

# Cache tickers for 10 minutes to avoid hitting the warehouse on every request.
_CACHE_TTL_SEC = 600
_cache_lock = threading.Lock()
_cache: Dict[str, object] = {"ts": 0.0, "by_symbol": {}, "alias_pairs": []}


def _refresh() -> Tuple[Dict[str, dict], List[Tuple[str, str]]]:
    rows = query(f"SELECT symbol, company_name, sector, industry, aliases, exchange FROM {cfg.T_TICKERS}")
    by_symbol: Dict[str, dict] = {}
    pairs: List[Tuple[str, str]] = []
    for r in rows:
        sym = (r.get("symbol") or "").upper()
        if not sym:
            continue
        by_symbol[sym] = r
        name = (r.get("company_name") or "").lower()
        if name:
            pairs.append((name, sym))
            short = re.sub(
                r",?\s+(inc\.?|corp\.?|corporation|company|co\.?|ltd\.?|plc|s\.a\.|n\.v\.|group|holdings?|technologies|limited)$",
                "",
                name,
            ).strip()
            if short and short != name and len(short) >= 4:
                pairs.append((short, sym))
        for a in (r.get("aliases") or "").split("|"):
            a = a.strip().lower()
            if a and len(a) >= 3:
                pairs.append((a, sym))
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return by_symbol, pairs


def _get_maps() -> Tuple[Dict[str, dict], List[Tuple[str, str]]]:
    with _cache_lock:
        if time.time() - float(_cache["ts"]) < _CACHE_TTL_SEC and _cache["by_symbol"]:
            return _cache["by_symbol"], _cache["alias_pairs"]  # type: ignore[return-value]
        by_symbol, pairs = _refresh()
        _cache["by_symbol"] = by_symbol
        _cache["alias_pairs"] = pairs
        _cache["ts"] = time.time()
        return by_symbol, pairs


def reload_cache() -> None:
    with _cache_lock:
        _cache["ts"] = 0.0


def resolve(text: str) -> List[str]:
    if not text:
        return []
    by_symbol, pairs = _get_maps()
    hits: List[str] = []
    seen: set[str] = set()

    def add(sym: str) -> None:
        if sym in by_symbol and sym not in seen:
            seen.add(sym)
            hits.append(sym)

    for m in SYMBOL_DOLLAR_RE.findall(text):
        add(m.upper())
    for m in SYMBOL_PAREN_RE.findall(text):
        add(m.upper())
    for m in SYMBOL_COLON_RE.findall(text):
        add(m.upper())

    low = text.lower()
    for needle, sym in pairs:
        if sym in seen:
            continue
        idx = low.find(needle)
        if idx < 0:
            continue
        before_ok = idx == 0 or not low[idx - 1].isalnum()
        end = idx + len(needle)
        after_ok = end == len(low) or not low[end].isalnum()
        if before_ok and after_ok:
            add(sym)

    for m in re.findall(r"\b([A-Z]{2,5}(?:\.[A-Z])?)\b", text):
        u = m.upper()
        if u in AMBIGUOUS_WHEN_BARE:
            continue
        if u in by_symbol:
            add(u)

    return hits


def ticker_details(symbol: str) -> dict | None:
    by_symbol, _ = _get_maps()
    return by_symbol.get(symbol.upper())

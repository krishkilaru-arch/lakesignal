"""Impact analyzer — calls the configured Databricks serving endpoint.

Uses the OpenAI-compatible `/serving-endpoints` API so we can set
`response_format={"type": "json_object"}` for reliable structured output.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Dict, List, Optional

from databricks.sdk.core import Config
from openai import OpenAI

from app import config as cfg
from app.resolver import ticker_details

log = logging.getLogger(__name__)


def _client() -> OpenAI:
    c = Config()
    hdr = c.authenticate()
    auth = hdr.get("Authorization", "")
    token = auth[7:] if auth.lower().startswith("bearer ") else ""
    return OpenAI(api_key=token, base_url=f"{cfg.DATABRICKS_HOST}/serving-endpoints")


SYSTEM_PROMPT = """You are a sell-side equity analyst. Given one news story and a
list of potentially-impacted tickers, score the likely short-term stock impact for
each ticker. Be calibrated, not dramatic: most stories are low-magnitude.

Return ONLY valid JSON matching this schema (no prose, no markdown):

{
  "impacts": [
    {
      "ticker": "AAPL",
      "direction": "positive" | "negative" | "neutral",
      "sentiment_score": -1.0 to 1.0,
      "magnitude": integer 1-10,
      "predicted_move_pct_1d": number (typical range -6 to 6),
      "predicted_move_pct_5d": number (typical range -10 to 10),
      "confidence": 0.0 to 1.0,
      "risk_tags": [ "earnings" | "m&a" | "regulatory" | "litigation" | "supply_chain" | "cyber" | "management" | "macro" | "product" | "labor" | "geopolitical" ],
      "rationale": "one sentence, <=160 chars"
    }
  ]
}
"""


def analyze(headline: str, body: str, tickers: List[str]) -> Dict[str, dict]:
    if not tickers:
        return {}

    ticker_lines = []
    for t in tickers:
        d = ticker_details(t) or {}
        ticker_lines.append(f"- {t}: {d.get('company_name', '')} ({d.get('sector', '')})")

    user = (
        f"Headline: {headline}\n\n"
        f"Body: {(body or '')[:2000]}\n\n"
        f"Tickers to score:\n" + "\n".join(ticker_lines) + "\n\n"
        "Output the JSON now."
    )

    try:
        resp = _client().chat.completions.create(
            model=cfg.MODEL_ENDPOINT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            max_tokens=1024,
            temperature=0.2,
        )
    except Exception as e:  # noqa: BLE001
        log.warning("Serving endpoint call failed: %s", e)
        return {}

    text = resp.choices[0].message.content or "{}"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            log.warning("No JSON in model output: %r", text[:200])
            return {}
        try:
            parsed = json.loads(m.group(0))
        except json.JSONDecodeError:
            log.warning("Malformed model JSON: %r", text[:200])
            return {}

    out: Dict[str, dict] = {}
    for entry in parsed.get("impacts", []):
        tk = str(entry.get("ticker", "")).upper()
        if tk not in tickers:
            continue
        try:
            out[tk] = {
                "direction": entry["direction"],
                "sentiment_score": float(entry["sentiment_score"]),
                "magnitude": int(entry["magnitude"]),
                "predicted_move_pct_1d": float(entry.get("predicted_move_pct_1d", 0.0)),
                "predicted_move_pct_5d": float(entry.get("predicted_move_pct_5d", 0.0)),
                "confidence": float(entry["confidence"]),
                "risk_tags": list(entry.get("risk_tags") or []),
                "rationale": str(entry.get("rationale", ""))[:160],
            }
        except (KeyError, ValueError, TypeError) as e:
            log.warning("Skipping malformed entry %s: %s", entry, e)
    return out

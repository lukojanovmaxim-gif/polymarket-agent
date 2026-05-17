"""
Polymarket API client.

Gamma API  (https://gamma-api.polymarket.com) — market discovery and prices, no auth.
CLOB API   (https://clob.polymarket.com)       — orderbook data, no auth for reads.

Gamma API market format (actual):
  outcomes       — JSON string: '["Yes", "No"]'
  outcomePrices  — JSON string: '["0.51", "0.49"]'
  clobTokenIds   — JSON string: '["<yes_token_id>", "<no_token_id>"]'
  conditionId    — hex condition ID
  volumeNum      — float volume
  liquidityNum   — float liquidity

All markets are normalized to a flat dict with these keys:
  id, question, yes_price, no_price, price_sum,
  yes_token_id, no_token_id, volume, volume_24hr,
  liquidity, end_date, active, closed, slug, spread
"""
import json
import logging

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

logger = logging.getLogger(__name__)


def _normalize(raw: dict) -> dict | None:
    """Convert a Gamma API market dict to internal format. Returns None for non-binary markets."""
    try:
        outcomes = json.loads(raw.get("outcomes") or "[]")
        prices_raw = json.loads(raw.get("outcomePrices") or "[]")
    except (json.JSONDecodeError, TypeError):
        return None

    if len(outcomes) != 2 or len(prices_raw) != 2:
        return None

    outcomes_lower = [str(o).lower() for o in outcomes]

    # Accept yes/no or up/down (btc-updown markets use Up/Down)
    if "yes" in outcomes_lower and "no" in outcomes_lower:
        yes_idx = outcomes_lower.index("yes")
        no_idx  = outcomes_lower.index("no")
    elif "up" in outcomes_lower and "down" in outcomes_lower:
        yes_idx = outcomes_lower.index("up")
        no_idx  = outcomes_lower.index("down")
    else:
        return None

    try:
        yes_price = float(prices_raw[yes_idx])
        no_price  = float(prices_raw[no_idx])
    except (ValueError, IndexError):
        return None

    # CLOB token IDs (used for orderbook lookups)
    clob_ids: list[str] = []
    try:
        clob_ids = json.loads(raw.get("clobTokenIds") or "[]")
    except (json.JSONDecodeError, TypeError):
        pass

    yes_token_id = clob_ids[yes_idx] if len(clob_ids) > yes_idx else ""
    no_token_id  = clob_ids[no_idx]  if len(clob_ids) > no_idx  else ""

    cid = raw.get("conditionId") or raw.get("id", "")

    return {
        "id": cid,
        "question": raw.get("question", ""),
        "yes_price": yes_price,
        "no_price": no_price,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "price_sum": round(yes_price + no_price, 4),
        "volume": float(raw.get("volumeNum") or raw.get("volume") or 0),
        "volume_24hr": float(raw.get("volume24hr") or 0),
        "liquidity": float(raw.get("liquidityNum") or raw.get("liquidity") or 0),
        "end_date": raw.get("endDate") or raw.get("endDateIso") or "",
        "active":     bool(raw.get("active", True)),
        "closed":     bool(raw.get("closed", False)),
        "restricted": bool(raw.get("restricted", False)),
        "slug":       raw.get("slug", ""),
        "spread":     float(raw.get("spread") or 0),
    }


class PolymarketClient:
    def __init__(self, timeout: float = 10.0):
        self._timeout = timeout

    async def get_markets(
        self,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        search: str = "",
    ) -> list[dict]:
        """Fetch and normalize binary markets from the Gamma API."""
        params: dict = {
            "limit": limit,
            "active": "true" if active else "false",
            "closed": "true" if closed else "false",
        }
        if search:
            params["_q"] = search

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(f"{GAMMA_BASE}/markets", params=params)
                resp.raise_for_status()
                raw_list = resp.json()
        except Exception as exc:
            logger.warning("PolyClient.get_markets error: %s", exc)
            return []

        result = []
        for raw in raw_list:
            m = _normalize(raw)
            if m and m["active"] and not m["closed"]:
                result.append(m)
        return result

    async def get_market(self, condition_id: str) -> dict | None:
        """Fetch a single market by condition_id from the Gamma API."""
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(f"{GAMMA_BASE}/markets/{condition_id}")
                resp.raise_for_status()
                return _normalize(resp.json())
        except Exception as exc:
            logger.warning("PolyClient.get_market(%s) error: %s", condition_id, exc)
            return None

    async def get_orderbook(self, token_id: str) -> dict:
        """Fetch CLOB orderbook for a single token (best bids/asks)."""
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            logger.warning("PolyClient.get_orderbook(%s) error: %s", token_id[:16], exc)
            return {}

    async def close(self) -> None:
        pass  # httpx clients are created/closed per-request

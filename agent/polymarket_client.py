"""
Polymarket API client.

Gamma API  (https://gamma-api.polymarket.com) — market discovery and prices, no auth.
CLOB API   (https://clob.polymarket.com)       — orderbook data, no auth for reads.

All market data is normalized to a flat dict with these keys:
  id, question, yes_price, no_price, price_sum,
  yes_token_id, no_token_id, volume, volume_24hr,
  liquidity, end_date, active, closed, tags, spread
"""
import logging

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

logger = logging.getLogger(__name__)


def _normalize(raw: dict) -> dict | None:
    """Convert a Gamma API market dict to internal format. Returns None for non-binary markets."""
    tokens = raw.get("tokens") or []
    yes_tok = next((t for t in tokens if str(t.get("outcome", "")).lower() == "yes"), None)
    no_tok = next((t for t in tokens if str(t.get("outcome", "")).lower() == "no"), None)
    if not yes_tok or not no_tok:
        return None

    try:
        yes_price = float(yes_tok.get("price") or 0.5)
        no_price = float(no_tok.get("price") or 0.5)
    except (TypeError, ValueError):
        return None

    tags = [t.get("slug", "") for t in (raw.get("tags") or [])]
    cid = raw.get("condition_id") or raw.get("id", "")

    return {
        "id": cid,
        "question": raw.get("question", ""),
        "yes_price": yes_price,
        "no_price": no_price,
        "yes_token_id": yes_tok.get("token_id", ""),
        "no_token_id": no_tok.get("token_id", ""),
        "price_sum": round(yes_price + no_price, 4),
        "volume": float(raw.get("volume") or 0),
        "volume_24hr": float(raw.get("volume_24hr") or 0),
        "liquidity": float(raw.get("liquidity") or 0),
        "end_date": raw.get("endDate") or raw.get("end_date_iso") or "",
        "active": bool(raw.get("active", True)),
        "closed": bool(raw.get("closed", False)),
        "tags": tags,
        "spread": float(raw.get("spread") or 0),
    }


class PolymarketClient:
    def __init__(self, timeout: float = 10.0):
        self._timeout = timeout

    async def get_markets(
        self,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        tag_slug: str = "",
        search: str = "",
        order: str = "volume",
        ascending: bool = False,
    ) -> list[dict]:
        """Fetch and normalize binary markets from the Gamma API."""
        params: dict = {
            "limit": limit,
            "active": "true" if active else "false",
            "closed": "true" if closed else "false",
            "order": order,
            "ascending": "true" if ascending else "false",
        }
        if tag_slug:
            params["tag_slug"] = tag_slug
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

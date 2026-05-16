"""
Polymarket market scanner — runs every 30 seconds.

Fetches markets in three pools so each strategy sees the right data:
  general — top 200 open markets by volume (Gamma API default sort)
  crypto  — subset of general where question contains BTC/ETH keywords
  sports  — subset of general where question contains sports keywords

Filtering is done locally after fetching the general pool to avoid
relying on Gamma API tag parameters that may not be supported.

Detects whale activity: YES price swing ≥ WHALE_DELTA in one scan interval.
"""
import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

from agent.polymarket_client import PolymarketClient

logger = logging.getLogger(__name__)

SCAN_INTERVAL_S = 30
HISTORY_DEPTH = 10
WHALE_DELTA = 0.05   # 5¢ YES price swing triggers whale alert
GENERAL_LIMIT = 200  # fetch a wide pool and split locally

CRYPTO_KWS = ("bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "sol")
SPORTS_KWS = ("nfl", "nba", "mlb", "nhl", "soccer", "mls", "ufc", "nascar",
              "super bowl", "world series", "stanley cup", "game ", " game",
              " win ", "beat ", " vs ", "match")


@dataclass
class MarketSnapshot:
    id: str
    yes_price: float
    no_price: float
    price_sum: float
    volume: float
    ts: datetime


@dataclass
class WhaleAlert:
    id: str
    question: str
    delta: float        # positive = YES moved up
    current_yes: float


WhaleCallback = Callable[[WhaleAlert], Awaitable[None]]


class MarketScanner:
    def __init__(self, client: PolymarketClient, on_whale: WhaleCallback | None = None):
        self._client = client
        self._on_whale = on_whale
        self._pools: dict[str, list[dict]] = {"crypto": [], "sports": [], "general": []}
        self._history: dict[str, deque[MarketSnapshot]] = defaultdict(
            lambda: deque(maxlen=HISTORY_DEPTH)
        )
        self._lock = asyncio.Lock()
        self._running = False

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def markets(self) -> list[dict]:
        """All markets from all pools, deduplicated by id."""
        seen: set[str] = set()
        result: list[dict] = []
        for pool in self._pools.values():
            for m in pool:
                mid = m.get("id", "")
                if mid and mid not in seen:
                    seen.add(mid)
                    result.append(m)
        return result

    def markets_for(self, category: str) -> list[dict]:
        """Markets from a specific pool (crypto / sports / general)."""
        return list(self._pools.get(category, []))

    def pool_sizes(self) -> dict[str, int]:
        return {cat: len(pool) for cat, pool in self._pools.items()}

    # ── Background task ───────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        logger.info("MarketScanner started — interval %ds | pool size %d", SCAN_INTERVAL_S, GENERAL_LIMIT)
        while self._running:
            await self._scan()
            await asyncio.sleep(SCAN_INTERVAL_S)

    def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _is_crypto(m: dict) -> bool:
        q = m.get("question", "").lower()
        return any(kw in q for kw in CRYPTO_KWS)

    @staticmethod
    def _is_sports(m: dict) -> bool:
        q = m.get("question", "").lower()
        return any(kw in q for kw in SPORTS_KWS)

    async def _scan(self) -> None:
        try:
            # Fetch the general pool — split into category sub-pools locally
            raw = await self._client.get_markets(limit=GENERAL_LIMIT)
            if not raw:
                logger.warning("Scanner: Gamma API returned 0 markets")
                return

            new_pools: dict[str, list[dict]] = {
                "general": raw,
                "crypto":  [m for m in raw if self._is_crypto(m)],
                "sports":  [m for m in raw if self._is_sports(m)],
            }

            async with self._lock:
                self._pools = new_pools

            # Whale detection
            now = datetime.now(timezone.utc)
            for m in self.markets:
                mid = m.get("id", "")
                snap = MarketSnapshot(
                    id=mid,
                    yes_price=m.get("yes_price", 0.5),
                    no_price=m.get("no_price", 0.5),
                    price_sum=m.get("price_sum", 1.0),
                    volume=m.get("volume", 0),
                    ts=now,
                )
                hist = self._history[mid]
                if hist and self._on_whale:
                    delta = snap.yes_price - hist[-1].yes_price
                    if abs(delta) >= WHALE_DELTA:
                        asyncio.create_task(self._on_whale(WhaleAlert(
                            id=mid,
                            question=m.get("question", ""),
                            delta=round(delta, 4),
                            current_yes=snap.yes_price,
                        )))
                hist.append(snap)

            sizes = {cat: len(p) for cat, p in new_pools.items()}
            logger.info("Scanner: pool sizes %s | total %d unique", sizes, len(self.markets))

        except Exception as exc:
            logger.error("Scan error: %s", exc)

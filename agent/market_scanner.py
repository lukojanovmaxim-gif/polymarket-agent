"""
Polymarket market scanner — runs every 30 seconds.

Fetches markets in three pools so each strategy sees the right data:
  crypto  — BTC/ETH/crypto tagged markets
  sports  — sports game markets
  general — top 100 open markets by volume (broad opportunity scan)

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
WHALE_DELTA = 0.05  # 5¢ YES price swing triggers whale alert

CATEGORY_TAGS: dict[str, list[str]] = {
    "crypto":  ["crypto", "bitcoin", "ethereum"],
    "sports":  ["sports", "nfl", "nba", "mlb", "nhl", "soccer"],
}
GENERAL_LIMIT = 100


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
        self._pools: dict[str, list[dict]] = {cat: [] for cat in [*CATEGORY_TAGS, "general"]}
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
        logger.info("MarketScanner started — interval %ds | tags: %s", SCAN_INTERVAL_S, CATEGORY_TAGS)
        while self._running:
            await self._scan()
            await asyncio.sleep(SCAN_INTERVAL_S)

    def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch_tag_pool(self, category: str, tags: list[str]) -> list[dict]:
        markets: list[dict] = []
        seen: set[str] = set()
        for tag in tags:
            try:
                batch = await self._client.get_markets(limit=100, tag_slug=tag)
                for m in batch:
                    mid = m.get("id", "")
                    if mid and mid not in seen:
                        seen.add(mid)
                        markets.append(m)
            except Exception as exc:
                logger.debug("Scanner: %s/%s skipped: %s", category, tag, exc)
        return markets

    async def _scan(self) -> None:
        try:
            category_keys = list(CATEGORY_TAGS.keys())
            coros = [
                self._fetch_tag_pool(cat, CATEGORY_TAGS[cat])
                for cat in category_keys
            ] + [self._client.get_markets(limit=GENERAL_LIMIT, order="volume")]

            results = await asyncio.gather(*coros, return_exceptions=True)

            new_pools: dict[str, list[dict]] = {}
            for cat, result in zip(category_keys, results[:-1]):
                if isinstance(result, Exception):
                    logger.warning("Scanner: %s pool error: %s", cat, result)
                    new_pools[cat] = self._pools.get(cat, [])
                else:
                    new_pools[cat] = result

            gen = results[-1]
            if isinstance(gen, Exception):
                logger.warning("Scanner: general pool error: %s", gen)
                new_pools["general"] = self._pools.get("general", [])
            else:
                new_pools["general"] = gen

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

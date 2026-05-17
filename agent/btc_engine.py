"""
BtcUpDownEngine — contrarian + arb strategy for BTC/ETH Up-or-Down markets.

Fetches active "btc-updown" and "eth-updown" markets from the Gamma API every
60 seconds (sorted by startDate descending). Three entry signals per market:

  1. ARB: YES + NO < 0.97 — buy both sides simultaneously
  2. CONTRARIAN UP:   YES < 0.35 (market bearish) + asset actually rising → buy YES
  3. CONTRARIAN DOWN: YES > 0.65 (market bullish) + asset actually falling → buy NO

Asset price trend uses the Coinbase public spot API, comparing the current price
to the reading from ~5 minutes ago stored in an in-memory rolling buffer.

Position sizing: 2% of balance per trade, minimum $5.
Each (condition_id, signal_type) pair is traded at most once per session.
"""
import asyncio
import logging
import time
from collections import deque

import httpx

from agent.ledger import Ledger
from agent.polymarket_client import _normalize
from agent.risk_manager import RiskManager

logger = logging.getLogger(__name__)

GAMMA_BASE        = "https://gamma-api.polymarket.com"
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/{pair}/spot"

POLL_INTERVAL_S  = 60
ARB_THRESHOLD    = 0.97
CONTRARIAN_LOW   = 0.35    # buy YES when market this bearish but price rising
CONTRARIAN_HIGH  = 0.65    # buy NO  when market this bullish but price falling
MAX_BALANCE_PCT  = 0.02    # 2% of balance per trade
MIN_TRADE_USD    = 5.0
PRICE_LOOKBACK_S = 300     # 5 minutes for trend comparison
MIN_TREND_MOVE   = 0.001   # 0.1% price move required to declare a trend

ASSETS = [
    {"slug_kw": "btc-updown", "pair": "BTC-USD", "label": "BTC"},
    {"slug_kw": "eth-updown", "pair": "ETH-USD", "label": "ETH"},
]


class BtcUpDownEngine:
    def __init__(
        self,
        ledger: Ledger,
        risk: RiskManager,
        paper_mode: bool,
        get_state,
        get_balance,
    ) -> None:
        self._ledger      = ledger
        self._risk        = risk
        self._paper_mode  = paper_mode
        self._get_state   = get_state
        self._get_balance = get_balance

        # (condition_id, signal_type) traded this session
        self._traded: set[tuple[str, str]] = set()
        self._fired   = 0
        self._running = False

        # Rolling price buffer per asset pair: deque of (monotonic_ts, price)
        self._price_history: dict[str, deque] = {
            a["pair"]: deque(maxlen=20) for a in ASSETS
        }

        # Last-tick market scan results
        self._markets_found_last_tick: dict[str, int] = {a["label"]: 0 for a in ASSETS}
        self._last_market: dict[str, dict | None] = {a["label"]: None for a in ASSETS}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        logger.info("BtcUpDownEngine started — polling every %ds", POLL_INTERVAL_S)
        while self._running:
            await asyncio.sleep(POLL_INTERVAL_S)
            if self._get_state() == "running":
                await self._tick()

    def stop(self) -> None:
        self._running = False

    def btc_status(self) -> dict:
        recent_prices = {
            pair: [
                {"price": round(p, 2), "age_s": round(time.monotonic() - ts)}
                for ts, p in list(hist)[-5:]
            ]
            for pair, hist in self._price_history.items()
        }
        def _market_summary(label: str) -> dict | None:
            m = self._last_market.get(label)
            if m is None:
                return None
            return {
                "question":  m["question"],
                "yes_price": m["yes_price"],
                "no_price":  m["no_price"],
            }

        return {
            "running":                 self._running,
            "trades_fired":            self._fired,
            "markets_traded":          [f"{cid[:16]}:{sig}" for cid, sig in list(self._traded)[:20]],
            "price_snapshots":         recent_prices,
            "trends":                  {a["pair"]: self._price_trend(a["pair"]) for a in ASSETS},
            "markets_found_last_tick": sum(self._markets_found_last_tick.values()),
            "last_btc_market":         _market_summary("BTC"),
            "last_eth_market":         _market_summary("ETH"),
        }

    # ── Core tick ──────────────────────────────────────────────────────────────

    async def _tick(self) -> None:
        balance = await self._get_balance()
        if balance < MIN_TRADE_USD:
            logger.debug("BtcUpDown: balance $%.2f too low", balance)
            return

        # Refresh asset prices in parallel, then scan each asset's markets
        await asyncio.gather(
            *[self._refresh_price(a["pair"]) for a in ASSETS],
            return_exceptions=True,
        )

        for asset in ASSETS:
            markets = await self._fetch_updown_markets(asset["slug_kw"])
            self._markets_found_last_tick[asset["label"]] = len(markets)
            if not markets:
                logger.debug("BtcUpDown: no active markets for %s", asset["slug_kw"])
                continue

            self._last_market[asset["label"]] = {
                "question":  markets[0].get("question", ""),
                "yes_price": markets[0]["yes_price"],
                "no_price":  markets[0]["no_price"],
            }

            trend = self._price_trend(asset["pair"])
            logger.info(
                "BtcUpDown: %s — %d markets | trend=%s",
                asset["label"], len(markets), trend or "flat/unknown",
            )

            for market in markets:
                await self._evaluate(market, asset, trend, balance)

    # ── Market evaluation ──────────────────────────────────────────────────────

    async def _evaluate(
        self,
        market: dict,
        asset: dict,
        trend: str | None,
        balance: float,
    ) -> None:
        cid       = market["id"]
        yes       = market["yes_price"]
        no        = market["no_price"]
        price_sum = yes + no

        if price_sum < ARB_THRESHOLD:
            key = (cid, "arb")
            if key not in self._traded:
                self._traded.add(key)
                await self._fire_arb(market, asset, yes, no, price_sum, balance)

        if yes < CONTRARIAN_LOW and trend == "up":
            key = (cid, "contrarian_up")
            if key not in self._traded:
                self._traded.add(key)
                await self._fire_contrarian(market, asset, "yes", yes, trend, balance)

        if yes > CONTRARIAN_HIGH and trend == "down":
            key = (cid, "contrarian_down")
            if key not in self._traded:
                self._traded.add(key)
                await self._fire_contrarian(market, asset, "no", no, trend, balance)

    # ── Trade execution ────────────────────────────────────────────────────────

    async def _fire_arb(
        self,
        market: dict,
        asset: dict,
        yes: float,
        no: float,
        price_sum: float,
        balance: float,
    ) -> None:
        ok, reason = self._risk.can_trade()
        if not ok:
            logger.warning("BtcUpDown arb: risk block — %s", reason)
            return

        edge      = ARB_THRESHOLD - price_sum
        half_usd  = max(MIN_TRADE_USD / 2, balance * MAX_BALANCE_PCT / 2)
        yes_shares = max(1, int(half_usd / yes)) if yes > 0 else 1
        no_shares  = max(1, int(half_usd / no))  if no > 0  else 1
        cost_usd   = round(yes_shares * yes + no_shares * no, 2)

        logger.info(
            "BtcUpDown ARB: %s YES=%.3f NO=%.3f sum=%.3f edge=+%.3f cost=$%.2f | %s",
            asset["label"], yes, no, price_sum, edge, cost_usd,
            market.get("question", "")[:60],
        )

        reasoning = (
            f"{asset['label']} updown arb: YES={yes:.3f} + NO={no:.3f} = {price_sum:.3f} "
            f"< {ARB_THRESHOLD}. Edge: +{edge:.3f}."
        )
        self._ledger.add(
            event_type="UPDOWN_ARB",
            ticker=market["id"],
            market_title=market.get("question", market["id"])[:120],
            side="arb",
            count=yes_shares + no_shares,
            entry_price_cents=int(price_sum * 50),
            cost_usd=cost_usd,
            confidence=round(1.0 - price_sum, 3),
            reasoning=reasoning,
            headline=f"{asset['label']} updown arb: sum={price_sum:.3f} edge=+{edge:.3f}",
            strategy="btc_updown",
        )
        self._risk.record_trade(cost_usd)
        self._fired += 1

    async def _fire_contrarian(
        self,
        market: dict,
        asset: dict,
        side: str,
        price: float,
        trend: str,
        balance: float,
    ) -> None:
        ok, reason = self._risk.can_trade()
        if not ok:
            logger.warning("BtcUpDown contrarian: risk block — %s", reason)
            return

        trade_usd = max(MIN_TRADE_USD, balance * MAX_BALANCE_PCT)
        shares    = max(1, int(trade_usd / price)) if price > 0 else 1
        cost_usd  = round(shares * price, 2)

        yes       = market["yes_price"]
        direction = "bearish but rising" if side == "yes" else "bullish but falling"

        logger.info(
            "BtcUpDown CONTRARIAN: %s BUY %s @ %.3f | YES=%.3f (%s) | cost=$%.2f | %s",
            asset["label"], side.upper(), price, yes, direction, cost_usd,
            market.get("question", "")[:60],
        )

        reasoning = (
            f"{asset['label']} contrarian: market YES={yes:.3f} ({direction}) "
            f"but {asset['pair']} trend is {trend}. Buy {side.upper()} @ {price:.3f}."
        )
        self._ledger.add(
            event_type="UPDOWN_CONTRARIAN",
            ticker=market["id"],
            market_title=market.get("question", market["id"])[:120],
            side=side,
            count=shares,
            entry_price_cents=int(price * 100),
            cost_usd=cost_usd,
            confidence=round(abs(0.5 - yes), 3),
            reasoning=reasoning,
            headline=f"{asset['label']} contrarian {side.upper()} @ {price:.3f} | {direction}",
            strategy="btc_updown",
        )
        self._risk.record_trade(cost_usd)
        self._fired += 1

    # ── Gamma API ──────────────────────────────────────────────────────────────

    async def _fetch_updown_markets(self, slug_kw: str) -> list[dict]:
        # _q searches question text, not slug — fetch a large batch and filter locally
        params = {
            "active":    "true",
            "closed":    "false",
            "limit":     200,
            "order":     "startDate",
            "ascending": "false",
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{GAMMA_BASE}/markets", params=params)
                resp.raise_for_status()
                raw_list = resp.json()
        except Exception as exc:
            logger.warning("BtcUpDown: Gamma fetch failed for %s: %s", slug_kw, exc)
            return []

        kw = slug_kw.lower()
        result = []
        for raw in raw_list:
            if kw not in raw.get("slug", "").lower():
                continue
            m = _normalize(raw)
            if not m or not m["active"] or m["closed"]:
                continue
            if m["restricted"]:
                logger.debug("BtcUpDown: restricted market included: %s", m["slug"])
            result.append(m)
        logger.info("BtcUpDown: _fetch_updown_markets(%s) → %d active markets", slug_kw, len(result))
        return result

    # ── Price data ─────────────────────────────────────────────────────────────

    async def _refresh_price(self, pair: str) -> None:
        url = COINBASE_SPOT_URL.format(pair=pair)
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                price = float(resp.json()["data"]["amount"])
                self._price_history[pair].append((time.monotonic(), price))
                logger.debug("BtcUpDown: %s price=%.2f", pair, price)
        except Exception as exc:
            logger.warning("BtcUpDown: price fetch failed for %s: %s", pair, exc)

    def _price_trend(self, pair: str) -> str | None:
        """
        Return 'up', 'down', or None by comparing current price to ~5 minutes ago.
        Uses the deque entry whose age is closest to PRICE_LOOKBACK_S seconds.
        Returns None when there is insufficient history.
        """
        hist = self._price_history[pair]
        if len(hist) < 2:
            return None

        now              = time.monotonic()
        current_ts, cur  = hist[-1]

        # Find the reading closest to PRICE_LOOKBACK_S seconds old
        ref_price    = None
        best_age_gap = float("inf")
        for ts, price in hist:
            age = now - ts
            if age >= PRICE_LOOKBACK_S:
                gap = age - PRICE_LOOKBACK_S
                if gap < best_age_gap:
                    best_age_gap = gap
                    ref_price    = price

        if ref_price is None:
            # Not 5 minutes of history yet — fall back to oldest available if > 30s old
            oldest_ts, oldest_price = hist[0]
            if now - oldest_ts < 30:
                return None
            ref_price = oldest_price

        change = (cur - ref_price) / ref_price
        logger.debug(
            "BtcUpDown trend: %s ref=%.2f cur=%.2f chg=%.3f%%",
            pair, ref_price, cur, change * 100,
        )

        if change > MIN_TREND_MOVE:
            return "up"
        if change < -MIN_TREND_MOVE:
            return "down"
        return None

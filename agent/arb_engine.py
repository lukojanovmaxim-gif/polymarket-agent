"""
ArbEngine — buys both sides of BTC markets when YES + NO < $1.00.

On a binary Polymarket market exactly one outcome pays $1.00 per share at
resolution. If YES price + NO price < $1.00, buying N shares of each side
costs N × (YES + NO) and is guaranteed to return N × $1.00 — pure risk-free
profit equal to N × (1 − YES − NO).

Strategy:
  1. Scan the crypto pool every 30 s for BTC-tagged markets
  2. If price_sum < ARB_THRESHOLD and edge ≥ ARB_MIN_EDGE, enter the trade
  3. In paper mode: deduct cost from balance and record in ledger as side="arb"

Position sizing: 5 % of balance per arb pair, minimum $5.
"""
import asyncio
import logging

from agent.ledger import Ledger
from agent.market_scanner import MarketScanner
from agent.risk_manager import RiskManager

logger = logging.getLogger(__name__)

ARB_THRESHOLD = 0.99    # YES + NO must be strictly below this
ARB_MIN_EDGE  = 0.02    # minimum edge per share ($0.02)
MAX_BALANCE_PCT = 0.05
MIN_TRADE_USD   = 5.0
POLL_INTERVAL_S = 30

BTC_KEYWORDS = ("btc", "bitcoin")


class ArbEngine:
    def __init__(
        self,
        scanner: MarketScanner,
        ledger: Ledger,
        risk: RiskManager,
        paper_mode: bool,
        get_state,
        get_balance,
    ) -> None:
        self._scanner = scanner
        self._ledger = ledger
        self._risk = risk
        self._paper_mode = paper_mode
        self._get_state = get_state
        self._get_balance = get_balance
        self._traded: set[str] = set()
        self._fired: int = 0
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("ArbEngine started — polling every %ds", POLL_INTERVAL_S)
        while self._running:
            await asyncio.sleep(POLL_INTERVAL_S)
            if self._get_state() == "running":
                await self._tick()

    def stop(self) -> None:
        self._running = False

    def arb_status(self) -> dict:
        return {
            "running": self._running,
            "arb_trades_fired": self._fired,
            "markets_traded": list(self._traded),
        }

    async def _tick(self) -> None:
        crypto_markets = self._scanner.markets_for("crypto")
        candidates = [
            m for m in crypto_markets
            if any(kw in m.get("question", "").lower() for kw in BTC_KEYWORDS)
            and m.get("price_sum", 1.0) < ARB_THRESHOLD
            and (1.0 - m.get("price_sum", 1.0)) >= ARB_MIN_EDGE
        ]

        if not candidates:
            logger.debug("ArbEngine: no BTC arb candidates (crypto pool: %d markets)", len(crypto_markets))
            return

        logger.info("ArbEngine: %d BTC arb candidate(s)", len(candidates))
        for m in candidates:
            await self._try_arb(m)

    async def _try_arb(self, market: dict) -> None:
        cid = market.get("id", "")
        if not cid or cid in self._traded:
            return

        yes_price = market.get("yes_price", 0.5)
        no_price  = market.get("no_price", 0.5)
        price_sum = yes_price + no_price
        edge      = 1.0 - price_sum

        ok, reason = self._risk.can_trade()
        if not ok:
            logger.warning("ArbEngine: risk block — %s", reason)
            return

        balance   = await self._get_balance()
        max_spend = balance * MAX_BALANCE_PCT
        shares    = int(max_spend / price_sum)
        cost_usd  = round(shares * price_sum, 2)

        if shares < 1 or cost_usd < MIN_TRADE_USD:
            logger.info("ArbEngine: %s — position too small ($%.2f)", cid[:16], cost_usd)
            return

        expected_profit = round(shares * edge, 2)
        logger.info(
            "ArbEngine: FIRE %s | YES=%.3f NO=%.3f sum=%.3f edge=+%.3f | "
            "%d shares | cost $%.2f | expected profit $%.2f | %s",
            cid[:16], yes_price, no_price, price_sum, edge,
            shares, cost_usd, expected_profit,
            market.get("question", "")[:60],
        )

        self._traded.add(cid)
        self._risk.record_trade(cost_usd)

        self._ledger.add(
            event_type="POLYARB",
            ticker=cid,
            market_title=market.get("question", cid)[:120],
            side="arb",
            count=shares,
            entry_price_cents=int(price_sum * 100),
            cost_usd=cost_usd,
            confidence=round(edge, 3),
            reasoning=(
                f"YES={yes_price:.3f} + NO={no_price:.3f} = {price_sum:.3f} < $1.00. "
                f"Edge: +${edge:.3f}/share. Expected profit: ${expected_profit:.2f}"
            ),
            headline=f"BTC arb: YES={yes_price:.3f} NO={no_price:.3f} edge=+{edge:.3f}",
            strategy="arb",
        )
        self._fired += 1

"""
SportsArbEngine — mirrors Kalshi sports arb but for Polymarket game markets.

Strategy:
  The moment a game becomes FINAL with a confirmed winner:
    1. Find Polymarket markets for the winning team
    2. Buy YES if price < $0.98 — the market hasn't settled yet
    3. Also runs a periodic anomaly scan for extreme mispricings

Two entry triggers:
  - on_game_final() callback (wired to external sports feed)
  - Periodic anomaly scan: any sports market where one outcome is ≥ 97¢
    but the market is still open (clearly awaiting settlement)

Payout is $1.00/share. Any price below $0.98 is free money after the result is known.
"""
import asyncio
import logging
from dataclasses import dataclass

from agent.ledger import Ledger
from agent.market_scanner import MarketScanner
from agent.risk_manager import RiskManager

logger = logging.getLogger(__name__)

ARB_THRESHOLD   = 0.98
MIN_EDGE        = 0.02
MAX_BALANCE_PCT = 0.05
MIN_TRADE_USD   = 5.0
ANOMALY_POLL_S  = 60

FUTURES_PHRASES = (
    "stanley cup", "nba finals", "world series", "championship",
    "playoffs", "win the season", "advance to", "make the playoffs",
    "reach the finals", "conference finals", "semifinal", "quarterfinal",
    "sweep", "league title",
)


@dataclass
class GameResult:
    sport: str
    winner_name: str
    loser_name: str
    winner_score: int
    loser_score: int


class SportsArbEngine:
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
        logger.info("SportsArbEngine started — anomaly scan every %ds", ANOMALY_POLL_S)
        while self._running:
            await asyncio.sleep(ANOMALY_POLL_S)
            if self._get_state() == "running":
                await self._scan_anomalies()

    def stop(self) -> None:
        self._running = False

    def sports_status(self) -> dict:
        return {
            "running": self._running,
            "arb_trades_fired": self._fired,
            "markets_traded": list(self._traded),
        }

    # ── External callback ─────────────────────────────────────────────────────

    async def on_game_final(self, result: GameResult) -> None:
        """Called by an external sports monitor when a game result is confirmed."""
        if self._get_state() != "running":
            logger.info("SportsArb: skipping %s final — state %s", result.sport, self._get_state())
            return

        logger.info(
            "SportsArb: FINAL %s — %s %d-%d %s",
            result.sport, result.winner_name, result.winner_score,
            result.loser_score, result.loser_name,
        )

        all_markets = self._scanner.markets_for("sports") + self._scanner.markets_for("general")
        candidates = self._find_winner_markets(result, all_markets)

        if not candidates:
            logger.warning(
                "SportsArb: no Polymarket markets matched for %s winner=%s",
                result.sport, result.winner_name,
            )
            return

        logger.info("SportsArb: evaluating %d candidate(s) for %s", len(candidates), result.winner_name)
        await asyncio.gather(
            *[self._try_arb(m, result.sport, "yes") for m in candidates],
            return_exceptions=True,
        )

    # ── Anomaly scan ──────────────────────────────────────────────────────────

    async def _scan_anomalies(self) -> None:
        """Look for sports markets with extreme one-sided prices — likely post-game stale."""
        all_sports = self._scanner.markets_for("sports") + self._scanner.markets_for("general")
        game_kws = ("game", " win", "beat", " vs ", "match")

        for m in all_sports:
            q = m.get("question", "").lower()
            if not any(kw in q for kw in game_kws):
                continue
            if any(phrase in q for phrase in FUTURES_PHRASES):
                continue

            yes_price = m.get("yes_price", 0.5)
            no_price  = m.get("no_price", 0.5)

            if MIN_EDGE <= (1.0 - yes_price) < (1.0 - ARB_THRESHOLD) and yes_price >= 0.97:
                await self._try_arb(m, "SPORTS", "yes")
            elif MIN_EDGE <= (1.0 - no_price) < (1.0 - ARB_THRESHOLD) and no_price >= 0.97:
                await self._try_arb(m, "SPORTS", "no")

    # ── Market matching ───────────────────────────────────────────────────────

    def _find_winner_markets(self, result: GameResult, markets: list[dict]) -> list[dict]:
        winner_terms = {part for part in result.winner_name.lower().split() if len(part) > 2}
        sport_kw = result.sport.lower()

        matched = []
        for m in markets:
            if m.get("id", "") in self._traded:
                continue
            q = m.get("question", "").lower()
            if any(phrase in q for phrase in FUTURES_PHRASES):
                continue
            tags_str = str(m.get("tags", [])).lower()
            if sport_kw not in q and sport_kw not in tags_str:
                continue
            if not any(t in q for t in winner_terms):
                continue
            if not any(kw in q for kw in ("win", "beat", "advance", "champion")):
                continue
            yes_price = m.get("yes_price", 0.5)
            if yes_price < ARB_THRESHOLD:
                matched.append(m)
        return matched

    # ── Execution ─────────────────────────────────────────────────────────────

    async def _try_arb(self, market: dict, sport: str, side: str) -> None:
        cid = market.get("id", "")
        if not cid or cid in self._traded:
            return

        price = market.get("yes_price") if side == "yes" else market.get("no_price")
        if price is None or price >= ARB_THRESHOLD:
            return
        edge = 1.0 - price
        if edge < MIN_EDGE:
            return

        ok, reason = self._risk.can_trade()
        if not ok:
            logger.warning("SportsArb: risk block — %s", reason)
            return

        balance  = await self._get_balance()
        shares   = int((balance * MAX_BALANCE_PCT) / price)
        cost_usd = round(shares * price, 2)

        if shares < 1 or cost_usd < MIN_TRADE_USD:
            logger.info("SportsArb: %s — position too small ($%.2f)", cid[:16], cost_usd)
            return

        expected_profit = round(shares * edge, 2)
        logger.info(
            "SportsArb: FIRE %s %s @ $%.3f | %d shares | cost $%.2f | edge +$%.3f | profit $%.2f | %s",
            side.upper(), cid[:16], price, shares, cost_usd, edge, expected_profit,
            market.get("question", "")[:60],
        )

        self._traded.add(cid)
        self._risk.record_trade(cost_usd)

        self._ledger.add(
            event_type=sport,
            ticker=cid,
            market_title=market.get("question", cid)[:120],
            side=side,
            count=shares,
            entry_price_cents=int(price * 100),
            cost_usd=cost_usd,
            confidence=round(edge, 3),
            reasoning=f"{sport} arb: {side.upper()} @ ${price:.3f} | edge +${edge:.3f}/share",
            headline=f"{sport}: {side.upper()} @ ${price:.2f} | {market.get('question', '')[:60]}",
            strategy="sports",
        )
        self._fired += 1

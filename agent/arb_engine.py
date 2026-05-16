"""
CrossArbEngine — cross-market arbitrage between Polymarket and Kalshi.

Compares the same binary event across both exchanges. If one platform
prices YES low and the other prices NO low, buying both sides can cost
less than $1.00 combined — risk-free profit at resolution.

Case A: poly_yes_price + kalshi_no_price < ARB_THRESHOLD
  → buy YES on Polymarket + NO on Kalshi

Case B: kalshi_yes_price + poly_no_price < ARB_THRESHOLD
  → buy YES on Kalshi + NO on Polymarket

Matching: Jaccard keyword similarity on market questions (≥ SIMILARITY_MIN).
Each (poly_id, kalshi_ticker, direction) tuple is fired at most once per session.
"""
import asyncio
import json
import logging
import re

import httpx

from agent.config import DATA_DIR, KALSHI_AGENT_URL
from agent.ledger import Ledger
from agent.market_scanner import MarketScanner
from agent.reporter import Reporter
from agent.risk_manager import RiskManager

logger = logging.getLogger(__name__)

ARB_THRESHOLD   = 0.95   # combined cost must be below this
SIMILARITY_MIN  = 0.55   # minimum Jaccard similarity — high enough to ensure same event
MAX_BALANCE_PCT = 0.03   # 3% of balance per cross-arb trade
MIN_TRADE_USD   = 5.0
POLL_INTERVAL_S = 60

FIRED_SET_FILE = DATA_DIR / "fired_arb.json"

_STOP_WORDS = frozenset({
    "a", "an", "the", "to", "of", "in", "is", "will", "be", "by",
    "at", "on", "or", "and", "for", "with", "before", "end", "this",
    "that", "it", "its", "which", "than", "from", "as", "are",
    "was", "were", "been", "has", "have", "had",
})


def _keywords(text: str) -> frozenset[str]:
    words = re.findall(r"[a-z0-9]+", text.lower())
    return frozenset(w for w in words if w not in _STOP_WORDS and len(w) > 1)


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


class CrossArbEngine:
    def __init__(
        self,
        scanner: MarketScanner,
        ledger: Ledger,
        risk: RiskManager,
        reporter: Reporter,
        paper_mode: bool,
        get_state,
        get_balance,
    ) -> None:
        self._scanner = scanner
        self._ledger = ledger
        self._risk = risk
        self._reporter = reporter
        self._paper_mode = paper_mode
        self._get_state = get_state
        self._get_balance = get_balance
        self._fired_set: set[tuple[str, str, str]] = self._load_fired_set()
        self._fired: int = len(self._fired_set)
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("CrossArbEngine started — polling every %ds", POLL_INTERVAL_S)
        while self._running:
            await asyncio.sleep(POLL_INTERVAL_S)
            if self._get_state() == "running":
                await self._tick()

    def stop(self) -> None:
        self._running = False

    def arb_status(self) -> dict:
        traded = [f"{p[:12]}/{k}" for p, k, _ in list(self._fired_set)[:10]]
        return {
            "running": self._running,
            "arb_trades_fired": self._fired,
            "markets_traded": traded,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load_fired_set(self) -> set[tuple[str, str, str]]:
        try:
            with open(FIRED_SET_FILE) as f:
                return {tuple(item) for item in json.load(f)}
        except Exception:
            return set()

    def _save_fired_set(self) -> None:
        try:
            DATA_DIR.mkdir(exist_ok=True)
            with open(FIRED_SET_FILE, "w") as f:
                json.dump([list(item) for item in self._fired_set], f)
        except Exception as exc:
            logger.warning("CrossArbEngine: could not save fired set: %s", exc)

    async def _tick(self) -> None:
        if not KALSHI_AGENT_URL:
            logger.debug("CrossArbEngine: KALSHI_AGENT_URL not set — skipping tick")
            return

        balance = await self._get_balance()
        if balance < MIN_TRADE_USD:
            logger.info("CrossArbEngine: balance $%.2f below minimum — skipping tick", balance)
            return

        poly_markets = self._scanner.markets
        if not poly_markets:
            logger.debug("CrossArbEngine: Polymarket pool is empty — skipping tick")
            return

        kalshi_markets = await self._fetch_kalshi()
        if not kalshi_markets:
            return

        logger.debug(
            "CrossArbEngine: comparing %d poly × %d kalshi markets",
            len(poly_markets), len(kalshi_markets),
        )

        poly_kw   = [(m, _keywords(m.get("question", ""))) for m in poly_markets]
        kalshi_kw = [(m, _keywords(m.get("title", "")))    for m in kalshi_markets]

        for p_mkt, p_kw in poly_kw:
            if not p_kw:
                continue
            poly_id  = p_mkt.get("id", "")
            poly_yes = p_mkt.get("yes_price", 0.5)
            poly_no  = p_mkt.get("no_price", 0.5)

            for k_mkt, k_kw in kalshi_kw:
                if not k_kw:
                    continue
                sim = _jaccard(p_kw, k_kw)
                if sim < SIMILARITY_MIN:
                    continue

                kalshi_ticker = k_mkt.get("ticker", "")
                kalshi_yes    = k_mkt.get("yes_price", 0.5)
                kalshi_no     = k_mkt.get("no_price", 0.5)

                # Case A: Poly YES + Kalshi NO
                combined_a = poly_yes + kalshi_no
                if combined_a < ARB_THRESHOLD:
                    key = (poly_id, kalshi_ticker, "A")
                    if key not in self._fired_set:
                        self._fired_set.add(key)
                        self._save_fired_set()
                        await self._fire(
                            direction="A", poly_mkt=p_mkt, kalshi_mkt=k_mkt,
                            poly_price=poly_yes, kalshi_price=kalshi_no,
                            combined=combined_a, edge=ARB_THRESHOLD - combined_a, sim=sim,
                        )

                # Case B: Kalshi YES + Poly NO
                combined_b = kalshi_yes + poly_no
                if combined_b < ARB_THRESHOLD:
                    key = (poly_id, kalshi_ticker, "B")
                    if key not in self._fired_set:
                        self._fired_set.add(key)
                        self._save_fired_set()
                        await self._fire(
                            direction="B", poly_mkt=p_mkt, kalshi_mkt=k_mkt,
                            poly_price=poly_no, kalshi_price=kalshi_yes,
                            combined=combined_b, edge=ARB_THRESHOLD - combined_b, sim=sim,
                        )

    async def _fire(
        self,
        direction: str,
        poly_mkt: dict,
        kalshi_mkt: dict,
        poly_price: float,
        kalshi_price: float,
        combined: float,
        edge: float,
        sim: float,
    ) -> None:
        poly_id       = poly_mkt.get("id", "")
        kalshi_ticker = kalshi_mkt.get("ticker", "")
        poly_q        = poly_mkt.get("question", "")
        kalshi_q      = kalshi_mkt.get("title", "")

        if direction == "A":
            action     = "BUY Poly YES + Kalshi NO"
            poly_side  = "yes"
            kalshi_side = "no"
        else:
            action     = "BUY Kalshi YES + Poly NO"
            poly_side  = "no"
            kalshi_side = "yes"

        ok, reason = self._risk.can_trade()
        if not ok:
            logger.warning("CrossArbEngine: risk block — %s", reason)
            return

        balance   = await self._get_balance()
        max_spend = balance * MAX_BALANCE_PCT
        shares    = max(1, int(max_spend / combined))
        cost_usd  = round(shares * combined, 2)

        if cost_usd < MIN_TRADE_USD:
            shares   = max(1, int(MIN_TRADE_USD / combined))
            cost_usd = round(shares * combined, 2)

        logger.info(
            "CrossArbEngine: FIRE dir=%s | poly='%s' | kalshi='%s' | "
            "poly_price=%.3f kalshi_price=%.3f combined=%.3f edge=+%.3f sim=%.0f%%",
            direction, poly_q[:50], kalshi_q[:50],
            poly_price, kalshi_price, combined, edge, sim * 100,
        )

        reasoning = (
            f"{action}: Poly {poly_side}={poly_price:.3f} + "
            f"Kalshi {kalshi_side}={kalshi_price:.3f} = {combined:.3f} < {ARB_THRESHOLD}. "
            f"Edge: +{edge:.3f}. Similarity: {sim:.0%}."
        )
        self._ledger.add(
            event_type="CROSSARB",
            ticker=f"{poly_id[:16]}/{kalshi_ticker}",
            market_title=f"[Poly] {poly_q[:60]} / [Kalshi] {kalshi_q[:60]}",
            side=poly_side,
            count=shares,
            entry_price_cents=int(combined * 100),
            cost_usd=cost_usd,
            confidence=round(1.0 - combined, 3),
            reasoning=reasoning,
            headline=f"Cross-arb {direction}: combined={combined:.3f} edge=+{edge:.3f}",
            strategy="cross_arb",
        )
        self._risk.record_trade(cost_usd)
        self._fired += 1

        await self._reporter._post("cross_arb", {
            "direction": direction,
            "action": action,
            "poly_question": poly_q,
            "kalshi_question": kalshi_q,
            "poly_price": round(poly_price, 3),
            "kalshi_price": round(kalshi_price, 3),
            "combined_cost": round(combined, 3),
            "edge": round(edge, 3),
            "similarity": round(sim, 2),
            "poly_id": poly_id,
            "kalshi_ticker": kalshi_ticker,
        })

    async def _fetch_kalshi(self) -> list[dict]:
        """Fetch Kalshi markets from the Kalshi agent and normalize prices."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{KALSHI_AGENT_URL.rstrip('/')}/markets/sample",
                    params={"n": 500},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            logger.warning("CrossArbEngine: Kalshi fetch failed: %s", exc)
            return []

        raw = data if isinstance(data, list) else data.get("markets", [])
        result: list[dict] = []
        for m in raw:
            try:
                yes_ask = m.get("yes_ask") or 50
                yes_bid = m.get("yes_bid") or 50
                result.append({
                    "ticker":    m.get("ticker", ""),
                    "title":     m.get("title", ""),
                    "yes_price": yes_ask / 100,
                    "no_price":  (100 - yes_bid) / 100,
                })
            except Exception:
                continue
        return result

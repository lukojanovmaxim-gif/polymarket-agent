"""
Persistent trade journal with outcome tracking and P&L analytics.

Stores every trade in DATA_DIR/trades.json.
After a market resolves, call resolve() to record won/lost and P&L.

Note on prices: entry_price_cents stores the price in integer cents (0-100),
matching Polymarket's 0.0-1.0 price range × 100. count is shares.
"""
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

from agent.config import DATA_DIR

logger = logging.getLogger(__name__)

TRADES_FILE = DATA_DIR / "trades.json"


@dataclass
class TradeRecord:
    id: str
    event_type: str         # POLYARB | NHL | NBA | MLB | SPORTS | ...
    ticker: str             # Polymarket condition_id
    market_title: str
    side: str               # yes | no | arb
    count: int              # shares
    entry_price_cents: int  # price × 100 (0-100)
    cost_usd: float
    signal_confidence: float
    signal_reasoning: str
    news_headline: str
    timestamp: str          # ISO UTC
    strategy: str = "arb"  # arb | sports
    outcome: str = "open"  # open | won | lost | exited_early
    exit_price_cents: int = 0
    pnl_usd: float = 0.0
    resolved_at: str = ""


class Ledger:
    def __init__(self) -> None:
        DATA_DIR.mkdir(exist_ok=True)
        self._trades: list[TradeRecord] = []
        self._load()

    # ── Write ─────────────────────────────────────────────────────────────────

    def add(
        self,
        event_type: str,
        ticker: str,
        market_title: str,
        side: str,
        count: int,
        entry_price_cents: int,
        cost_usd: float,
        confidence: float,
        reasoning: str,
        headline: str,
        strategy: str = "arb",
    ) -> TradeRecord:
        rec = TradeRecord(
            id=uuid.uuid4().hex[:8].upper(),
            event_type=event_type,
            ticker=ticker,
            market_title=market_title,
            side=side,
            count=count,
            entry_price_cents=entry_price_cents,
            cost_usd=cost_usd,
            signal_confidence=confidence,
            signal_reasoning=reasoning,
            news_headline=headline,
            timestamp=datetime.now(timezone.utc).isoformat(),
            strategy=strategy,
        )
        self._trades.append(rec)
        self._save()
        return rec

    def resolve(self, trade_id: str, won: bool, exit_price_cents: int = 0) -> "TradeRecord | None":
        for t in self._trades:
            if t.id == trade_id and t.outcome == "open":
                payout = t.count if won else 0.0  # each share pays $1
                t.outcome = "won" if won else "lost"
                t.exit_price_cents = exit_price_cents if exit_price_cents else (99 if won else 1)
                t.pnl_usd = round(payout - t.cost_usd, 2)
                t.resolved_at = datetime.now(timezone.utc).isoformat()
                self._save()
                logger.info("Trade %s resolved: %s | P&L $%.2f", trade_id, t.outcome, t.pnl_usd)
                return t
        return None

    # ── Read ──────────────────────────────────────────────────────────────────

    def open_trades(self) -> list[TradeRecord]:
        return [t for t in self._trades if t.outcome == "open"]

    def today_trades(self) -> list[TradeRecord]:
        today = date.today().isoformat()
        return [t for t in self._trades if t.timestamp[:10] == today]

    def daily_summary(self) -> dict[str, Any]:
        today = self.today_trades()
        closed = [t for t in today if t.outcome in ("won", "lost")]
        wins   = [t for t in closed if t.outcome == "won"]
        losses = [t for t in closed if t.outcome == "lost"]
        pnl    = sum(t.pnl_usd for t in closed)
        return {
            "total_today": len(today),
            "closed": len(closed),
            "open": len(today) - len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "pnl": pnl,
            "trades": today,
        }

    def analytics(self) -> dict[str, Any]:
        closed = [t for t in self._trades if t.outcome in ("won", "lost")]
        if not closed:
            return {}

        by_type: dict[str, dict] = {}
        for t in closed:
            et = t.event_type
            s = by_type.setdefault(et, {"wins": 0, "losses": 0, "pnl": 0.0, "total_conf": 0.0, "n": 0})
            s["n"] += 1
            s["pnl"] += t.pnl_usd
            s["total_conf"] += t.signal_confidence
            if t.outcome == "won":
                s["wins"] += 1
            else:
                s["losses"] += 1

        results: dict[str, Any] = {}
        for et, s in by_type.items():
            wr = s["wins"] / s["n"] if s["n"] else 0
            avg_conf = s["total_conf"] / s["n"] if s["n"] else 0
            results[et] = {
                "trades": s["n"],
                "win_rate": wr,
                "win_rate_pct": f"{wr:.1%}",
                "pnl": s["pnl"],
                "pnl_str": f"${s['pnl']:+.2f}",
                "avg_confidence": f"{avg_conf:.0%}",
            }

        all_pnl  = sum(t.pnl_usd for t in closed)
        all_wins = sum(1 for t in closed if t.outcome == "won")
        return {
            "by_event": results,
            "total_closed": len(closed),
            "overall_win_rate": f"{all_wins / len(closed):.1%}",
            "total_pnl": f"${all_pnl:+.2f}",
        }

    def strategy_analytics(self) -> dict[str, Any]:
        closed = [t for t in self._trades if t.outcome in ("won", "lost")]

        by_strategy: dict[str, Any] = {}
        for s in ("arb", "sports"):
            bucket = [t for t in closed if getattr(t, "strategy", "arb") == s]
            wins   = sum(1 for t in bucket if t.outcome == "won")
            pnl    = sum(t.pnl_usd for t in bucket)
            if bucket:
                by_strategy[s] = {
                    "trades": len(bucket),
                    "wins": wins,
                    "losses": len(bucket) - wins,
                    "pnl": round(pnl, 2),
                    "pnl_str": f"${pnl:+.2f}",
                    "win_rate_pct": f"{wins / len(bucket):.0%}",
                }
            else:
                by_strategy[s] = {
                    "trades": 0, "wins": 0, "losses": 0,
                    "pnl": 0.0, "pnl_str": "$0", "win_rate_pct": None,
                }

        all_pnl  = sum(t.pnl_usd for t in closed)
        all_wins = sum(1 for t in closed if t.outcome == "won")
        return {
            "by_strategy": by_strategy,
            "overall": {
                "total_closed": len(closed),
                "total_pnl": f"${all_pnl:+.2f}",
                "overall_win_rate": f"{all_wins / len(closed):.0%}" if closed else None,
            },
        }

    def paper_balance(self, initial: float) -> float:
        """Reconstruct running paper cash from ledger — survives restarts."""
        realized = sum(t.pnl_usd for t in self._trades if t.outcome in ("won", "lost"))
        locked   = sum(t.cost_usd for t in self._trades if t.outcome == "open")
        return round(initial + realized - locked, 2)

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not TRADES_FILE.exists():
            return
        try:
            with open(TRADES_FILE) as f:
                rows = json.load(f)
            self._trades = [TradeRecord(**r) for r in rows]
            logger.info("Ledger loaded: %d records (%d open)", len(self._trades), len(self.open_trades()))
        except Exception as exc:
            logger.warning("Could not load ledger: %s", exc)

    def _save(self) -> None:
        try:
            with open(TRADES_FILE, "w") as f:
                json.dump([asdict(t) for t in self._trades], f, indent=2, default=str)
        except Exception as exc:
            logger.error("Could not save ledger: %s", exc)

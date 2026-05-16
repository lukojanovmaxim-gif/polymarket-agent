"""
PolyTradingCore — state machine and top-level orchestrator.

States:  STOPPED → RUNNING ⇄ PAUSED → STOPPED
                    ↓ (15% loss)
                  HALTED

Paper mode only. Telegram/Jarvis controls state via POST /commands.
"""
import asyncio
import logging
import os
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

from agent.arb_engine import ArbEngine
from agent.config import PAPER_BALANCE_INITIAL
from agent.ledger import Ledger
from agent.market_scanner import MarketScanner, WhaleAlert
from agent.polymarket_client import PolymarketClient
from agent.reporter import Reporter
from agent.risk_manager import RiskManager
from agent.sports_arb import SportsArbEngine

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class AgentState(str, Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED  = "paused"
    HALTED  = "halted"


class PolyTradingCore:
    def __init__(self, reporter: Reporter) -> None:
        self._reporter = reporter

        self._client  = PolymarketClient()
        self._ledger  = Ledger()
        self._risk    = RiskManager(paper_mode=True)

        self._paper_balance = self._ledger.paper_balance(PAPER_BALANCE_INITIAL)
        self.state: AgentState = AgentState.STOPPED
        self._tasks: list[asyncio.Task] = []

        self._risk.set_halt_callback(self._on_halt)

        self._scanner = MarketScanner(self._client, on_whale=self._on_whale)

        self._arb = ArbEngine(
            scanner=self._scanner,
            ledger=self._ledger,
            risk=self._risk,
            paper_mode=True,
            get_state=lambda: self.state,
            get_balance=self._get_balance,
        )
        self._sports_arb = SportsArbEngine(
            scanner=self._scanner,
            ledger=self._ledger,
            risk=self._risk,
            paper_mode=True,
            get_state=lambda: self.state,
            get_balance=self._get_balance,
        )

    async def _get_balance(self) -> float:
        return self._paper_balance

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def launch(self) -> None:
        self._risk.start_day(self._paper_balance)

        self._tasks = [
            self._make_task(self._scanner.start(),      "market-scanner"),
            self._make_task(self._arb.start(),          "arb-engine"),
            self._make_task(self._sports_arb.start(),   "sports-arb"),
            asyncio.create_task(
                self._reporter.run_daily_scheduler(self._build_summary),
                name="daily-reporter",
            ),
        ]
        self.state = AgentState.RUNNING

        await self._reporter.startup(
            state=self.state,
            balance=self._paper_balance,
            paper_mode=True,
        )
        logger.info("PolyTradingCore running | paper=$%.2f", self._paper_balance)

    async def shutdown(self) -> None:
        self.state = AgentState.STOPPED
        self._scanner.stop()
        self._arb.stop()
        self._sports_arb.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._client.close()

    @staticmethod
    def _make_task(coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                logger.error("Task '%s' crashed: %s", name, exc, exc_info=exc)
        task.add_done_callback(_on_done)
        return task

    # ── Commands ──────────────────────────────────────────────────────────────

    async def cmd_start(self) -> str:
        if self.state == AgentState.RUNNING:
            return "Already running."
        self.state = AgentState.RUNNING
        logger.info("Command: start")
        return "Trading resumed."

    async def cmd_pause(self) -> str:
        if self.state != AgentState.RUNNING:
            return f"Cannot pause — state: {self.state}"
        self.state = AgentState.PAUSED
        logger.info("Command: pause")
        return "Trading paused. Monitoring continues."

    async def cmd_resume(self) -> str:
        if self.state != AgentState.PAUSED:
            return f"Cannot resume — state: {self.state}"
        self.state = AgentState.RUNNING
        logger.info("Command: resume")
        return "Trading resumed."

    async def cmd_stop(self) -> str:
        self.state = AgentState.STOPPED
        logger.info("Command: stop")
        return "Trading stopped."

    async def cmd_set_risk(self, max_loss_pct: float) -> str:
        self._risk.update_loss_limit(max_loss_pct)
        return f"Daily loss limit updated to {max_loss_pct:.0%}."

    # ── Status / data ─────────────────────────────────────────────────────────

    def status(self) -> dict:
        result = {
            "state": self.state,
            "paper_mode": True,
            "balance": round(self._paper_balance, 2),
            "daily_pnl": round(self._risk.net_pnl, 2),
            "daily_loss_pct": round(self._risk.daily_loss_pct * 100, 1),
            "daily_limit_pct": round(self._risk.max_loss_pct * 100, 1),
            "trades_today": self._risk.trade_count,
            "open_positions": len(self._ledger.open_trades()),
            "pool_sizes": self._scanner.pool_sizes(),
        }
        if self._risk.halt_reason:
            result["halt_reason"] = self._risk.halt_reason
        return result

    def trades(self, status: str = "open") -> dict:
        trades_list = self._ledger._trades if status == "all" else self._ledger.open_trades()
        return {
            "count": len(trades_list),
            "trades": [
                {
                    "id": t.id,
                    "strategy": getattr(t, "strategy", "arb"),
                    "market_id": t.ticker,
                    "market": t.market_title,
                    "side": t.side,
                    "entry_price": f"${t.entry_price_cents / 100:.3f}",
                    "shares": t.count,
                    "cost": f"${t.cost_usd:.2f}",
                    "confidence": f"{t.signal_confidence:.1%}",
                    "reasoning": t.signal_reasoning,
                    "opened": t.timestamp,
                    "outcome": t.outcome,
                    "pnl": f"${t.pnl_usd:+.2f}" if t.outcome != "open" else None,
                }
                for t in trades_list
            ],
        }

    def stats(self) -> dict:
        return self._ledger.strategy_analytics()

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _on_whale(self, alert: WhaleAlert) -> None:
        logger.info(
            "WHALE: %s %+.3f → %.3f | %s",
            alert.id[:16], alert.delta, alert.current_yes, alert.question[:60],
        )

    async def _on_halt(self, reason: str, loss_pct: float) -> None:
        self.state = AgentState.HALTED
        await self._reporter.halt_alert(reason, loss_pct)

    def _build_summary(self) -> dict:
        daily     = self._ledger.daily_summary()
        analytics = self._ledger.analytics()
        status    = self.status()
        return {
            "date": datetime.now(ET).strftime("%Y-%m-%d"),
            "paper_mode": True,
            **daily,
            "analytics": analytics,
            "agent_status": status,
        }

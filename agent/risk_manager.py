"""
Daily risk controls — 15 % daily loss limit with auto-halt.
State persisted to DATA_DIR/daily_state.json so restarts don't reset counters.
"""
import json
import logging
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from zoneinfo import ZoneInfo

from agent.config import DATA_DIR

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
STATE_FILE = DATA_DIR / "daily_state.json"
DEFAULT_LOSS_LIMIT_PCT = 0.15


@dataclass
class DailyState:
    date_et: str = ""
    starting_balance: float = 0.0
    deployed: float = 0.0
    returned: float = 0.0
    realized_losses: float = 0.0
    trade_count: int = 0
    halted: bool = False
    halt_reason: str = ""
    max_loss_pct: float = DEFAULT_LOSS_LIMIT_PCT


class RiskManager:
    def __init__(self, loss_limit_pct: float = DEFAULT_LOSS_LIMIT_PCT, paper_mode: bool = False) -> None:
        self._paper_mode = paper_mode
        self._state = DailyState(max_loss_pct=loss_limit_pct)
        DATA_DIR.mkdir(exist_ok=True)
        self._load()
        self._on_halt = None

    def set_halt_callback(self, cb) -> None:
        self._on_halt = cb

    # ── Day init ──────────────────────────────────────────────────────────────

    def start_day(self, balance: float) -> None:
        today = datetime.now(ET).date().isoformat()
        if self._state.date_et != today:
            self._state = DailyState(
                date_et=today,
                starting_balance=balance,
                max_loss_pct=self._state.max_loss_pct,
            )
            self._save()
            logger.info("Daily risk reset — balance $%.2f | limit %.0f%%", balance, self._state.max_loss_pct * 100)

    # ── Gate ──────────────────────────────────────────────────────────────────

    @property
    def is_halted(self) -> bool:
        self._maybe_reset()
        return self._state.halted

    def can_trade(self) -> tuple[bool, str]:
        self._maybe_reset()
        if self._state.halted:
            return False, self._state.halt_reason
        if self._state.starting_balance > 0:
            pct = self._state.realized_losses / self._state.starting_balance
            if pct >= self._state.max_loss_pct:
                reason = f"Daily loss limit reached ({pct:.1%} of balance)"
                self._halt(reason)
                return False, reason
        return True, ""

    # ── Record ────────────────────────────────────────────────────────────────

    def record_trade(self, cost_usd: float) -> None:
        self._state.deployed += cost_usd
        self._state.trade_count += 1
        self._save()

    def record_resolution(self, payout_usd: float, won: bool, cost_usd: float = 0.0) -> None:
        self._state.returned += payout_usd
        if not won and cost_usd > 0:
            self._state.realized_losses += cost_usd
            self._save()
            if not self._paper_mode and not self._state.halted and self._state.starting_balance > 0:
                pct = self._state.realized_losses / self._state.starting_balance
                if pct >= self._state.max_loss_pct:
                    self._halt(f"Daily loss limit reached ({pct:.1%} of balance)")
        else:
            self._save()

    # ── Settings ──────────────────────────────────────────────────────────────

    def update_loss_limit(self, pct: float) -> None:
        self._state.max_loss_pct = max(0.01, min(pct, 0.50))
        self._save()
        logger.info("Daily loss limit updated to %.0f%%", self._state.max_loss_pct * 100)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def starting_balance(self) -> float:
        return self._state.starting_balance

    @property
    def net_pnl(self) -> float:
        return self._state.returned - self._state.deployed

    @property
    def daily_loss_pct(self) -> float:
        if self._state.starting_balance <= 0:
            return 0.0
        return self._state.realized_losses / self._state.starting_balance

    @property
    def trade_count(self) -> int:
        return self._state.trade_count

    @property
    def max_loss_pct(self) -> float:
        return self._state.max_loss_pct

    @property
    def halt_reason(self) -> str:
        return self._state.halt_reason

    # ── Internal ─────────────────────────────────────────────────────────────

    def _halt(self, reason: str) -> None:
        logger.warning("TRADING HALTED: %s", reason)
        self._state.halted = True
        self._state.halt_reason = reason
        self._save()
        if self._on_halt:
            import asyncio
            asyncio.create_task(self._on_halt(reason, self.daily_loss_pct))

    def _maybe_reset(self) -> None:
        today = datetime.now(ET).date().isoformat()
        if self._state.date_et and self._state.date_et != today:
            prev_balance = max(0.0, self._state.starting_balance + self.net_pnl)
            self._state = DailyState(
                date_et=today,
                starting_balance=prev_balance,
                max_loss_pct=self._state.max_loss_pct,
            )
            self._save()

    def _load(self) -> None:
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    raw = json.load(f)
                known = {f.name for f in fields(DailyState)}
                self._state = DailyState(**{k: v for k, v in raw.items() if k in known})
                self._maybe_reset()
                if self._state.halted and self._state.starting_balance > 0:
                    pct = self._state.realized_losses / self._state.starting_balance
                    if pct < self._state.max_loss_pct:
                        logger.info("Clearing stale halt — realized losses %.1f%% < limit %.0f%%", pct * 100, self._state.max_loss_pct * 100)
                        self._state.halted = False
                        self._state.halt_reason = ""
                        self._save()
            except Exception as exc:
                logger.warning("Could not load risk state: %s", exc)

    def _save(self) -> None:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(asdict(self._state), f, indent=2)
        except Exception as exc:
            logger.error("Could not save risk state: %s", exc)

"""
Reporter — forwards events to Jarvis via HTTP callback.

Events sent:
  startup        — agent came online
  daily_summary  — sent at 22:00 Berlin, includes P&L + analytics
  halt_alert     — 15% daily loss limit hit
  critical       — unrecoverable error

Set JARVIS_CALLBACK_URL to receive events. Leave unset to run silently.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)
BERLIN = ZoneInfo("Europe/Berlin")


def _seconds_until_10pm_berlin() -> float:
    now = datetime.now(BERLIN)
    target = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


class Reporter:
    def __init__(self, url: str) -> None:
        self._url = url
        self._enabled = bool(url)
        if self._enabled:
            logger.info("Reporter → %s", url)
        else:
            logger.warning("Reporter: JARVIS_CALLBACK_URL not set — events will not be forwarded")

    async def startup(self, state: str, balance: float, paper_mode: bool) -> None:
        await self._post("startup", {"state": state, "balance": balance, "paper_mode": paper_mode})

    async def daily_summary(self, summary: dict) -> None:
        await self._post("daily_summary", summary)

    async def halt_alert(self, reason: str, daily_loss_pct: float) -> None:
        await self._post("halt_alert", {"reason": reason, "daily_loss_pct": daily_loss_pct})

    async def critical(self, message: str) -> None:
        await self._post("critical", {"message": message})

    async def run_daily_scheduler(self, get_summary_fn) -> None:
        """Fires daily_summary at 22:00 Berlin every day."""
        while True:
            wait = _seconds_until_10pm_berlin()
            logger.info("Daily summary scheduled in %.0fh %.0fm", wait // 3600, (wait % 3600) // 60)
            await asyncio.sleep(wait)
            try:
                summary = get_summary_fn()
                await self.daily_summary(summary)
            except Exception as exc:
                logger.error("Daily summary send failed: %s", exc)

    async def _post(self, event_type: str, data: dict) -> None:
        if not self._enabled:
            logger.info("Reporter disabled — event: %s", event_type)
            return
        payload = {"type": event_type, "data": data, "source": "polymarket-agent"}
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(self._url, json=payload)
                resp.raise_for_status()
        except Exception as exc:
            logger.error("Reporter: failed [%s] at %s: %s", event_type, self._url, exc)

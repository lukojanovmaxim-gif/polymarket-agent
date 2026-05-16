import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from agent.core import PolyTradingCore
from agent.reporter import Reporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    reporter = Reporter(url=os.environ.get("JARVIS_CALLBACK_URL", ""))
    core = PolyTradingCore(reporter=reporter)
    app.state.core = core

    await core.launch()
    logger.info("Polymarket agent launched")

    yield

    await core.shutdown()
    logger.info("Polymarket agent shut down")


app = FastAPI(title="Polymarket Trading Agent", lifespan=lifespan)


def _core(request: Request) -> PolyTradingCore:
    core = getattr(request.app.state, "core", None)
    if core is None:
        raise HTTPException(status_code=503, detail="Agent not initialised")
    return core


# ── Models ────────────────────────────────────────────────────────────────────

class CommandRequest(BaseModel):
    action: str
    params: dict = {}

    @field_validator("action")
    @classmethod
    def valid_action(cls, v: str) -> str:
        allowed = {"start", "stop", "pause", "resume", "set_risk"}
        if v not in allowed:
            raise ValueError(f"Unknown action '{v}'. Allowed: {allowed}")
        return v


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/commands")
async def handle_command(body: CommandRequest, request: Request):
    core   = _core(request)
    action = body.action
    params = body.params

    if action == "start":
        msg = await core.cmd_start()
    elif action == "stop":
        msg = await core.cmd_stop()
    elif action == "pause":
        msg = await core.cmd_pause()
    elif action == "resume":
        msg = await core.cmd_resume()
    elif action == "set_risk":
        pct = params.get("max_loss_pct")
        if pct is None:
            raise HTTPException(400, "set_risk requires params.max_loss_pct (0.01-0.50)")
        msg = await core.cmd_set_risk(float(pct))
    else:
        raise HTTPException(400, f"Unknown action: {action}")

    return {"ok": True, "message": msg, "state": core.state}


@app.get("/status")
async def get_status(request: Request):
    return _core(request).status()


@app.get("/trades")
async def get_trades(request: Request, status: str = "open"):
    return _core(request).trades(status=status)


@app.get("/stats")
async def get_stats(request: Request):
    return _core(request).stats()


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/markets")
async def get_markets(request: Request, n: int = 30, search: str = "", category: str = ""):
    """Return cached markets. ?category=crypto|sports|general, ?search=btc"""
    scanner = _core(request)._scanner
    markets = scanner.markets_for(category) if category else scanner.markets
    if search:
        s = search.lower()
        markets = [m for m in markets if s in m.get("question", "").lower()]
    return {
        "pool_sizes": scanner.pool_sizes(),
        "total_in_cache": len(scanner.markets),
        "returned": min(n, len(markets)),
        "markets": [
            {
                "id": m.get("id", ""),
                "question": m.get("question", ""),
                "yes_price": m.get("yes_price"),
                "no_price": m.get("no_price"),
                "price_sum": m.get("price_sum"),
                "arb_edge": round(1.0 - m["price_sum"], 4) if m.get("price_sum", 1.0) < 1.0 else 0,
                "volume": m.get("volume"),
                "liquidity": m.get("liquidity"),
                "end_date": m.get("end_date"),
                "tags": m.get("tags"),
            }
            for m in markets[:n]
        ],
    }


@app.get("/arb")
async def get_arb_status(request: Request):
    return _core(request)._arb.arb_status()


@app.get("/sports")
async def get_sports_status(request: Request):
    return _core(request)._sports_arb.sports_status()


@app.get("/btc")
async def get_btc_status(request: Request):
    return _core(request)._btc.btc_status()


@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.error("Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"error": str(exc)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, log_level="info")

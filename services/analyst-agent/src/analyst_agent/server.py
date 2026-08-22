"""Thin FastAPI wrapper: POST /ask.

Deliberately thin. All the logic is in graph.py; this exists so the agent can be
demoed with curl and hit from a dashboard later. Because `synthesize` returns a
Pydantic `AnalystAnswer`, this layer never parses prose — it serializes a typed
object straight to JSON.
"""

from __future__ import annotations

import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from analyst_agent.graph import run_agent
from analyst_agent.schemas import AskResponse
from analyst_agent.settings import settings
from analyst_agent.tools import mcp_tools

log = structlog.get_logger()

app = FastAPI(title="analyst-agent", version="0.1.0")


class AskRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict:
    """Ready only if the MCP server is reachable and exposing tools."""
    try:
        async with mcp_tools() as tools:
            return {"status": "ready", "tools": [t.name for t in tools]}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"mcp-server unreachable: {exc}") from exc


@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest) -> AskResponse:
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY is not set; the analyst agent requires it.",
        )

    log.info("question received", question=req.question)
    try:
        # The session is opened per request rather than held open for the
        # process lifetime: a dropped MCP connection then affects one question
        # instead of wedging the service until restart.
        async with mcp_tools() as tools:
            answer, trace = await run_agent(req.question, tools)
    except Exception as exc:  # noqa: BLE001
        log.error("agent run failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"agent failed: {exc}") from exc

    log.info(
        "question answered",
        confidence=answer.confidence,
        tool_calls=len(trace.tool_calls),
        hit_budget=trace.hit_call_budget,
    )
    return AskResponse(answer=answer, trace=trace)

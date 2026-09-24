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


def _flatten_exceptions(exc: BaseException) -> list[str]:
    """Recursively unwrap an ExceptionGroup into leaf exception messages.

    ExceptionGroups can nest (a TaskGroup inside a TaskGroup), so this walks
    all the way down rather than assuming one level.
    """
    if isinstance(exc, BaseExceptionGroup):
        leaves: list[str] = []
        for sub in exc.exceptions:
            leaves.extend(_flatten_exceptions(sub))
        return leaves
    return [f"{type(exc).__name__}: {exc}"]


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
        # The MCP SDK's SSE transport runs its read/write loops in an anyio
        # TaskGroup, so a failure there (or during cleanup while unwinding an
        # earlier error) surfaces as an ExceptionGroup whose str() is just
        # "unhandled errors in a TaskGroup (N sub-exceptions)" - the real cause
        # is nested inside .exceptions and would otherwise be lost. Unwrap it
        # so the log actually says what broke.
        detail = "; ".join(_flatten_exceptions(exc))
        log.error("agent run failed", error=detail)
        raise HTTPException(status_code=500, detail=f"agent failed: {detail}") from exc

    log.info(
        "question answered",
        confidence=answer.confidence,
        tool_calls=len(trace.tool_calls),
        hit_budget=trace.hit_call_budget,
    )
    return AskResponse(answer=answer, trace=trace)

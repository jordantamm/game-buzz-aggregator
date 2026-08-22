"""Analyst agent entrypoint.

Two modes:
  python -m analyst_agent "what's trending and why"   ask one question, print, exit
  python -m analyst_agent --serve                      run the POST /ask HTTP wrapper

The CLI mode is the demo path — it prints the tool-call sequence alongside the
answer, which is the interesting part.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

import structlog
import uvicorn

from analyst_agent.graph import run_agent
from analyst_agent.settings import settings
from analyst_agent.telemetry import init_telemetry
from analyst_agent.tools import mcp_tools


def _configure_logging(pretty: bool) -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    renderer = (
        structlog.dev.ConsoleRenderer() if pretty else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


log = structlog.get_logger()


async def ask_once(question: str, as_json: bool) -> int:
    async with mcp_tools() as tools:
        answer, trace = await run_agent(question, tools)

    if as_json:
        print(json.dumps({"answer": answer.model_dump(), "trace": trace.model_dump()}, indent=2))
        return 0

    print(f"\nQ: {question}\n")
    print("Tool calls:")
    for index, record in enumerate(trace.tool_calls, start=1):
        marker = " (empty)" if record.was_empty else ""
        print(f"  {index}. {record.tool}({json.dumps(record.arguments)}){marker}")
    if trace.reflections:
        print("\nSelf-corrections:")
        for reflection in trace.reflections:
            print(f"  - {reflection}")
    if trace.hit_call_budget:
        print(f"\n  [tool-call budget of {settings.max_tool_calls} was exhausted]")

    print(f"\nAnswer (confidence {answer.confidence:.2f}):\n{answer.summary}\n")
    if answer.citations:
        print("Citations:")
        for citation in answer.citations:
            game = f" [{citation.game_id}]" if citation.game_id else ""
            print(f"  - {citation.mention_id}{game}: {citation.excerpt}")
    print()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="analyst-agent")
    parser.add_argument("question", nargs="?", help="Question to ask")
    parser.add_argument("--serve", action="store_true", help="Run the HTTP server instead")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of prose")
    args = parser.parse_args()

    _configure_logging(pretty=not args.json)
    init_telemetry(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    if not settings.anthropic_api_key:
        print(
            "ANTHROPIC_API_KEY is not set. The analyst agent needs it to plan and "
            "synthesize; set it in .env and retry.",
            file=sys.stderr,
        )
        return 2

    if args.serve:
        log.info("analyst-agent serving", port=settings.server_port)
        uvicorn.run(
            "analyst_agent.server:app",
            host="0.0.0.0",
            port=settings.server_port,
            log_config=None,
        )
        return 0

    if not args.question:
        parser.error("provide a question, or pass --serve")

    return asyncio.run(ask_once(args.question, args.json))


if __name__ == "__main__":
    raise SystemExit(main())

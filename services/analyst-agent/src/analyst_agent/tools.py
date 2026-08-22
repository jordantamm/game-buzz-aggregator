"""MCP tool discovery.

Tools are discovered from the running mcp-server rather than declared here.
That keeps one source of truth: the Go server owns the tool surface and its
descriptions, and adding a tool there makes it available to the agent without
touching this service. Hand-mirroring the definitions would guarantee drift.
"""

from __future__ import annotations

import contextlib
from typing import AsyncIterator

import structlog
from langchain_core.tools import BaseTool
from langchain_mcp_adapters.tools import load_mcp_tools
from mcp import ClientSession
from mcp.client.sse import sse_client

from analyst_agent.settings import settings

log = structlog.get_logger()


@contextlib.asynccontextmanager
async def mcp_tools() -> AsyncIterator[list[BaseTool]]:
    """Open an MCP session and yield its tools as LangChain tools.

    Scoped as a context manager because the tools hold a live session — they
    stop working the moment it closes, so the graph run has to happen inside
    this block rather than the tools being cached at import time.
    """
    async with sse_client(settings.mcp_server_url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await load_mcp_tools(session)
            log.info(
                "discovered mcp tools",
                count=len(tools),
                names=[t.name for t in tools],
                url=settings.mcp_server_url,
            )
            if not tools:
                raise RuntimeError(
                    f"mcp-server at {settings.mcp_server_url} exposed no tools; "
                    "is it running with --http?"
                )
            yield tools

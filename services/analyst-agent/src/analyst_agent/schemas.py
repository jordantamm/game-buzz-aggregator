"""Structured output contract for the analyst agent.

The HTTP wrapper and CLI both consume `AnalystAnswer` directly. Nothing parses
prose out of the model's reply — the final synthesis step is forced through
Instructor against this schema for the same reason the enricher's disambiguation
call is: a caller that has to regex an answer out of free text breaks silently
the first time the model changes its phrasing.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class MentionRef(BaseModel):
    """A citation pointing at a specific mention the answer relied on."""

    mention_id: str = Field(description="The mention's ULID, exactly as returned by the tool")
    game_id: str | None = Field(default=None, description="Game slug, if the mention was attributed")
    excerpt: str = Field(
        max_length=280,
        description="A short verbatim quote from the mention that supports the claim.",
    )


class AnalystAnswer(BaseModel):
    """The agent's final answer."""

    summary: str = Field(
        description=(
            "The answer to the question, in prose. State findings directly and "
            "include concrete numbers from the tool results where they exist."
        )
    )
    citations: list[MentionRef] = Field(
        default_factory=list,
        description=(
            "Mentions supporting the summary. Only cite mention_ids that actually "
            "appeared in a tool result — never invent one."
        ),
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "How well the gathered evidence answers the question. Below 0.4 when the "
            "tools returned little or nothing, or when the tool-call budget ran out "
            "before the question was fully answered."
        ),
    )


class ToolCallRecord(BaseModel):
    """One executed tool call, kept for tracing and for the reflect step."""

    tool: str
    arguments: dict = Field(default_factory=dict)
    result_summary: str = ""
    was_empty: bool = False


class AgentTrace(BaseModel):
    """What the agent did, returned alongside the answer.

    Exposed rather than hidden so a demo can show the reasoning path, and so a
    bad answer can be debugged without re-running with logging turned up.
    """

    question: str
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    reflections: list[str] = Field(default_factory=list)
    hit_call_budget: bool = False


class AskResponse(BaseModel):
    answer: AnalystAnswer
    trace: AgentTrace


ReflectDecision = Literal["retry", "synthesize"]

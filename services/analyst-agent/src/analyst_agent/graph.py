"""The analyst agent's LangGraph state machine.

    plan ──▶ call_tool ──▶ reflect ──┬──▶ (retry) ──▶ plan
                                     └──▶ synthesize ──▶ END

Why a graph and not a Temporal workflow: the enricher's workflow is a fixed
durable DAG — the same four activities, in the same order, every time, where
surviving a crash matters. This is the opposite shape. Which tool to call, how
many times, and when to stop all depend on the question and on what came back,
and a failed run is not a data-loss event (the user just asks again). Branching
tool choice with a self-correction loop is what LangGraph's conditional edges
model; forcing it into Temporal would mean encoding a reasoning loop as a DAG.

The `reflect` node is the part that earns the graph. A single-shot agent that
calls `get_game_buzz` for a game with no recent mentions returns "no data" and
stops. This one notices the empty result and retries with a broader window or a
different tool before giving up.
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

import structlog
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from opentelemetry import trace

from analyst_agent.schemas import AgentTrace, AnalystAnswer, ToolCallRecord
from analyst_agent.settings import settings

log = structlog.get_logger()
tracer = trace.get_tracer("analyst-agent")


PLANNER_SYSTEM_PROMPT = """You are a game industry analyst with access to a live database of \
Reddit discussion about video games.

Answer the user's question by calling the available tools. Guidance:

- If you do not know a game's game_id slug, call list_trending first. It is the only tool that
  discovers slugs, and the others require one.
- Prefer get_game_buzz for "how much / is it rising / what is sentiment" questions, and
  search_mentions for "what are people actually saying / why" questions. Questions asking
  "what's trending and why" usually need both: the numbers, then the reasons.
- search_mentions matches semantically as well as literally, so search for the idea
  ("people complaining about performance"), not just keywords.
- When a tool returns nothing useful, do not give up. Widen the time window, drop the game_id
  filter, or rephrase the search query.
- Stop calling tools once you can answer the question. Do not gather evidence you will not use.

You have a hard budget of {max_tool_calls} tool calls for this question."""


SYNTHESIS_SYSTEM_PROMPT = """You are writing the final answer for a game industry analyst.

Base the answer only on the tool results in the conversation. Do not add outside knowledge \
about these games, and do not speculate beyond what the data shows.

Citations must reference mention_id values that actually appear in the tool results above. \
Never invent a mention_id. If you have no specific mentions to cite, return an empty citations \
list rather than fabricating one.

Set confidence honestly:
  0.8-1.0  the tools returned direct, sufficient evidence
  0.4-0.8  partial evidence; the answer is directionally right but thin
  0.0-0.4  the tools returned little or nothing, or the tool-call budget ran out first

If the tool budget was exhausted before you could fully answer, say so plainly in the summary \
and lower the confidence accordingly."""


class AgentState(TypedDict):
    """State threaded through the graph."""

    question: str
    messages: Annotated[list[AnyMessage], add_messages]
    tool_calls_made: int
    tool_records: list[ToolCallRecord]
    reflections: list[str]
    hit_budget: bool
    answer: AnalystAnswer | None


def build_graph(tools: list[BaseTool]):
    """Compile the agent graph against a discovered tool set."""
    llm = ChatAnthropic(
        model=settings.model,
        temperature=settings.temperature,
        api_key=settings.anthropic_api_key,
        max_tokens=2048,
    )
    planner = llm.bind_tools(tools)
    tools_by_name = {tool.name: tool for tool in tools}

    # ── nodes ────────────────────────────────────────────────────────────

    async def plan(state: AgentState) -> dict:
        """Decide the next tool call (or that no more are needed)."""
        with tracer.start_as_current_span("agent.plan") as span:
            span.set_attribute("agent.tool_calls_made", state["tool_calls_made"])

            messages: list[AnyMessage] = [
                SystemMessage(
                    content=PLANNER_SYSTEM_PROMPT.format(max_tool_calls=settings.max_tool_calls)
                ),
                *state["messages"],
            ]
            # Feed reflections back in so the planner knows what already failed
            # and does not simply repeat the call that returned nothing.
            if state["reflections"]:
                messages.append(
                    HumanMessage(
                        content=(
                            "Notes from previous attempts:\n"
                            + "\n".join(f"- {r}" for r in state["reflections"])
                            + "\n\nAdjust your approach accordingly."
                        )
                    )
                )

            response = await planner.ainvoke(messages)
            span.set_attribute("agent.planned_tool_calls", len(response.tool_calls or []))
            return {"messages": [response]}

    async def call_tool(state: AgentState) -> dict:
        """Execute the tool calls the planner requested."""
        last = state["messages"][-1]
        assert isinstance(last, AIMessage)

        new_messages: list[AnyMessage] = []
        records: list[ToolCallRecord] = []
        calls_made = state["tool_calls_made"]

        for call in last.tool_calls or []:
            if calls_made >= settings.max_tool_calls:
                # Must still answer every tool_call id, or the message history
                # becomes malformed and the next model call errors.
                new_messages.append(
                    ToolMessage(
                        content="Tool call budget exhausted. Answer with what you have.",
                        tool_call_id=call["id"],
                    )
                )
                continue

            with tracer.start_as_current_span(f"agent.tool.{call['name']}") as span:
                span.set_attribute("tool.name", call["name"])
                calls_made += 1

                tool = tools_by_name.get(call["name"])
                if tool is None:
                    content = f"Unknown tool {call['name']!r}. Available: {sorted(tools_by_name)}"
                    was_empty = True
                else:
                    try:
                        # Pass a copy, not call["args"] itself. LangChain's
                        # BaseTool.arun() mutates the input dict in place to
                        # inject run_manager/config (harmless for a Pydantic
                        # args_schema, which _parse_input copies - but MCP
                        # tools have a raw JSON-schema dict as args_schema, so
                        # _parse_input returns the SAME object). call["args"]
                        # is the literal dict living inside the AIMessage in
                        # `state["messages"]`; mutating it poisons the
                        # conversation history with a non-JSON-serializable
                        # AsyncCallbackManagerForToolRun, which blows up the
                        # next time this history is sent to the Anthropic API.
                        result = await tool.ainvoke(dict(call["args"]))
                        content = str(result)
                        was_empty = _looks_empty(content)
                    except Exception as exc:  # noqa: BLE001
                        # Surface the failure to the model as a tool result. It
                        # can often recover by adjusting arguments; crashing the
                        # request cannot.
                        log.warning("tool call failed", tool=call["name"], error=str(exc))
                        span.record_exception(exc)
                        content = f"Tool call failed: {exc}"
                        was_empty = True

                span.set_attribute("tool.result_empty", was_empty)
                new_messages.append(ToolMessage(content=content, tool_call_id=call["id"]))
                records.append(
                    ToolCallRecord(
                        tool=call["name"],
                        arguments=call["args"],
                        result_summary=content[:300],
                        was_empty=was_empty,
                    )
                )

        return {
            "messages": new_messages,
            "tool_calls_made": calls_made,
            "tool_records": state["tool_records"] + records,
            "hit_budget": calls_made >= settings.max_tool_calls,
        }

    async def reflect(state: AgentState) -> dict:
        """Judge whether the results so far can answer the question.

        Deliberately rule-based rather than another LLM call: the decision is
        "was this empty", which does not need a model, and an extra round trip
        per loop iteration is real latency on every question.
        """
        with tracer.start_as_current_span("agent.reflect") as span:
            recent = state["tool_records"][-3:]
            reflections = list(state["reflections"])

            if recent and all(record.was_empty for record in recent):
                note = (
                    f"The last {len(recent)} tool call(s) returned no usable data "
                    f"({', '.join(r.tool for r in recent)}). Try a wider time window, "
                    "remove the game_id filter, or switch to a different tool."
                )
                if note not in reflections:
                    reflections.append(note)

            span.set_attribute("agent.reflection_count", len(reflections))
            return {"reflections": reflections}

    async def synthesize(state: AgentState) -> dict:
        """Produce the typed final answer."""
        with tracer.start_as_current_span("agent.synthesize") as span:
            budget_note = ""
            if state["hit_budget"]:
                budget_note = (
                    f"\n\nNOTE: the {settings.max_tool_calls}-call tool budget was exhausted. "
                    "Answer from the evidence gathered and lower your confidence to reflect it."
                )

            structured = llm.with_structured_output(AnalystAnswer)
            answer: AnalystAnswer = await structured.ainvoke(
                [
                    SystemMessage(content=SYNTHESIS_SYSTEM_PROMPT + budget_note),
                    *state["messages"],
                    HumanMessage(
                        content=f"Now answer the original question: {state['question']}"
                    ),
                ]
            )

            # A citation naming a mention_id that never appeared in a tool
            # result is a fabricated source. Drop it rather than presenting it.
            answer = _drop_unsupported_citations(answer, state["tool_records"])

            span.set_attribute("agent.confidence", answer.confidence)
            span.set_attribute("agent.citation_count", len(answer.citations))
            return {"answer": answer}

    # ── edges ────────────────────────────────────────────────────────────

    def after_plan(state: AgentState) -> str:
        """If the planner requested tools, run them; otherwise it is ready to answer."""
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "call_tool"
        return "synthesize"

    def after_reflect(state: AgentState) -> str:
        """Loop back to planning only if there is budget left and results were thin."""
        if state["tool_calls_made"] >= settings.max_tool_calls:
            return "synthesize"
        recent = state["tool_records"][-3:]
        if recent and all(record.was_empty for record in recent):
            return "plan"
        return "synthesize"

    graph = StateGraph(AgentState)
    graph.add_node("plan", plan)
    graph.add_node("call_tool", call_tool)
    graph.add_node("reflect", reflect)
    graph.add_node("synthesize", synthesize)

    graph.set_entry_point("plan")
    graph.add_conditional_edges("plan", after_plan, {"call_tool": "call_tool", "synthesize": "synthesize"})
    graph.add_edge("call_tool", "reflect")
    graph.add_conditional_edges("reflect", after_reflect, {"plan": "plan", "synthesize": "synthesize"})
    graph.add_edge("synthesize", END)

    return graph.compile()


# ── helpers ──────────────────────────────────────────────────────────────────


def _looks_empty(content: str) -> bool:
    """Heuristic for "the tool ran fine but found nothing".

    The MCP tools return JSON, so an empty result is a zero-length list or an
    explicit note field rather than an error. Without this the agent treats
    `{"results": []}` as a successful answer and reports "no buzz" for a game
    that simply needed a wider window.
    """
    stripped = content.strip()
    if not stripped:
        return True
    markers = (
        '"results": []',
        '"series": []',
        '"result_count": 0',
        '"results":[]',
        '"series":[]',
        "Tool call failed",
        "No mentions for this game",
        "No similar games found",
    )
    return any(marker in stripped for marker in markers)


def _drop_unsupported_citations(
    answer: AnalystAnswer, records: list[ToolCallRecord]
) -> AnalystAnswer:
    """Remove citations whose mention_id never appeared in a tool result."""
    corpus = " ".join(record.result_summary for record in records)
    kept = [c for c in answer.citations if c.mention_id and c.mention_id in corpus]

    dropped = len(answer.citations) - len(kept)
    if dropped:
        log.warning("dropped fabricated citations", count=dropped)
        return answer.model_copy(update={"citations": kept})
    return answer


async def run_agent(question: str, tools: list[BaseTool]) -> tuple[AnalystAnswer, AgentTrace]:
    """Answer one question. Returns the typed answer plus what the agent did."""
    with tracer.start_as_current_span("agent.run") as span:
        span.set_attribute("agent.question", question)

        graph = build_graph(tools)
        final: dict[str, Any] = await graph.ainvoke(
            {
                "question": question,
                "messages": [HumanMessage(content=question)],
                "tool_calls_made": 0,
                "tool_records": [],
                "reflections": [],
                "hit_budget": False,
                "answer": None,
            },
            # Generous: the graph's own budget is max_tool_calls, this only
            # guards against an edge-case cycle in the graph itself.
            {"recursion_limit": settings.max_tool_calls * 4 + 10},
        )

        answer = final.get("answer")
        if answer is None:
            answer = AnalystAnswer(
                summary="The agent finished without producing an answer.",
                citations=[],
                confidence=0.0,
            )

        trace_obj = AgentTrace(
            question=question,
            tool_calls=final.get("tool_records", []),
            reflections=final.get("reflections", []),
            hit_call_budget=final.get("hit_budget", False),
        )
        span.set_attribute("agent.total_tool_calls", len(trace_obj.tool_calls))
        return answer, trace_obj

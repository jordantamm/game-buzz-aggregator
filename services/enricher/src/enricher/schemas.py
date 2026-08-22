"""Pydantic models exchanged between the Kafka consumer, the Temporal workflow,
and its activities.

Everything crossing a Temporal boundary must be JSON-serializable: Temporal
persists workflow inputs and activity results to its event history so a run can
be replayed after a worker crash. That rules out passing live objects (Kafka
producers, DB pools, model handles) as arguments — those are process-local and
are reached from inside activities via module-level singletons instead.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ── Entity resolution ────────────────────────────────────────────────────────

ResolutionMethod = Literal["exact", "trigram", "llm"]


class GameMatch(BaseModel):
    """One resolved game for a mention."""

    game_id: str = Field(description="Canonical game slug, e.g. 'elden-ring'")
    confidence: float = Field(ge=0.0, le=1.0)
    method: ResolutionMethod = Field(
        description="How the match was made: exact alias hit, trigram score, or LLM adjudication"
    )


class LLMGameMatch(BaseModel):
    """A single match as returned by the disambiguation LLM.

    This is the schema Instructor enforces on the model's output. `rationale` is
    required deliberately: asking for a justification alongside the ID measurably
    improves selection quality, and it gives a human something to read when an
    eval case regresses.
    """

    game_id: str = Field(
        description=(
            "The game_id of a candidate from the supplied list. "
            "Must be copied exactly from that list — never invented."
        )
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "How confident you are this post is about this game. "
            "Use below 0.5 when the post is only tangentially related."
        ),
    )
    rationale: str = Field(
        max_length=300,
        description="One sentence citing the specific wording in the post that identifies this game.",
    )


class DisambiguationResult(BaseModel):
    """Top-level schema for the disambiguation call.

    Returning an empty `matches` list is a valid and expected outcome — a post
    that merely name-drops a word colliding with a game title is not a mention
    of that game, and forcing a choice is what produces false attributions.
    """

    matches: list[LLMGameMatch] = Field(
        default_factory=list,
        description=(
            "Games this post is actually discussing. Empty list if the post is about "
            "none of the candidates. Do not guess to avoid returning an empty list."
        ),
    )

    @field_validator("matches")
    @classmethod
    def _reject_duplicates(cls, matches: list[LLMGameMatch]) -> list[LLMGameMatch]:
        seen: set[str] = set()
        for match in matches:
            if match.game_id in seen:
                raise ValueError(f"duplicate game_id {match.game_id!r} in matches")
            seen.add(match.game_id)
        return matches


# ── Activity payloads ────────────────────────────────────────────────────────


class SentimentResult(BaseModel):
    # `model_version` collides with pydantic's protected "model_" namespace.
    # The field name matches the protobuf field, so the namespace is disabled
    # rather than renaming and translating at every boundary.
    model_config = ConfigDict(protected_namespaces=())

    score: float = Field(ge=-1.0, le=1.0, description="-1.0 negative to +1.0 positive")
    magnitude: float = Field(ge=0.0, le=1.0, description="Distance from neutral")
    model_version: str


class EmbeddingResult(BaseModel):
    """A 384-dim embedding, base64-encoded.

    Raw bytes do not survive Temporal's JSON payload conversion cleanly, so the
    little-endian float32 buffer is carried as base64 and decoded in emit.
    """

    model_config = ConfigDict(protected_namespaces=())

    embedding_b64: str
    model: str
    dims: int


class EnrichmentOutcome(BaseModel):
    """What the workflow returns — used by tests and for Temporal UI legibility."""

    mention_id: str
    matched_game_ids: list[str] = Field(default_factory=list)
    emitted: int = 0
    skipped_reason: str | None = None

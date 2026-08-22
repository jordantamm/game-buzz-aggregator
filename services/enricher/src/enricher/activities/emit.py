"""Produce enriched events to `mentions.enriched`.

One input mention fans out to one output event per matched game, keyed by
`game:{game_id}` so every event for a game lands on the same partition and the
sink sees them in order.
"""

from __future__ import annotations

import base64

import structlog
from temporalio import activity

from enricher import runtime
from enricher.activities.protoio import decode_mention
from enricher.settings import settings

from gba.v1 import enriched_pb2

log = structlog.get_logger()


@activity.defn
async def emit_enriched(
    mention_b64: str,
    game_matches: list[dict],
    sentiment: dict,
    embedding: dict,
) -> int:
    """Emit one EnrichedMention per matched game. Returns the number emitted.

    This activity is retried by the workflow on failure. Re-emitting a duplicate
    is safe: `mention_id` is deterministic from (source, native_id), and the
    sink upserts on that key, so a redelivered event overwrites rather than
    duplicating.
    """
    rt = runtime.get()
    assert rt.producer is not None

    mention = decode_mention(mention_b64)
    embedding_bytes = base64.b64decode(embedding["embedding_b64"])

    emitted = 0
    for match in game_matches:
        enriched = enriched_pb2.EnrichedMention()
        enriched.mention.CopyFrom(mention)
        enriched.game_matches.append(
            enriched_pb2.GameMatch(
                game_id=match["game_id"],
                confidence=match["confidence"],
                method=match["method"],
            )
        )
        enriched.sentiment.CopyFrom(
            enriched_pb2.Sentiment(
                score=sentiment["score"],
                magnitude=sentiment["magnitude"],
                model_version=sentiment["model_version"],
            )
        )
        enriched.embedding = embedding_bytes
        enriched.embedding_model = embedding["model"]
        enriched.enricher_version = settings.enricher_version

        await rt.producer.send_and_wait(
            settings.output_topic,
            value=enriched.SerializeToString(),
            key=f"game:{match['game_id']}".encode(),
            headers=[
                ("content-type", b"application/protobuf"),
                ("trace_id", mention.trace_id.encode()),
            ],
        )
        emitted += 1
        activity.logger.debug(
            "emitted enriched mention",
            extra={"game_id": match["game_id"], "mention_id": mention.mention_id},
        )

    return emitted

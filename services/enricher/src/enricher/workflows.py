"""Temporal workflow orchestrating enrichment of a single mention.

Why Temporal at all: enrichment is a multi-step pipeline where the expensive
steps (an LLM call, two local model inferences) must not be redone if a later
step fails or the worker dies mid-mention. Temporal persists each activity's
result to its event history, so a crashed run resumes at the first incomplete
step rather than replaying the whole thing.

That durability is exactly why workflow inputs and activity results must be
JSON-serializable — they are written to that history. Live objects (the Kafka
producer, the games cache, model handles) never cross this boundary; activities
reach them through `enricher.runtime`.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Activity *references* are imported for their names/signatures. The imports are
# passed through the workflow sandbox because importing the activity modules
# would drag in torch/transformers, which the sandbox cannot safely re-import.
with workflow.unsafe.imports_passed_through():
    from enricher.activities.embeddings import generate_embedding
    from enricher.activities.emit import emit_enriched
    from enricher.activities.resolve import resolve_games
    from enricher.activities.sentiment import compute_sentiment
    from enricher.schemas import EnrichmentOutcome

# Applied to every activity. Jitter is on by default in the Temporal SDK.
RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=60),
    backoff_coefficient=2.0,
)

# Model inference is slower than the LLM call in the worst case (cold model
# load on first mention), so these get generous ceilings.
RESOLVE_TIMEOUT = timedelta(seconds=45)
INFERENCE_TIMEOUT = timedelta(seconds=120)
EMIT_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class EnrichMentionWorkflow:
    """Entity resolution, sentiment, and embedding for one Mention.

    Input is the base64-encoded serialized `Mention` protobuf. Protobuf bytes
    are used rather than a decoded dict so the workflow never has to know the
    schema — it forwards the payload to the activities that do.
    """

    @workflow.run
    async def run(self, mention_b64: str, mention_id: str) -> EnrichmentOutcome:
        # Step 1: entity resolution. Runs first because a mention matching no
        # game is dropped, and there is no point paying for sentiment and
        # embedding inference on something that will never be stored.
        text = await workflow.execute_activity(
            "extract_text",
            args=[mention_b64],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RETRY,
        )

        game_matches = await workflow.execute_activity(
            resolve_games,
            args=[text],
            start_to_close_timeout=RESOLVE_TIMEOUT,
            retry_policy=RETRY,
        )

        if not game_matches:
            return EnrichmentOutcome(
                mention_id=mention_id,
                skipped_reason="no_game_matched",
            )

        # Steps 2 and 3 are independent, so they run concurrently. Note these
        # are two separate activity tasks — the previous implementation awaited
        # them sequentially while appearing to parallelise them.
        sentiment_future = workflow.execute_activity(
            compute_sentiment,
            args=[text],
            start_to_close_timeout=INFERENCE_TIMEOUT,
            retry_policy=RETRY,
        )
        embedding_future = workflow.execute_activity(
            generate_embedding,
            args=[text],
            start_to_close_timeout=INFERENCE_TIMEOUT,
            retry_policy=RETRY,
        )
        sentiment = await sentiment_future
        embedding = await embedding_future

        # Step 4: one enriched event per matched game, keyed by game_id so all
        # events for a game land on one partition.
        emitted = await workflow.execute_activity(
            emit_enriched,
            args=[mention_b64, game_matches, sentiment, embedding],
            start_to_close_timeout=EMIT_TIMEOUT,
            retry_policy=RETRY,
        )

        return EnrichmentOutcome(
            mention_id=mention_id,
            matched_game_ids=[m["game_id"] for m in game_matches],
            emitted=emitted,
        )

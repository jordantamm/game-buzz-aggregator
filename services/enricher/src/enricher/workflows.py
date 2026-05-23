from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from enricher.activities.resolve import resolve_games
    from enricher.activities.sentiment import compute_sentiment
    from enricher.activities.embeddings import generate_embedding

RETRY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=60),
    backoff_coefficient=2.0,
)


@workflow.defn
class EnrichMentionWorkflow:
    """Orchestrates entity resolution, sentiment, and embedding for one Mention."""

    @workflow.run
    async def run(self, mention_bytes: bytes, game_cache, disambiguation_cache, producer) -> None:
        import sys
        sys.path.insert(0, "/app/gen/python")
        from gba.v1 import mention_pb2

        mention = mention_pb2.Mention()
        mention.ParseFromString(mention_bytes)
        text = mention.text

        # Step 1: resolve games
        game_matches = await workflow.execute_activity(
            resolve_games,
            args=[text, game_cache, disambiguation_cache],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )

        if not game_matches:
            return  # No game found — drop

        # Steps 2 & 3 can run in parallel
        sentiment_result, embedding_bytes = await workflow.execute_activity(
            compute_sentiment,
            args=[text],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RETRY,
        ), await workflow.execute_activity(
            generate_embedding,
            args=[text],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RETRY,
        )

        # Step 4: emit one event per matched game
        from enricher.activities.emit import emit_enriched
        await workflow.execute_activity(
            emit_enriched,
            args=[mention_bytes, game_matches, sentiment_result, embedding_bytes, producer],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )

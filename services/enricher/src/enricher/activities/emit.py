import structlog
from temporalio import activity

from enricher.settings import settings

log = structlog.get_logger()


@activity.defn
async def emit_enriched(
    mention_bytes: bytes,
    game_matches: list[dict],
    sentiment: dict,
    embedding: bytes,
    producer,
) -> None:
    """Serialize and produce one EnrichedMention per matched game."""
    import sys
    sys.path.insert(0, "/app/gen/python")

    from gba.v1 import mention_pb2, enriched_pb2

    mention = mention_pb2.Mention()
    mention.ParseFromString(mention_bytes)

    for match in game_matches:
        em = enriched_pb2.EnrichedMention()
        em.mention.CopyFrom(mention)
        em.game_matches.append(
            enriched_pb2.GameMatch(
                game_id=match["game_id"],
                confidence=match["confidence"],
                method=match["method"],
            )
        )
        em.sentiment.CopyFrom(
            enriched_pb2.Sentiment(
                score=sentiment["score"],
                magnitude=sentiment["magnitude"],
                model_version=sentiment["model_version"],
            )
        )
        em.embedding = embedding
        em.embedding_model = "all-MiniLM-L6-v2"
        em.enricher_version = settings.enricher_version

        key = f"game:{match['game_id']}"
        await producer.send_and_wait(
            settings.output_topic,
            value=em.SerializeToString(),
            key=key.encode(),
            headers={"content-type": b"application/protobuf"},
        )
        log.info("emitted enriched mention", game_id=match["game_id"], mention_id=mention.mention_id)

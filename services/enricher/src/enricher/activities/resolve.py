import structlog
from temporalio import activity

from enricher.caches.disambiguation import DisambiguationCache
from enricher.caches.games import GameCache
from enricher.settings import settings

log = structlog.get_logger()


@activity.defn
async def resolve_games(
    text: str,
    game_cache: GameCache,
    disambiguation_cache: DisambiguationCache,
) -> list[dict]:
    """
    Returns a list of {"game_id": str, "confidence": float, "method": str}.
    Method is "exact", "trigram", or "llm".
    """
    candidates = game_cache.lookup(text)

    if not candidates:
        return []

    best_id, best_score = candidates[0]

    # High-confidence single match — no LLM needed
    if best_score >= 0.9 and (len(candidates) == 1 or best_score - candidates[1][1] > 0.2):
        return [{"game_id": best_id, "confidence": best_score, "method": "trigram"}]

    # Ambiguous — try LLM
    top_ids = [gid for gid, _ in candidates[:5]]

    cached = await disambiguation_cache.get(text, top_ids)
    if cached is not None:
        return [{"game_id": gid, "confidence": 0.85, "method": "llm"} for gid in cached]

    llm_result = await _llm_disambiguate(text, game_cache.get_candidates(top_ids))
    await disambiguation_cache.set(text, top_ids, llm_result)
    return [{"game_id": gid, "confidence": 0.85, "method": "llm"} for gid in llm_result]


async def _llm_disambiguate(text: str, candidates: list) -> list[str]:
    if not settings.anthropic_api_key:
        log.warning("no anthropic key, skipping llm disambiguation")
        return []

    try:
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        candidate_list = "\n".join(
            f"- {c.game_id}: {c.canonical_name} (aliases: {', '.join(c.aliases[:3])})"
            for c in candidates
        )
        prompt = f"""You are a video game entity resolver. Given this social media post:

"{text}"

Which of these games is it about? Return ONLY the game_id(s) as a JSON array.
If it's about none of them, return [].

Games:
{candidate_list}

Response (JSON array of game_id strings only):"""

        msg = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=128,
            messages=[{"role": "user", "content": prompt}],
        )
        import json
        raw = msg.content[0].text.strip()
        result = json.loads(raw)
        if isinstance(result, list):
            return [str(gid) for gid in result]
        return []
    except Exception as e:
        log.warning("llm disambiguation failed", error=str(e))
        return []

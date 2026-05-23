import asyncio
import structlog
from functools import lru_cache
from temporalio import activity

log = structlog.get_logger()

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
MODEL_VERSION = "twitter-roberta-base-sentiment-latest-v1"


@lru_cache(maxsize=1)
def _load_pipeline():
    from transformers import pipeline
    return pipeline("text-classification", model=MODEL_NAME, return_all_scores=True)


@activity.defn
async def compute_sentiment(text: str) -> dict:
    """Returns {"score": float, "magnitude": float, "model_version": str}"""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run_sentiment, text[:512])
    return result


def _run_sentiment(text: str) -> dict:
    try:
        pipe = _load_pipeline()
        scores = pipe(text)[0]
        # Labels: LABEL_0=negative, LABEL_1=neutral, LABEL_2=positive
        label_map = {s["label"]: s["score"] for s in scores}
        neg = label_map.get("LABEL_0", 0.0)
        pos = label_map.get("LABEL_2", 0.0)
        # Map to -1..1
        score = pos - neg
        # Magnitude = how far from neutral
        magnitude = abs(score)
        return {"score": round(score, 4), "magnitude": round(magnitude, 4), "model_version": MODEL_VERSION}
    except Exception as e:
        log.warning("sentiment failed, returning neutral", error=str(e))
        return {"score": 0.0, "magnitude": 0.0, "model_version": MODEL_VERSION}

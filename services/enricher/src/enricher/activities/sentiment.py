"""Sentiment scoring via cardiffnlp/twitter-roberta-base-sentiment-latest.

Chosen over a general-purpose sentiment model because it is trained on short,
informal, emoji-bearing social text, which is what Reddit comments are.
"""

from __future__ import annotations

import asyncio
from functools import lru_cache

import structlog
from temporalio import activity

from enricher.schemas import SentimentResult

log = structlog.get_logger()

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
MODEL_VERSION = "twitter-roberta-base-sentiment-latest-v1"

# The model's positional embeddings cap at 512 tokens; characters are a cheap
# conservative proxy for staying under that.
MAX_CHARS = 1024


@lru_cache(maxsize=1)
def _load_pipeline():
    from transformers import pipeline

    log.info("loading sentiment model", model=MODEL_NAME)
    return pipeline(
        "text-classification",
        model=MODEL_NAME,
        top_k=None,  # return all class scores (replaces deprecated return_all_scores)
        truncation=True,
        max_length=512,
    )


def warm_up() -> None:
    """Load the model at worker startup instead of inside the first activity."""
    _load_pipeline()


@activity.defn
async def compute_sentiment(text: str) -> dict:
    """Score text from -1.0 (negative) to +1.0 (positive)."""
    return await asyncio.to_thread(_score, text[:MAX_CHARS])


def _score(text: str) -> dict:
    scores = _load_pipeline()(text)[0]
    by_label = {entry["label"].lower(): entry["score"] for entry in scores}

    # This checkpoint emits named labels; older revisions emit LABEL_0/1/2.
    # Accept both so a model-hub revision bump does not silently zero every score.
    negative = by_label.get("negative", by_label.get("label_0", 0.0))
    positive = by_label.get("positive", by_label.get("label_2", 0.0))
    neutral = by_label.get("neutral", by_label.get("label_1", 0.0))

    score = positive - negative
    # Magnitude is how opinionated the text is, independent of direction: a
    # strongly mixed post and a strongly positive post both carry more signal
    # than a flatly neutral one.
    magnitude = min(1.0, 1.0 - neutral)

    return SentimentResult(
        score=round(max(-1.0, min(1.0, score)), 4),
        magnitude=round(max(0.0, magnitude), 4),
        model_version=MODEL_VERSION,
    ).model_dump()

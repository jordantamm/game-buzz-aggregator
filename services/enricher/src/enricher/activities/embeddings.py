"""Embedding generation via sentence-transformers all-MiniLM-L6-v2 (384 dims).

The model is loaded once per worker process and shared. Inference is CPU-bound
and releases the GIL inside torch, so it runs in a thread executor to avoid
blocking the activity event loop.
"""

from __future__ import annotations

import asyncio
import base64
import struct
from functools import lru_cache

import structlog
from temporalio import activity

from enricher.schemas import EmbeddingResult

log = structlog.get_logger()

MODEL_NAME = "all-MiniLM-L6-v2"
DIMS = 384

# The model has a 256-token window; feeding it more just wastes time truncating.
MAX_CHARS = 1024


@lru_cache(maxsize=1)
def _load_model():
    from sentence_transformers import SentenceTransformer

    log.info("loading embedding model", model=MODEL_NAME)
    return SentenceTransformer(MODEL_NAME)


def warm_up() -> None:
    """Load the model at worker startup rather than on the first mention.

    Without this the first enrichment pays a multi-second model download/load
    inside an activity timeout, which looks like a flaky pipeline.
    """
    _load_model()


@activity.defn
async def generate_embedding(text: str) -> dict:
    """Return a base64-encoded little-endian float32[384] embedding."""
    vector = await asyncio.to_thread(_encode, text[:MAX_CHARS])
    return EmbeddingResult(
        embedding_b64=base64.b64encode(vector).decode("ascii"),
        model=MODEL_NAME,
        dims=DIMS,
    ).model_dump()


def _encode(text: str) -> bytes:
    model = _load_model()
    # normalize_embeddings makes cosine distance equivalent to dot product,
    # which is what the pgvector HNSW index is built for.
    vector = model.encode(text, normalize_embeddings=True)
    values = vector.tolist()
    if len(values) != DIMS:
        raise ValueError(f"embedding model returned {len(values)} dims, expected {DIMS}")
    return struct.pack(f"<{DIMS}f", *values)

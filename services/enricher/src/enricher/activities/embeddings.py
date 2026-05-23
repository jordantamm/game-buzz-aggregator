import asyncio
import struct
import structlog
from functools import lru_cache
from temporalio import activity

log = structlog.get_logger()

MODEL_NAME = "all-MiniLM-L6-v2"


@lru_cache(maxsize=1)
def _load_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(MODEL_NAME)


@activity.defn
async def generate_embedding(text: str) -> bytes:
    """Returns 384-dim float32 vector as little-endian bytes."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_embedding, text[:512])


def _run_embedding(text: str) -> bytes:
    try:
        model = _load_model()
        vec = model.encode(text, normalize_embeddings=True)
        return struct.pack(f"<{len(vec)}f", *vec.tolist())
    except Exception as e:
        log.warning("embedding failed, returning zeros", error=str(e))
        return bytes(384 * 4)

"""HTTP endpoint exposing the enricher's embedding model.

Hybrid search needs the *query* embedded with the same model that produced the
stored mention embeddings — a vector from a different model is not comparable,
and cosine distance against it is noise. That model lives here, in Python, and
the search path lives in Go (api-gateway, mcp-server), so it is exposed over
HTTP rather than loaded twice.

Served from the enricher process so there is exactly one copy of the model
weights in memory and one place where the model version is decided.
"""

from __future__ import annotations

import base64
import struct

import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from enricher.activities.embeddings import DIMS, MODEL_NAME, _encode

log = structlog.get_logger()

app = FastAPI(title="enricher-embedder", version="0.1.0")


class EmbedRequest(BaseModel):
    text: str = Field(min_length=1, max_length=8192)


class EmbedResponse(BaseModel):
    embedding: list[float]
    model: str
    dims: int


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict:
    """Ready only once the model is actually loaded and can encode."""
    try:
        _encode("readiness probe")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"model not ready: {exc}") from exc
    return {"status": "ready", "model": MODEL_NAME, "dims": DIMS}


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest) -> EmbedResponse:
    """Embed a query string.

    Runs synchronously rather than in a thread: query embedding of a short
    string is single-digit milliseconds, and the extra hop costs more than it
    saves. Bulk mention embedding goes through the Temporal activity instead.
    """
    try:
        packed = _encode(req.text)
    except Exception as exc:  # noqa: BLE001
        log.warning("embed failed", error=str(exc))
        raise HTTPException(status_code=500, detail="embedding failed") from exc

    values = list(struct.unpack(f"<{DIMS}f", packed))
    return EmbedResponse(embedding=values, model=MODEL_NAME, dims=DIMS)


@app.post("/embed_b64")
async def embed_b64(req: EmbedRequest) -> dict:
    """Same vector, as a base64 little-endian float32 buffer.

    Cheaper on the wire than a JSON array of 384 floats for callers that only
    need to forward the bytes.
    """
    packed = _encode(req.text)
    return {
        "embedding_b64": base64.b64encode(packed).decode("ascii"),
        "model": MODEL_NAME,
        "dims": DIMS,
    }

"""Protobuf decoding at the activity boundary.

The workflow carries the mention as an opaque base64 string and never decodes
it. Decoding happens here, in an activity, for two reasons: the generated
protobuf modules are a non-deterministic import that the Temporal workflow
sandbox should not load, and keeping the workflow schema-agnostic means a proto
change does not force a workflow revision.
"""

from __future__ import annotations

import base64

import structlog
from temporalio import activity

from gba.v1 import mention_pb2

log = structlog.get_logger()


def decode_mention(mention_b64: str) -> mention_pb2.Mention:
    """Decode a base64-encoded serialized Mention protobuf."""
    mention = mention_pb2.Mention()
    mention.ParseFromString(base64.b64decode(mention_b64))
    return mention


def encode_mention(mention: mention_pb2.Mention) -> str:
    """Encode a Mention protobuf as base64 for transport through Temporal."""
    return base64.b64encode(mention.SerializeToString()).decode("ascii")


@activity.defn(name="extract_text")
async def extract_text(mention_b64: str) -> str:
    """Pull the text field out of a serialized Mention.

    Exists as an activity rather than inline workflow code so the protobuf
    import stays out of the workflow sandbox.
    """
    return decode_mention(mention_b64).text

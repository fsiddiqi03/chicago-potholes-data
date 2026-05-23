"""
AWS Lambda entry point for the daily pothole ingest.

Lambda calls a function with signature `handler(event, context)` — this
module provides that, wrapping the same `ingest()` function the CLI uses.
No business logic lives here; this is purely glue between Lambda's
invocation contract and our existing code.

Event payload (all optional):
    {
        "mode": "incremental" | "backfill",   # default: incremental
        "since": "YYYY-MM-DD"                  # required if mode=backfill
    }

Empty event ({}) runs an incremental — which is what EventBridge will send.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from .loaders.load_potholes import ingest

# Lambda's runtime captures stdout/stderr and routes it to CloudWatch.
# A single configured root logger means our existing module-level loggers
# (which use logging.getLogger(__name__)) flow through to CloudWatch.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,  # override any prior config (Lambda runtime sets its own)
)
logger = logging.getLogger(__name__)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda entry point.

    Always returns a 200-shaped response on success. On failure, raises
    the exception — Lambda's retry behavior and CloudWatch logging will
    surface it. EventBridge schedules are configured to not retry, so a
    raised exception just gets logged and we wait for tomorrow's run.
    """
    logger.info("Lambda invocation start. Event: %s", json.dumps(event or {}))

    # Defensive: AWS sometimes passes None as event in test invocations.
    event = event or {}

    mode = event.get("mode", "incremental")
    since = event.get("since")

    if mode == "backfill":
        if not since:
            raise ValueError("Backfill mode requires 'since' (YYYY-MM-DD)")
        logger.info("Running backfill from %s", since)
        ingest(backfill_since=since, dry_run=False)
    elif mode == "incremental":
        logger.info("Running incremental ingest")
        ingest(backfill_since=None, dry_run=False)
    else:
        raise ValueError(f"Unknown mode: {mode!r}")

    logger.info("Lambda invocation complete.")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "mode": mode,
            "request_id": getattr(context, "aws_request_id", None),
        }),
    }
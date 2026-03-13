"""Structured logging configuration for the budget comparator backend.

Usage:
    from src.shared.observability.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Pipeline started", extra={"job_id": job_id})

Environment variables:
    LOG_LEVEL  – DEBUG | INFO (default) | WARNING | ERROR | CRITICAL
    LOG_FORMAT – text (default) | json
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

_RESERVED_LOG_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__.keys())


def _serialize_extra(value: Any) -> Any:
    """Convert non-JSON-safe values into strings for robust structured logs."""
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        return str(value)


def _extract_extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    extras: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED_LOG_RECORD_ATTRS:
            continue
        if key in {"message", "asctime"}:
            continue
        if key.startswith("_"):
            continue
        extras[key] = _serialize_extra(value)
    return extras


class _StructuredFormatter(logging.Formatter):
    """Outputs one JSON object per log line for machine parsing."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_extract_extra_fields(record))
        if record.exc_info and record.exc_info[1]:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class _HumanFormatter(logging.Formatter):
    """Readable coloured output for local development."""

    FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    DATEFMT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self.FMT, datefmt=self.DATEFMT)

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = _extract_extra_fields(record)
        if not extras:
            return base
        serialized = " ".join(
            f"{key}={json.dumps(value, ensure_ascii=False)}"
            for key, value in sorted(extras.items())
        )
        return f"{base} | {serialized}"


def get_logger(name: str) -> logging.Logger:
    """Return a pre-configured logger.

    First call for a given *name* creates the handler; subsequent calls
    return the cached logger (standard ``logging`` behaviour).
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, level_name, logging.INFO))

        handler = logging.StreamHandler(sys.stdout)
        log_format = os.environ.get("LOG_FORMAT", "text").strip().lower()
        if log_format == "json":
            handler.setFormatter(_StructuredFormatter())
        else:
            handler.setFormatter(_HumanFormatter())

        logger.addHandler(handler)
        logger.propagate = False

    return logger

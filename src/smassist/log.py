from __future__ import annotations

import logging
import os
from typing import Optional


def configure_logging(level: str = "INFO") -> None:
    """Configure stdlib logging once for the app.

    Uses a simple, readable format that works well locally and in CI.
    """
    level = (level or os.getenv("SMASSIST_LOG_LEVEL") or "INFO").upper()

    root = logging.getLogger()
    if root.handlers:
        # Avoid double-configuring when called multiple times.
        root.setLevel(level)
        _tune_third_party_loggers()
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    _tune_third_party_loggers()


def _tune_third_party_loggers() -> None:
    # yfinance can be very noisy (404s for delisted/unsupported symbols) and
    # frequently logs at ERROR even when the application can safely proceed.
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

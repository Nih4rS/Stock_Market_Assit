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
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

from __future__ import annotations

from typing import Iterable, List

import pandas as pd


GOOD_STOCKS_COLUMNS: List[str] = [
    "Timestamp",
    "Ticker",
    "Strategy",
    "Score",
    "Close",
    "RSI14",
    "SMA50",
    "SMA200",
    "Dist_52wHigh",
    "Vol5x20",
]


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """Ensure DataFrame has the given columns (in-place add if missing)."""
    for c in columns:
        if c not in df.columns:
            df[c] = None


def ordered_df(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    ensure_columns(df, columns)
    # Use .loc to guarantee DataFrame return type.
    return df.loc[:, columns]

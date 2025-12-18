from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .data import sma, rsi

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    ticker: str
    strategy: str
    score: float
    metrics: Dict[str, float]


def last_valid(series: pd.Series) -> float | None:
    if series is None or series.empty:
        return None
    val = series.dropna()
    return float(val.iloc[-1]) if not val.empty else None


def _get_series(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a single Series for a column name.

    Some upstream data sources (or transformations) can yield duplicate column
    labels or MultiIndex columns, where `df["Close"]` returns a DataFrame.
    Strategies expect a 1-D series; in those cases we take the first column.
    """
    col = df[column]
    if isinstance(col, pd.DataFrame):
        if col.shape[1] == 0:
            return pd.Series(dtype="float64")
        return col.iloc[:, 0]
    return col


def strat_golden_cross(df: pd.DataFrame) -> Tuple[bool, float, Dict[str, float]]:
    close = _get_series(df, "Close")
    s50 = sma(close, 50)
    s200 = sma(close, 200)
    if s50.isna().all() or s200.isna().all():
        return False, 0.0, {}
    cond = s50.iloc[-1] > s200.iloc[-1]
    # bonus if recently crossed
    crossed = (s50.shift(1) <= s200.shift(1)) & (s50 > s200)
    recent = crossed.tail(10).any()
    score = 2.0 + (1.0 if recent else 0.0) if cond else 0.0
    return cond, score, {
        "Close": last_valid(close) or np.nan,
        "SMA50": last_valid(s50) or np.nan,
        "SMA200": last_valid(s200) or np.nan,
    }


def strat_rsi_momentum(df: pd.DataFrame) -> Tuple[bool, float, Dict[str, float]]:
    close = _get_series(df, "Close")
    r = rsi(close, 14)
    if r.isna().all():
        return False, 0.0, {}
    val = r.iloc[-1]
    rising = False
    if len(r) >= 5:
        last5 = r.diff().iloc[-5:]
        positives = sum(1 for x in (last5 > 0).tolist() if x)
        rising = bool(positives >= 3)
    cond = (val >= 55) & (val <= 70) & rising
    score = 1.5 if cond else 0.0
    return bool(cond), score, {"Close": last_valid(close) or np.nan, "RSI14": float(val)}


def strat_breakout_52w(df: pd.DataFrame) -> Tuple[bool, float, Dict[str, float]]:
    close = _get_series(df, "Close")
    if len(close) < 200:
        return False, 0.0, {}
    high_52w_val = float(close.tail(252).max())
    last_val = float(close.iloc[-1])
    dist = (high_52w_val - last_val) / high_52w_val if high_52w_val > 0 else np.inf
    cond = (last_val >= 0.98 * high_52w_val) if high_52w_val > 0 else False
    score = 2.0 if cond else 0.0
    return cond, score, {
        "Close": float(last_val),
        "Dist_52wHigh": float(dist),
    }


def strat_volume_surge(df: pd.DataFrame) -> Tuple[bool, float, Dict[str, float]]:
    if "Volume" not in df:
        return False, 0.0, {}

    vol = _get_series(df, "Volume")
    if vol.empty or vol.isna().all():
        return False, 0.0, {}

    v5 = vol.rolling(5).mean()
    v20 = vol.rolling(20).mean()
    if v20.isna().all():
        return False, 0.0, {}
    ratio = (v5.iloc[-1] / v20.iloc[-1]) if v20.iloc[-1] else 0.0
    cond = ratio >= 1.5
    score = 1.0 if cond else 0.0
    return cond, score, {"Vol5x20": float(ratio)}


STRATEGY_FUNCS = {
    "golden_cross": strat_golden_cross,
    "rsi_momentum": strat_rsi_momentum,
    "breakout_52w": strat_breakout_52w,
    "volume_surge": strat_volume_surge,
}


def evaluate_strategies(ticker: str, df: pd.DataFrame, strategies: List[str]) -> List[Signal]:
    signals: List[Signal] = []
    for s in strategies:
        func = STRATEGY_FUNCS.get(s)
        if not func:
            continue
        try:
            ok, score, metrics = func(df)
        except Exception:
            logger.exception("Strategy evaluation failed", extra={"ticker": ticker, "strategy": s})
            ok, score, metrics = False, 0.0, {}
        if ok and score > 0:
            signals.append(Signal(ticker, s, float(score), metrics))
    return signals

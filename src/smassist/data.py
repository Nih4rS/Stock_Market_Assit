from __future__ import annotations

import io
import time
import logging
from typing import Iterable, List, Optional

import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)


_DEFAULT_HEADERS = {
    "User-Agent": "smassist/1.0 (+https://github.com/Nih4rS/Stock_Market_Assit)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _try_load_sp500_from_github() -> List[str]:
    urls = [
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
    ]
    last_err: Exception | None = None
    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers=_DEFAULT_HEADERS)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            if "Symbol" not in df.columns:
                continue
            tickers = df["Symbol"].astype(str).str.strip().tolist()
            tickers = [t.replace(".", "-") for t in tickers]
            tickers = [t for t in tickers if t]
            if tickers:
                return tickers
        except Exception as e:
            last_err = e
    if last_err is not None:
        raise last_err
    return []


def _try_load_sp500_from_wikipedia() -> List[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, timeout=15, headers=_DEFAULT_HEADERS)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text), match="Symbol")
    if not tables:
        return []
    df = tables[0]
    if "Symbol" not in df.columns:
        return []
    tickers = df["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
    return [t for t in tickers if t]


def load_universe_sp500() -> List[str]:
    # Prefer stable sources that won't 403 in automation.
    for name, loader in (
        ("github", _try_load_sp500_from_github),
        ("wikipedia", _try_load_sp500_from_wikipedia),
    ):
        try:
            tickers = loader()
            tickers = [t for t in tickers if t and str(t).strip()]
            if tickers:
                return tickers
        except Exception as e:
            # Avoid noisy stack traces in normal operation.
            logger.warning("Failed to load S&P 500 universe from %s: %s", name, e)

    # Minimal fallback to ensure runnable even offline
    return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "AVGO"]


def load_universe_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


def load_universe_from_excel(path: str, sheet: str | int | None = None) -> List[str]:
    """Load tickers from an Excel file.

    Tries a 'Ticker' column first; otherwise uses the first column.
    This is intentionally forgiving so it can work with many playbook formats.
    """
    df = pd.read_excel(path, sheet_name=sheet if sheet is not None else 0)
    if df.empty:
        return []
    if "Ticker" in df.columns:
        s = df["Ticker"]
    else:
        s = df[df.columns[0]]
    tickers = (
        s.astype(str)
        .str.strip()
        .replace({"nan": "", "None": ""})
        .tolist()
    )
    out = []
    for t in tickers:
        if not t:
            continue
        if t.startswith("#"):
            continue
        out.append(t)
    # de-dupe preserve order
    return list(dict.fromkeys(out))


def fetch_history(
    tickers: Iterable[str],
    period: str = "1y",
    interval: str = "1d",
    group_by: str = "ticker",
    auto_adjust: bool = True,
    backoff: float = 0.5,
) -> dict[str, pd.DataFrame]:
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return {}

    # yfinance can batch download, but sometimes individual fetches are more reliable
    result: dict[str, pd.DataFrame] = {}
    for t in tickers:
        for attempt in range(3):
            try:
                df = yf.download(
                    t,
                    period=period,
                    interval=interval,
                    auto_adjust=auto_adjust,
                    progress=False,
                )
                if isinstance(df, pd.DataFrame) and not df.empty:
                    df = df.rename(columns=str.title)
                    result[t] = df
                    break
            except Exception:
                logger.exception("Price fetch failed", extra={"ticker": t, "attempt": attempt + 1})
            time.sleep(backoff * (attempt + 1))
    return result


def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(span=period, adjust=False).mean()
    roll_down = down.ewm(span=period, adjust=False).mean()
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

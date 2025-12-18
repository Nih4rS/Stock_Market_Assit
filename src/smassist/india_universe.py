from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


NSE_EQUITY_LIST_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"


@dataclass(frozen=True)
class ListedCompany:
    symbol: str
    name: str
    exchange: str  # "NSE" or "BSE"

    def yfinance_ticker(self) -> str:
        if self.exchange.upper() == "NSE":
            return f"{self.symbol}.NS"
        if self.exchange.upper() == "BSE":
            return f"{self.symbol}.BO"
        return self.symbol


def _http_get_text(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Stock_Market_Assit/1.0)",
        "Accept": "text/csv,text/plain,*/*",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def fetch_nse_listed_companies() -> List[ListedCompany]:
    """Fetch NSE listed equities list.

    Source: NSE archives EQUITY_L.csv

    Returns symbol+name only (NSE does not include sector/industry in this file).
    """
    try:
        text = _http_get_text(NSE_EQUITY_LIST_URL)
        df = pd.read_csv(io.StringIO(text))
        # Column names include spaces in NSE CSV.
        sym_col = next((c for c in df.columns if str(c).strip().upper() == "SYMBOL"), None)
        name_col = next((c for c in df.columns if str(c).strip().upper() in ("NAME OF COMPANY", "NAME")), None)
        if sym_col is None or name_col is None:
            logger.error("Unexpected NSE equity list columns: %s", list(df.columns))
            return []

        out: List[ListedCompany] = []
        for _, row in df.iterrows():
            sym = str(row.get(sym_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            if not sym or sym.lower() == "nan":
                continue
            if sym.startswith("#"):
                continue
            out.append(ListedCompany(symbol=sym, name=name, exchange="NSE"))
        # de-dupe preserve order
        return list(dict.fromkeys(out))
    except Exception:
        logger.exception("Failed to fetch NSE listed companies")
        return []


def load_india_universe(universe: str) -> List[ListedCompany]:
    """Load India universe based on a short code.

    Supported:
    - "nse": NSE listed equities

    BSE support is intentionally not enabled by default because BSE does not
    provide a stable, unauthenticated symbol master endpoint.
    """
    u = (universe or "").strip().lower()
    if u == "nse":
        return fetch_nse_listed_companies()
    raise ValueError(f"Unsupported india universe: {universe}")


def companies_to_tickers(companies: List[ListedCompany]) -> List[str]:
    return [c.yfinance_ticker() for c in companies]


def companies_to_meta(companies: List[ListedCompany]) -> Dict[str, Dict[str, str]]:
    """Return a ticker-keyed metadata mapping."""
    out: Dict[str, Dict[str, str]] = {}
    for c in companies:
        out[c.yfinance_ticker()] = {
            "symbol": c.symbol,
            "name": c.name,
            "exchange": c.exchange,
        }
    return out

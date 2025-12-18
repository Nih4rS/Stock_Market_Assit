from __future__ import annotations

from dataclasses import asdict
from typing import Dict, List

import pandas as pd

from .config import ScanConfig
from .data import fetch_history, load_universe_sp500, load_universe_from_file, load_universe_from_excel
from .india_universe import companies_to_tickers, load_india_universe
from .strategies import evaluate_strategies


def load_universe(cfg: ScanConfig) -> List[str]:
    if cfg.universe.lower() == "sp500":
        return load_universe_sp500()
    if cfg.universe.lower() in ("nse",):
        companies = load_india_universe(cfg.universe)
        return companies_to_tickers(companies)
    if cfg.universe.lower().startswith("excel:"):
        # Format: excel:/path/to/file.xlsx (uses first sheet)
        path = cfg.universe.split(":", 1)[1]
        return load_universe_from_excel(path)
    return load_universe_from_file(cfg.universe)


def run_scan(cfg: ScanConfig, period: str = "1y", aggregate: str = "best") -> pd.DataFrame:
    tickers = load_universe(cfg)
    history = fetch_history(tickers, period=period, interval="1d")

    rows: List[Dict] = []
    for t, df in history.items():
        signals = evaluate_strategies(t, df, cfg.effective_strategies())
        if aggregate == "sum":
            # Summarize across strategies per ticker
            tot = 0.0
            metrics: Dict[str, float] = {}
            strat = "+".join([s.strategy for s in signals]) if signals else ""
            for s in signals:
                tot += s.score
                for k, v in s.metrics.items():
                    metrics[k] = v
            row = {"Ticker": t, "Strategy": strat or "none", "Score": float(tot)}
            row.update(metrics)
            rows.append(row)
        else:
            # best: keep each strategy row, later take best per ticker
            for sig in signals:
                row = {
                    "Ticker": sig.ticker,
                    "Strategy": sig.strategy,
                    "Score": sig.score,
                }
                row.update(sig.metrics)
                rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        if aggregate == "sum":
            agg = result.sort_values(["Score", "Ticker"], ascending=[False, True])
        else:
            # best per ticker
            agg = (
                result.sort_values(["Ticker", "Score"], ascending=[True, False])
                .groupby("Ticker", as_index=False)
                .first()
            )
            agg = agg.sort_values(["Score", "Ticker"], ascending=[False, True])
        return agg
    return result

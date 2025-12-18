#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# Ensure src/ is on sys.path when running from repo root
import sys

repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from smassist.settings import load_settings
from smassist.config import ScanConfig
from smassist.scanner import load_universe, run_scan
from smassist.analysis import fetch_news
from smassist.log import configure_logging


def safe_filename(ticker: str) -> str:
    return (
        ticker.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("^", "_")
        .upper()
    )


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def build_prices(tickers: list[str], out_dir: Path, period: str = "2y") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Batch download for speed
    raw = yf.download(
        tickers=tickers,
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True,
        progress=False,
    )

    if raw is None or getattr(raw, "empty", True):
        return

    # raw can be:
    # - MultiIndex columns when multiple tickers
    # - Single-ticker DataFrame otherwise
    if isinstance(raw.columns, pd.MultiIndex):
        for t in tickers:
            if t not in raw.columns.get_level_values(0):
                continue
            df = raw[t].dropna(how="all")
            if df.empty:
                continue
            df = df.reset_index().rename(columns={"Date": "Date"})
            keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
            df = df[keep]
            df.to_csv(out_dir / f"{safe_filename(t)}.csv", index=False)
    else:
        # single ticker
        t = tickers[0] if tickers else "TICKER"
        df = raw.dropna(how="all")
        if not df.empty:
            df = df.reset_index()
            keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
            df = df[keep]
            df.to_csv(out_dir / f"{safe_filename(t)}.csv", index=False)


def build_site() -> None:
    settings = load_settings(os.getenv("SMASSIST_CONFIG"))
    configure_logging(settings.log_level)

    site_root = repo_root / "site"
    data_dir = site_root / "data"
    prices_dir = data_dir / "prices"
    history_period = os.getenv("SMASSIST_HISTORY_PERIOD", "2y")

    # Universe + scan
    cfg = ScanConfig(
        universe=settings.scan.universe,
        strategies=list(settings.scan.strategies),
        lookback_days=settings.scan.lookback_days,
    )
    tickers = load_universe(cfg)

    scan_df = run_scan(cfg, period="1y", aggregate=settings.scan.aggregate)
    scan_df = scan_df.head(200) if scan_df is not None and not scan_df.empty else pd.DataFrame()

    # Good stocks JSON (for UI)
    good_rows = []
    if not scan_df.empty:
        for _, r in scan_df.iterrows():
            good_rows.append({k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()})

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    write_json(data_dir / "manifest.json", {
        "last_updated_utc": now_utc,
        "universe": "sp500" if settings.scan.universe == "sp500" else "custom",
        "tickers_count": len(tickers),
        "history_period": history_period,
    })
    write_json(data_dir / "good_stocks.json", {"generated_utc": now_utc, "rows": good_rows})

    # News: only top candidates to keep runtime reasonable
    news_items = []
    top_tickers = [r.get("Ticker") for r in good_rows[:25] if r.get("Ticker")]
    for t in top_tickers:
        for n in fetch_news(t, limit=3):
            news_items.append({
                "ticker": t,
                "title": n.title,
                "publisher": n.publisher,
                "link": n.link,
                "published": n.published,
            })
    write_json(data_dir / "news.json", {"generated_utc": now_utc, "items": news_items})

    # Prices for all tickers (2y)
    build_prices(tickers, prices_dir, period=history_period)


if __name__ == "__main__":
    build_site()

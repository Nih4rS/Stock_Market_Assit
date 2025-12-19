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
from smassist.india_universe import load_india_universe, companies_to_meta
from smassist.scanner import load_universe, run_scan
from smassist.news_rss import fetch_google_news
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


def safe_lower(s) -> str:
    try:
        return str(s).strip().lower()
    except Exception:
        return ""


def fetch_company_meta_yf(tickers: list[str]) -> dict[str, dict]:
    """Fetch light metadata for a small set of tickers."""
    out: dict[str, dict] = {}
    for t in tickers:
        try:
            info = yf.Ticker(t).info or {}
            market_cap = info.get("marketCap")
            market_price = info.get("regularMarketPrice") or info.get("currentPrice")
            out[t] = {
                "name": info.get("shortName") or info.get("longName") or None,
                "sector": info.get("sector") or None,
                "industry": info.get("industry") or None,
                "market_cap": market_cap if isinstance(market_cap, (int, float)) else None,
                "market_price": market_price if isinstance(market_price, (int, float)) else None,
            }
        except Exception:
            out[t] = {"name": None, "sector": None, "industry": None, "market_cap": None, "market_price": None}
    return out


def build_categories(good_rows: list[dict], meta: dict[str, dict]) -> dict:
    # Aggregate by sector/industry among candidate rows
    sector_counts: dict[str, int] = {}
    industry_counts: dict[str, int] = {}
    industry_by_sector: dict[str, dict[str, int]] = {}

    for r in good_rows:
        t = r.get("Ticker")
        m = meta.get(t, {}) if t else {}
        sector = m.get("sector") or "Uncategorized"
        industry = m.get("industry") or "Uncategorized"
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        industry_counts[industry] = industry_counts.get(industry, 0) + 1
        by_sector = industry_by_sector.setdefault(sector, {})
        by_sector[industry] = by_sector.get(industry, 0) + 1

    sectors = [{"name": k, "candidates": v} for k, v in sorted(sector_counts.items(), key=lambda x: (-x[1], x[0]))]
    industries = [{"name": k, "candidates": v} for k, v in sorted(industry_counts.items(), key=lambda x: (-x[1], x[0]))]

    sector_industries: dict[str, list[dict]] = {}
    for sector, ind_map in industry_by_sector.items():
        sector_industries[sector] = [
            {"name": k, "candidates": v}
            for k, v in sorted(ind_map.items(), key=lambda x: (-x[1], x[0]))
        ]

    return {"sectors": sectors, "industries": industries, "sector_industries": sector_industries}


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
    max_tickers_env = os.getenv("SMASSIST_SITE_MAX_TICKERS", "250")
    try:
        max_tickers = max(1, int(max_tickers_env))
    except Exception:
        max_tickers = 250

    # Universe + scan
    cfg = ScanConfig(
        universe=settings.scan.universe,
        strategies=list(settings.scan.strategies),
        lookback_days=settings.scan.lookback_days,
    )
    tickers = load_universe(cfg)

    # GitHub Pages builds must remain fast/reliable; scanning a full exchange list
    # can exceed CI time/data limits. Cap the universe deterministically.
    if len(tickers) > max_tickers:
        tickers = sorted(set(tickers))[:max_tickers]

    # If using NSE, also keep official company names for display.
    base_meta: dict[str, dict[str, str]] = {}
    if safe_lower(settings.scan.universe) == "nse":
        companies = load_india_universe("nse")
        base_meta = companies_to_meta(companies)

    scan_df = run_scan(cfg, period="1y", aggregate=settings.scan.aggregate)
    scan_df = scan_df.head(200) if scan_df is not None and not scan_df.empty else pd.DataFrame()

    # Good stocks JSON (for UI)
    good_rows = []
    if not scan_df.empty:
        for _, r in scan_df.iterrows():
            good_rows.append({k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()})

    # Candidate metadata (sector/industry) via yfinance for a small set
    candidate_tickers = [r.get("Ticker") for r in good_rows if r.get("Ticker")]
    yf_meta = fetch_company_meta_yf(candidate_tickers[:200])
    # Merge any official name metadata if available
    merged_meta: dict[str, dict] = {}
    for t in candidate_tickers[:200]:
        m = {}
        if t in base_meta:
            m.update(base_meta[t])
        m.update(yf_meta.get(t, {}))
        merged_meta[t] = m

    # Attach sector/industry fields to good rows for client-side filtering
    for r in good_rows:
        t = r.get("Ticker")
        m = merged_meta.get(t, {}) if t else {}
        if m.get("name") and not r.get("Name"):
            r["Name"] = m.get("name")
        if m.get("exchange") and not r.get("Exchange"):
            r["Exchange"] = m.get("exchange")
        if m.get("sector"):
            r["Sector"] = m.get("sector")
        if m.get("industry"):
            r["Industry"] = m.get("industry")
        if m.get("market_price") is not None:
            r["MarketPrice"] = float(m.get("market_price"))
        if m.get("market_cap") is not None:
            # Convert to crores (1 Cr = 10,000,000)
            r["MarketCapCr"] = float(m.get("market_cap")) / 1e7

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    write_json(data_dir / "manifest.json", {
        "last_updated_utc": now_utc,
        "universe": safe_lower(settings.scan.universe) or "custom",
        "tickers_count": len(tickers),
        "history_period": history_period,
    })
    write_json(data_dir / "good_stocks.json", {"generated_utc": now_utc, "rows": good_rows})

    # Categories (sector/industry) from candidates
    categories = build_categories(good_rows, merged_meta)
    write_json(data_dir / "categories.json", {"generated_utc": now_utc, **categories})

    # News: only top candidates to keep runtime reasonable
    news_items = []
    top_tickers = [r.get("Ticker") for r in good_rows[:25] if r.get("Ticker")]
    for t in top_tickers:
        # Use company name when available to improve RSS relevance
        name = (merged_meta.get(t, {}) or {}).get("name")
        q = f"{name or t} stock".strip()
        for n in fetch_google_news(q, limit=3):
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

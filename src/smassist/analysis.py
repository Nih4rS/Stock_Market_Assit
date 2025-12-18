from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from .data import rsi, sma


@dataclass
class NewsItem:
    title: str
    publisher: Optional[str]
    link: Optional[str]
    published: Optional[str]


def map_exchange_symbol(symbol: str, exchange: str) -> str:
    ex = (exchange or "").lower()
    if ex in ("nse", "ns"):
        return f"{symbol}.NS"
    if ex in ("bse", "bo"):
        return f"{symbol}.BO"
    return symbol


def fetch_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.rename(columns=str.title)
    return pd.DataFrame()


def fetch_fundamentals(ticker: str) -> Dict:
    try:
        t = yf.Ticker(ticker)
        info = getattr(t, "info", {}) or {}
        return info
    except Exception:
        return {}


def fetch_news(ticker: str, limit: int = 5) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        t = yf.Ticker(ticker)
        raw = getattr(t, "news", []) or []
        for n in raw[:limit]:
            ts = n.get("providerPublishTime")
            if ts:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                published = dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                published = None
            items.append(
                NewsItem(
                    title=n.get("title", ""),
                    publisher=n.get("publisher"),
                    link=n.get("link"),
                    published=published,
                )
            )
    except Exception:
        pass
    return items


def compute_technicals(df: pd.DataFrame) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if df.empty:
        return out
    close = df["Close"]
    out["close"] = float(close.iloc[-1])
    out["sma50"] = float(sma(close, 50).iloc[-1]) if len(close) >= 50 else float("nan")
    out["sma200"] = float(sma(close, 200).iloc[-1]) if len(close) >= 200 else float("nan")
    out["rsi14"] = float(rsi(close, 14).iloc[-1]) if len(close) >= 15 else float("nan")

    tail = close.tail(252)
    if not tail.empty:
        out["high_52w"] = float(tail.max())
        out["dist_52w_high"] = float((tail.max() - close.iloc[-1]) / tail.max()) if tail.max() else float("nan")
    else:
        out["high_52w"] = float("nan")
        out["dist_52w_high"] = float("nan")

    if len(close) >= 63:
        out["ret_3m"] = float(close.iloc[-1] / close.iloc[-63] - 1.0)
    if len(close) >= 21:
        out["ret_1m"] = float(close.iloc[-1] / close.iloc[-21] - 1.0)

    # ATR(14)
    if set(["High", "Low", "Close"]).issubset(df.columns):
        high = df["High"]
        low = df["Low"]
        prev_close = df["Close"].shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        out["atr14"] = float(atr.iloc[-1]) if not atr.empty else float("nan")
    else:
        out["atr14"] = float("nan")

    return out


def _to_float(val) -> float:
    try:
        return float(val)
    except Exception:
        return float("nan")


def classify_from_technicals(tech: Dict[str, float]) -> Tuple[str, str]:
    close = _to_float(tech.get("close", np.nan))
    s50 = _to_float(tech.get("sma50", np.nan))
    s200 = _to_float(tech.get("sma200", np.nan))
    dist_high = _to_float(tech.get("dist_52w_high", np.nan))
    ret_3m = _to_float(tech.get("ret_3m", np.nan))

    explanations: List[str] = []

    if not np.isnan(s50) and not np.isnan(s200) and not np.isnan(dist_high) and not np.isnan(ret_3m):
        trend = s50 > s200
        near_high = (1.0 - dist_high) <= 0.10  # within ~10% below 52w high
        strong_return = ret_3m >= 0.10
        if trend and near_high and strong_return:
            explanations.append("SMA50>SMA200, price near 52w high, 3m return ≥ 10% → momentum behavior.")
            return "Momentum", " ".join(explanations)

    # Mean-reversion if moving averages are flat-ish and RSI oscillatory
    if not np.isnan(s50) and not np.isnan(s200) and not np.isnan(close) and close != 0:
        ma_gap = abs(s50 - s200) / float(close)
        if ma_gap <= 0.03:
            explanations.append("SMA50 and SMA200 within 3% of price → flat regime; suitable for mean-reversion.")
            return "Mean-reversion", " ".join(explanations)

    # Fallback thematic/story-driven if data insufficient
    return "Thematic / story-driven", "Insufficient evidence for trend or mean reversion from price alone."


def format_analysis(ticker: str, exchange: str, df: pd.DataFrame, fundamentals: Dict, news: List[NewsItem]) -> str:
    tech = compute_technicals(df)
    category, cat_reason = classify_from_technicals(tech)

    # Extract fundamentals if available
    roe = fundamentals.get("returnOnEquity")
    roce = fundamentals.get("returnOnCapitalEmployed") or fundamentals.get("roc")  # rarely present
    debt_to_equity = fundamentals.get("debtToEquity")
    gross_margins = fundamentals.get("grossMargins")

    def fmt_pct(x: Optional[float]) -> str:
        return f"{x*100:.1f}%" if isinstance(x, (int, float)) and not np.isnan(x) else "unknown"

    tech_defs = (
        "RSI(14): momentum oscillator (overbought 70/oversold 30); "
        "SMA50/200: medium/long-term trend filters; "
        "ATR(14): volatility; 52-week high: highest close in ~252 sessions."
    )

    lines: List[str] = []
    lines.append(f"Stock: {ticker} ({exchange.upper()}) — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    # 1. Stock Classification
    lines.append("\n1. Stock Classification")
    lines.append(f"Primary: {category}")
    lines.append(f"Why: {cat_reason}")
    lines.append("Why not others: Doesn't show clear conflicting signals per current technicals.")

    # 2. Business Quality Check
    lines.append("\n2. Business Quality Check")
    lines.append(f"ROE: {fmt_pct(roe)}; ROCE: {fmt_pct(roce)}; Debt/Equity: {debt_to_equity if debt_to_equity is not None else 'unknown'}; Pricing power proxy (gross margin): {fmt_pct(gross_margins)}")
    lines.append("Typical expert constraints: ROCE ≥ 15%, ROE ≥ 15%, D/E ≤ 0.5, stable margins.")
    lines.append("Structural compounding: Unknown if key metrics are missing; require consistent ROCE/ROE over cycles.")

    # 3. Stock Behavior Analysis
    lines.append("\n3. Stock Behavior Analysis")
    lines.append(f"Triggers: Breaks near 52w high, SMA50>SMA200 with rising RSI (current RSI14: {tech.get('rsi14', float('nan')):.1f}).")
    lines.append("Drawdowns: Fails after extended runs (RSI>75) or breaks below SMA50 with expanding ATR.")
    lines.append("News/policy/sentiment: See recent items below; price can front-run fundamentals.")

    # 4. Suitable Strategy
    strat = "Momentum trade" if category == "Momentum" else ("Swing trade only" if category == "Mean-reversion" else "Avoid for now")
    lines.append("\n4. Suitable Strategy")
    lines.append(f"Recommended: {strat}")
    lines.append("Justification: Based on current technical regime and typical constraints.")

    # 5. How to Use This Stock
    lines.append("\n5. How to Use This Stock")
    if strat == "Momentum trade":
        lines.append("- Holding period: Weeks to a few months; ride trend.")
        lines.append("- Entry: Pullbacks to 20–50DMA with RSI 45–55; or on high-volume breakout.")
        lines.append("- Exit: Close below 50DMA or RSI breakdown < 50; partials into strength.")
    elif strat == "Swing trade only":
        lines.append("- Holding period: Days to weeks around mean.")
        lines.append("- Entry: RSI 30–35 at lower band in flat regime; targets near RSI 60–65.")
        lines.append("- Exit: Stop below recent swing low or ATR-based stop (1.5–2x ATR).")
    else:
        lines.append("- Not actionable until trend or base quality improves and liquidity confirms.")
    lines.append(f"Technical keywords: {tech_defs}")

    # 6. Risks and Invalidations
    lines.append("\n6. Risks and Invalidations")
    lines.append("- Regime change: Momentum flips to distribution (multiple high-volume down days).")
    lines.append("- Macro/policy shocks affecting sector multiples or liquidity.")
    lines.append("- Fundamental miss (margins/ROE compression), invalidating compounding narrative.")
    lines.append("Invalidate with: Close below SMA200 on volume, RSI < 40 for momentum, or earnings downgrades.")

    # 7. Investor Type Fit
    lines.append("\n7. Investor Type Fit")
    lines.append("- Suitable: Traders comfortable with disciplined entries/exits and risk controls.")
    lines.append("- Not suitable: Buy-and-forget investors without evidence of durable compounding metrics.")

    # Recent news
    lines.append("\nRecent News & Developments")
    if news:
        for n in news:
            lines.append(f"- {n.published or ''} {n.publisher or ''}: {n.title} {('(' + n.link + ')') if n.link else ''}")
    else:
        lines.append("- No recent items available from the data source.")

    return "\n".join(lines)


def analyze_stock(symbol: str, exchange: str = "nse", lookback_days: int = 252) -> str:
    ticker = map_exchange_symbol(symbol, exchange)
    period = "1y" if lookback_days <= 252 else "2y"
    df = fetch_price_history(ticker, period=period)
    fundamentals = fetch_fundamentals(ticker)
    news = fetch_news(ticker, limit=5)
    return format_analysis(ticker, exchange, df, fundamentals, news)

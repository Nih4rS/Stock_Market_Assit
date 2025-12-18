# Scoring & Constraints

This document explains how scores are computed and typical expert constraints used by the strategies.

## Modes
- `best` (default): Keep the highest-scoring strategy per ticker.
- `sum`: Sum scores across all triggered strategies for each ticker.

## Strategy Scores
- Golden Cross: Base 2.0, +1.0 bonus if recent cross in last 10 sessions.
- RSI Momentum: 1.5 if RSI(14) in 55–70 and rising (≥3 of last 5 diffs > 0).
- 52-week Breakout: 2.0 when within ~2% of 52w high.
- Volume Surge: 1.0 when 5-day avg volume ≥ 1.5x 20-day avg.

## Typical Expert Constraints (Filters)
- Trend health: `SMA50 / SMA200 ≥ 1.02` preferred.
- Momentum sanity: `RSI ≤ 72` at entry to avoid exhaustion.
- Breakout quality: Base duration ≥ 6 weeks; stop ≤ 7–10%.
- Liquidity: `AvgDailyDollarVolume ≥ $5–10M`.
- Regime filter: Prefer trades when `SMA50 ≥ SMA200`.

## Excel Output Columns
- Timestamp, Ticker, Strategy, Score, Close, RSI14, SMA50, SMA200, Dist_52wHigh, Vol5x20.

## Next Improvements
- Add sector/industry context and macro filters.
- Incorporate earnings dates and post-earnings drift effects.
- Parameterize thresholds per strategy via a config file.

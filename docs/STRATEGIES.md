# Core Strategies Playbook

This document defines the core trading strategies used by the scanner and single-stock analyzer. Each strategy lists: purpose, setup, exact rules, typical expert constraints, and exit/invalidations.

## Golden Cross (SMA50 > SMA200)
- Purpose: Capture medium-term uptrends as momentum strengthens.
- Setup: Simple moving averages of closing price, 50-day crossing above 200-day.
- Entry rules:
  - Primary: Daily close above both SMA50 and SMA200.
  - Optional: Enter on pullback to SMA50 within an uptrend.
- Expert constraints:
  - Trend health: `SMA50 / SMA200 >= 1.02` (≥2% spread) preferred.
  - Distance from 52-week high: `Close ≥ 0.9 * 52wHigh` (<10% below high) improves odds.
  - Liquidity: `AvgDailyDollarVolume ≥ $10M`.
- Exit/invalidations:
  - Close below SMA50 on above-average volume, or
  - Bearish cross (SMA50 < SMA200).

## RSI Momentum (RSI 55–70 with rising slope)
- Purpose: Participate in sustained momentum without chasing overbought extremes.
- Setup: RSI(14) between 55 and 70, rising over recent days.
- Entry rules:
  - RSI(14) in [55, 70] and 3-of-last-5 RSI changes > 0.
- Expert constraints:
  - Avoid overbought: `RSI ≤ 72` at entry.
  - Trend filter: `SMA50 ≥ SMA200`.
- Exit/invalidations:
  - RSI crosses below 50; or
  - Two consecutive closes below 20-day EMA.

## 52-Week Breakout
- Purpose: Ride post-breakout expansions when supply is thin above prior highs.
- Setup: Close within ~2% of 52-week high or making new highs.
- Entry rules:
  - New 52w high on volume ≥ 1.5x 20-day average, or
  - Rest/retest within 2–5 trading days after breakout.
- Expert constraints:
  - Base duration ≥ 6 weeks preferred.
  - Tight risk: `Stop ≤ 7–10%` below entry or below last swing low.
- Exit/invalidations:
  - Failed breakout: Close back into prior range on above-average volume.

## Volume Surge (Accumulation)
- Purpose: Detect institutional accumulation early.
- Setup: 5-day average volume ≥ 1.5x 20-day average.
- Entry rules:
  - Price holds above 20-day EMA on surge; or
  - Pocket pivot: Up day closing in top 30% of range on higher volume vs prior 10 days.
- Expert constraints:
  - Avoid illiquid names: `AvgDailyDollarVolume ≥ $5M`.
  - Context: Prefer within uptrends (SMA50 ≥ SMA200).
- Exit/invalidations:
  - Distribution: Two+ high-volume down days within 10 sessions.

## Mean-Reversion Pullback
- Purpose: Short-horizon swings around a flat to mildly trending mean.
- Setup: Price within ±5% of SMA200; RSI(14) oscillates 30–70.
- Entry rules:
  - Buy when RSI(14) ≤ 35 with price at lower Bollinger band; sell near RSI 60–65.
- Expert constraints:
  - Avoid strong trends (no trade if `|SMA50 - SMA200| / Price ≥ 5%`).
- Exit/invalidations:
  - Breakdown: Close below lower band with expanding ATR.

---

Definitions
- SMA/EMA: Simple/Exponential Moving Average of closing price.
- RSI(14): Relative Strength Index over 14 periods; 70 overbought, 30 oversold.
- 52-week high: Highest close of the last 252 trading days.
- ATR(14): Average True Range, a volatility measure.
- Pocket pivot: A specific high-volume accumulation day pattern.

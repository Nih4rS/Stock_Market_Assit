# Stock Market Assist

A professional-grade scanner that evaluates trading strategies across a broad universe (e.g., S&P 500) and updates an Excel playbook with the best candidates.

## Features
- Strategy-based scanning (Golden Cross, RSI momentum, 52-week breakout, Volume surge)
- Pluggable universe loader (S&P 500 via Wikipedia with offline fallback)
- Batch data fetching via `yfinance`
- Excel playbook updates into a `GoodStocks` sheet with key metrics
- CLI to run ad-hoc scans
- Optional GitHub Action to run on schedule and push playbook updates

## Quick Start

### 1) Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run a scan (dry run)
```bash
PYTHONPATH=src python -m smassist.cli scan --universe sp500 --dry-run
```

### 3) Single-stock analysis (NSE/BSE/US)
```bash
PYTHONPATH=src python -m smassist.cli analyze --ticker RELIANCE --exchange nse --days 252
# or write to a markdown file
PYTHONPATH=src python -m smassist.cli analyze --ticker TCS --exchange nse --output reports/TCS_analysis.md
```

### 4) Update your Excel playbook
By default this writes to `data/Top500_Sample_Strategy_Playbook.xlsx` (created if missing):
```bash
PYTHONPATH=src python -m smassist.cli scan --universe sp500 \
  --excel data/Top500_Sample_Strategy_Playbook.xlsx
Alternative (no PYTHONPATH):
```bash
python scripts/run_scan.py scan --universe sp500 --dry-run
python scripts/run_scan.py analyze --ticker RELIANCE --exchange nse
```

### 5) Scoring aggregation
Use `--aggregate best` (default) to keep the highest-scoring strategy per ticker.
Use `--aggregate sum` to sum scores across strategies for each ticker.
```

Options:
- `--universe sp500` (default) or `--universe-file tickers.txt`
- `--strategies golden_cross,rsi_momentum,breakout_52w,volume_surge`
- `--lookback 252` days of history
- `--dry-run` do not update Excel

### Output
- Excel sheet `GoodStocks` with columns: `Timestamp, Ticker, Strategy, Score, Close, RSI14, SMA50, SMA200, Dist_52wHigh, Vol5x20`
- Excel sheet `RunLog` with scan metadata

## Repository Layout
- `src/smassist/` core package
- `scripts/run_scan.py` simple runner
- `data/` Excel playbook location (gitignored)

## GitHub Actions (optional)
A workflow is provided in `.github/workflows/scan.yml`. To enable auto-updates:
1. Create a repository secret `ACTIONS_PAT` with a token that can push to `main`.
2. Adjust schedule as needed.
3. Ensure the Excel path is committed or writable at `data/Top500_Sample_Strategy_Playbook.xlsx`.

## Notes
- This project uses public data via `yfinance` and may be rate-limited.
- Strategy logic is educational and not investment advice.

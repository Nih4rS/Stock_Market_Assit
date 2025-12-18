from __future__ import annotations

import argparse
from pathlib import Path

from .log import configure_logging
from .settings import load_settings
from .config import ScanConfig
from .scanner import run_scan
from .excel_io import write_good_stocks


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("smassist")
    p.add_argument("--config", default=None, help="Path to settings TOML (defaults to settings.toml if present)")
    p.add_argument("--log-level", default=None, help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    sub = p.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan", help="Run strategy scan and optionally write Excel")
    scan.add_argument("--universe", default=None, help="'sp500' or path to tickers file (default from settings)")
    scan.add_argument("--strategies", default=None, help="Comma-separated strategy names (default from settings)")
    scan.add_argument("--lookback", type=int, default=None, help="Lookback days (default from settings)")
    scan.add_argument("--excel", default=None, help="Excel path (default from settings)")
    scan.add_argument("--aggregate", default=None, choices=["best", "sum"], help="Score aggregation mode (default from settings)")
    scan.add_argument("--dry-run", action="store_true")

    analyze = sub.add_parser("analyze", help="Single-stock analysis (NSE/BSE/US)")
    analyze.add_argument("--ticker", required=True, help="Base symbol without suffix, e.g., RELIANCE")
    analyze.add_argument("--exchange", default="nse", choices=["nse", "bse", "us"], help="Exchange for the symbol")
    analyze.add_argument("--days", type=int, default=252, help="Lookback days for technicals")
    analyze.add_argument("--output", default=None, help="Optional path to write markdown analysis")

    diag = sub.add_parser("diag", help="Run environment/data diagnostics")
    diag.add_argument("--excel", default=None, help="Excel path to validate")
    return p


def main(argv=None) -> int:
    p = build_parser()
    args = p.parse_args(argv)

    # Configure basic logging first (env-aware). We'll refine after config is loaded.
    configure_logging(args.log_level)

    settings = load_settings(args.config)
    if args.log_level is None:
        configure_logging(settings.log_level)

    if args.cmd == "scan":
        # CLI overrides settings; otherwise fall back to config/env.
        universe = args.universe or settings.scan.universe
        aggregate = args.aggregate or settings.scan.aggregate
        lookback = args.lookback or settings.scan.lookback_days
        strategies = (
            args.strategies.split(",")
            if args.strategies
            else list(settings.scan.strategies)
        )
        excel_path = args.excel or settings.scan.excel_path

        cfg = ScanConfig(universe=universe, strategies=strategies, lookback_days=lookback)
        df = run_scan(cfg, period="1y", aggregate=aggregate)
        if df is None or df.empty:
            print("No candidates found.")
            return 0
        print(df.head(25).to_string(index=False))
        if not args.dry_run:
            write_good_stocks(excel_path, df)
            print(f"Updated Excel: {excel_path}")
        else:
            print("Dry run: not writing Excel.")
        return 0

    if args.cmd == "analyze":
        from .analysis import analyze_stock
        md = analyze_stock(args.ticker, args.exchange, args.days)
        if args.output:
            out = Path(args.output)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(md, encoding="utf-8")
            print(f"Written analysis to {out}")
        else:
            print(md)
        return 0

    if args.cmd == "diag":
        from .diagnostics import run_diagnostics, format_diagnostics
        results = run_diagnostics(excel_path=args.excel)
        print(format_diagnostics(results))
        return 0 if all(r.ok for r in results) else 2

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

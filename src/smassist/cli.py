from __future__ import annotations

import argparse
from pathlib import Path

from .config import ScanConfig
from .scanner import run_scan
from .excel_io import write_good_stocks


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("smassist")
    sub = p.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan", help="Run strategy scan and optionally write Excel")
    scan.add_argument("--universe", default="sp500", help="'sp500' or path to tickers file")
    scan.add_argument("--strategies", default=None, help="Comma-separated strategy names")
    scan.add_argument("--lookback", type=int, default=252)
    scan.add_argument("--excel", default="data/Top500_Sample_Strategy_Playbook.xlsx")
    scan.add_argument("--aggregate", default="best", choices=["best", "sum"], help="Score aggregation mode")
    scan.add_argument("--dry-run", action="store_true")

    analyze = sub.add_parser("analyze", help="Single-stock analysis (NSE/BSE/US)")
    analyze.add_argument("--ticker", required=True, help="Base symbol without suffix, e.g., RELIANCE")
    analyze.add_argument("--exchange", default="nse", choices=["nse", "bse", "us"], help="Exchange for the symbol")
    analyze.add_argument("--days", type=int, default=252, help="Lookback days for technicals")
    analyze.add_argument("--output", default=None, help="Optional path to write markdown analysis")
    return p


def main(argv=None) -> int:
    p = build_parser()
    args = p.parse_args(argv)

    if args.cmd == "scan":
        strategies = args.strategies.split(",") if args.strategies else None
        cfg = ScanConfig(universe=args.universe, strategies=strategies, lookback_days=args.lookback)
        df = run_scan(cfg, period="1y", aggregate=args.aggregate)
        if df is None or df.empty:
            print("No candidates found.")
            return 0
        print(df.head(25).to_string(index=False))
        if not args.dry_run:
            write_good_stocks(args.excel, df)
            print(f"Updated Excel: {args.excel}")
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

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

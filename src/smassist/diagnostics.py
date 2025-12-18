from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .data import fetch_history
from .settings import load_settings
from .config import ScanConfig
from .scanner import load_universe


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _check_import(module: str) -> CheckResult:
    try:
        importlib.import_module(module)
        return CheckResult(f"import:{module}", True, "ok")
    except Exception as e:
        return CheckResult(f"import:{module}", False, str(e))


def _check_excel_path(path: str) -> CheckResult:
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        testfile = p.parent / ".smassist_write_test"
        testfile.write_text("ok", encoding="utf-8")
        testfile.unlink(missing_ok=True)  # py3.8+; safe here
        return CheckResult("excel:path", True, str(p))
    except Exception as e:
        return CheckResult("excel:path", False, f"{p}: {e}")


def _check_universe() -> CheckResult:
    try:
        settings = load_settings(os.getenv("SMASSIST_CONFIG"))
        cfg = ScanConfig(
            universe=settings.scan.universe,
            strategies=[],
            lookback_days=settings.scan.lookback_days,
        )
        tickers = load_universe(cfg)
        return CheckResult(
            "universe:configured",
            bool(tickers),
            f"universe={settings.scan.universe}, tickers={len(tickers)}",
        )
    except Exception as e:
        return CheckResult("universe:configured", False, str(e))


def _check_price_fetch(ticker: str = "AAPL") -> CheckResult:
    try:
        hist = fetch_history([ticker], period="3mo")
        df = hist.get(ticker)
        ok = df is not None and not df.empty
        return CheckResult("prices:yfinance", ok, f"ticker={ticker}, rows={0 if df is None else len(df)}")
    except Exception as e:
        return CheckResult("prices:yfinance", False, str(e))


def run_diagnostics(excel_path: Optional[str] = None) -> List[CheckResult]:
    results: List[CheckResult] = []

    # Imports
    for mod in ("pandas", "numpy", "yfinance", "openpyxl", "requests"):
        results.append(_check_import(mod))

    # Excel path
    excel_path = excel_path or os.getenv("SMASSIST_EXCEL") or "data/Top500_Sample_Strategy_Playbook.xlsx"
    results.append(_check_excel_path(excel_path))

    # Universe and prices
    results.append(_check_universe())
    results.append(_check_price_fetch("AAPL"))

    return results


def format_diagnostics(results: List[CheckResult]) -> str:
    lines = []
    ok_all = all(r.ok for r in results)
    lines.append(f"Diagnostics: {'PASS' if ok_all else 'FAIL'}")
    for r in results:
        status = "OK" if r.ok else "FAIL"
        lines.append(f"- {status:4} {r.name}: {r.detail}")
    return "\n".join(lines)

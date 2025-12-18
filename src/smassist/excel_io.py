from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook, load_workbook


GOOD_SHEET = "GoodStocks"
RUNLOG_SHEET = "RunLog"


def _ensure_workbook(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = GOOD_SHEET
        wb.create_sheet(RUNLOG_SHEET)
        wb.save(path)


def write_good_stocks(excel_path: str | Path, df: pd.DataFrame) -> None:
    path = Path(excel_path)
    _ensure_workbook(path)

    wb = load_workbook(path)
    if GOOD_SHEET in wb.sheetnames:
        ws = wb[GOOD_SHEET]
        wb.remove(ws)
        wb.create_sheet(GOOD_SHEET)
    else:
        wb.create_sheet(GOOD_SHEET)
    ws = wb[GOOD_SHEET]

    # Write header
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    columns = [
        "Timestamp",
        "Ticker",
        "Strategy",
        "Score",
        "Close",
        "RSI14",
        "SMA50",
        "SMA200",
        "Dist_52wHigh",
        "Vol5x20",
    ]
    ws.append(columns)

    # Write rows
    for _, row in df.iterrows():
        ws.append([
            timestamp,
            row.get("Ticker"),
            row.get("Strategy"),
            float(row.get("Score", 0.0)) if pd.notna(row.get("Score")) else None,
            float(row.get("Close")) if pd.notna(row.get("Close")) else None,
            float(row.get("RSI14")) if pd.notna(row.get("RSI14")) else None,
            float(row.get("SMA50")) if pd.notna(row.get("SMA50")) else None,
            float(row.get("SMA200")) if pd.notna(row.get("SMA200")) else None,
            float(row.get("Dist_52wHigh")) if pd.notna(row.get("Dist_52wHigh")) else None,
            float(row.get("Vol5x20")) if pd.notna(row.get("Vol5x20")) else None,
        ])

    # RunLog
    if RUNLOG_SHEET not in wb.sheetnames:
        wb.create_sheet(RUNLOG_SHEET)
    log = wb[RUNLOG_SHEET]
    if log.max_row == 1 and log.max_column == 1 and log.cell(1, 1).value is None:
        log.append(["Timestamp", "Candidates", "Notes"])
    log.append([timestamp, int(len(df)), "Auto scan update"])

    wb.save(path)

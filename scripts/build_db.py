#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import requests

# Ensure src/ is on sys.path when running from repo root
import sys

repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from smassist.database import StockUpsert, connect_db, init_db, seed_taxonomy_from_json, upsert_stock, upsert_industry_mapping


NSE_EQUITY_L_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
BSE_SCRIP_MASTER_URL = "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?segment=Equity&status=&Group=&Scripcode="


def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def download_text(session: requests.Session, url: str, *, referer: Optional[str] = None, timeout: int = 30) -> str:
    headers = {}
    if referer:
        headers["Referer"] = referer
    resp = session.get(url, timeout=timeout, headers=headers)
    resp.raise_for_status()
    return resp.text


def read_csv_rows(text: str) -> Iterable[Dict[str, str]]:
    # Handles BOM and common bad encodings gracefully
    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    for row in reader:
        out: Dict[str, str] = {}
        for k, v in (row or {}).items():
            if k is None:
                # Some CSVs have trailing commas causing None keys.
                continue
            key = k.strip()
            if not key:
                continue
            out[key] = v.strip() if isinstance(v, str) else v
        yield out


def ingest_nse_equity_list(conn, session: requests.Session, *, only_series_eq: bool = True) -> Tuple[int, int]:
    text = download_text(session, NSE_EQUITY_L_URL)

    inserted = 0
    updated = 0

    for row in read_csv_rows(text):
        symbol = (row.get("SYMBOL") or "").strip()
        name = (row.get("NAME OF COMPANY") or "").strip()
        series = (row.get(" SERIES") or row.get("SERIES") or "").strip()
        isin = (row.get(" ISIN NUMBER") or row.get("ISIN NUMBER") or "").strip()
        if not symbol:
            continue
        if only_series_eq and series and series != "EQ":
            continue

        before = conn.total_changes
        upsert_stock(
            conn,
            StockUpsert(
                symbol_nse=symbol,
                company_name=name or None,
                isin=isin or None,
                nse_series=series or None,
                status="active",
            ),
        )
        after = conn.total_changes
        if after > before:
            # Heuristic: total_changes increments for inserts/updates; we can't easily distinguish.
            updated += 1

    conn.commit()
    # We don't precisely know inserted vs updated without extra queries; keep inserted=0 for now.
    return inserted, updated


def ingest_bse_scrip_master(conn, session: requests.Session) -> Tuple[int, int]:
    text = download_text(session, BSE_SCRIP_MASTER_URL, referer="https://www.bseindia.com/corporates/List_Scrips.html")

    inserted = 0
    updated = 0

    for row in read_csv_rows(text):
        scrip_code = (row.get("Security Code") or "").strip()
        issuer_name = (row.get("Issuer Name") or "").strip()
        security_id = (row.get("Security Id") or "").strip()
        status = (row.get("Status") or "").strip()
        isin = (row.get("ISIN No") or "").strip()

        if not security_id and not isin:
            continue

        before = conn.total_changes
        upsert_stock(
            conn,
            StockUpsert(
                symbol_bse=security_id or None,
                company_name=issuer_name or None,
                isin=isin or None,
                bse_scrip_code=scrip_code or None,
                status=status.lower() if status else None,
            ),
        )
        after = conn.total_changes
        if after > before:
            updated += 1

    conn.commit()
    return inserted, updated


def ingest_mapping_csv(conn, mapping_path: Path) -> int:
    if not mapping_path.exists():
        return 0

    text = mapping_path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return 0

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    n = 0
    for row in reader:
        source = (row.get("source") or "").strip().lower()
        source_industry = (row.get("source_industry") or "").strip()
        sector = (row.get("sector") or "").strip()
        subsector = (row.get("subsector") or "").strip() or None
        if not (source and source_industry and sector):
            continue
        upsert_industry_mapping(
            conn,
            source=source,
            source_industry=source_industry,
            sector_name=sector,
            subsector_name=subsector,
        )
        n += 1

    conn.commit()
    return n


def main(argv=None) -> int:
    p = argparse.ArgumentParser("build_db")
    p.add_argument("--db", default="data/smassist.db", help="SQLite database path")
    p.add_argument(
        "--taxonomy",
        default=str(repo_root / "src" / "smassist" / "resources" / "groww_taxonomy.json"),
        help="Path to taxonomy JSON (sectors/subsectors)",
    )
    p.add_argument(
        "--mapping",
        default=str(repo_root / "config" / "industry_mapping.csv"),
        help="Industry→sector/subsector mapping CSV",
    )
    p.add_argument("--skip-nse", action="store_true")
    p.add_argument("--skip-bse", action="store_true")
    args = p.parse_args(argv)

    session = http_session()
    with connect_db(args.db) as conn:
        init_db(conn)
        seed_taxonomy_from_json(conn, args.taxonomy)

        mapped = ingest_mapping_csv(conn, Path(args.mapping))

        if not args.skip_nse:
            ingest_nse_equity_list(conn, session)
        if not args.skip_bse:
            ingest_bse_scrip_master(conn, session)

        stocks = conn.execute("SELECT COUNT(*) AS n FROM stocks").fetchone()["n"]
        sectors = conn.execute("SELECT COUNT(*) AS n FROM sectors").fetchone()["n"]
        subsectors = conn.execute("SELECT COUNT(*) AS n FROM subsectors").fetchone()["n"]

    print(f"DB ready: {args.db}")
    print(f"- stocks: {stocks}")
    print(f"- sectors: {sectors}")
    print(f"- subsectors: {subsectors}")
    print(f"- mapping rows: {mapped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

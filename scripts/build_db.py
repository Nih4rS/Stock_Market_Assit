#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import requests

# Ensure src/ is on sys.path when running from repo root
import sys

repo_root = Path(__file__).resolve().parents[1]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from smassist.database import (
    StockUpsert,
    connect_db,
    ensure_universe,
    export_universe_snapshot_csv,
    finish_ingest_run,
    init_db,
    record_ingest_source,
    seed_taxonomy_from_json,
    sha256_text,
    start_ingest_run,
    upsert_industry_mapping,
    upsert_stock,
    upsert_universe_membership,
    utc_now_str,
)


NSE_EQUITY_L_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
BSE_SCRIP_MASTER_URL = "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?segment=Equity&status=&Group=&Scripcode="

NSE_UNIVERSE_CODE = "nse_eq"
BSE_UNIVERSE_CODE = "bse_eq"


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


def read_snapshot_csv(path: Path) -> Iterable[Dict[str, str]]:
    if not path.exists():
        return []
    reader = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))
    out: list[Dict[str, str]] = []
    for row in reader:
        clean: Dict[str, str] = {}
        for k, v in (row or {}).items():
            if k is None:
                continue
            kk = k.strip()
            if not kk:
                continue
            clean[kk] = v.strip() if isinstance(v, str) else v
        out.append(clean)
    return out


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


def ingest_nse_equity_list(conn, session: requests.Session, *, run_id: int, only_series_eq: bool = True) -> Tuple[int, int]:
    fetched_utc = utc_now_str()
    text = download_text(session, NSE_EQUITY_L_URL)
    record_ingest_source(
        conn,
        run_id=run_id,
        source_code="nse_equity_l",
        url=NSE_EQUITY_L_URL,
        fetched_utc=fetched_utc,
        http_status=200,
        content_sha256=sha256_text(text),
        row_count=None,
        error=None,
    )

    inserted = 0
    updated = 0

    universe_id = ensure_universe(conn, NSE_UNIVERSE_CODE, description="NSE listed equities (EQUITY_L.csv, series=EQ)")
    row_count = 0

    for row in read_csv_rows(text):
        symbol = (row.get("SYMBOL") or "").strip()
        name = (row.get("NAME OF COMPANY") or "").strip()
        series = (row.get(" SERIES") or row.get("SERIES") or "").strip()
        isin = (row.get(" ISIN NUMBER") or row.get("ISIN NUMBER") or "").strip()
        if not symbol:
            continue
        if only_series_eq and series and series != "EQ":
            continue

        row_count += 1

        before = conn.total_changes
        stock_id = upsert_stock(
            conn,
            StockUpsert(
                symbol_nse=symbol,
                company_name=name or None,
                isin=isin or None,
                nse_series=series or None,
                status="active",
            ),
        )
        upsert_universe_membership(conn, universe_id=universe_id, stock_id=stock_id, included=True)
        after = conn.total_changes
        if after > before:
            # Heuristic: total_changes increments for inserts/updates; we can't easily distinguish.
            updated += 1

    conn.commit()
    record_ingest_source(
        conn,
        run_id=run_id,
        source_code="nse_equity_l",
        url=NSE_EQUITY_L_URL,
        fetched_utc=fetched_utc,
        http_status=200,
        content_sha256=sha256_text(text),
        row_count=row_count,
        error=None,
    )
    # We don't precisely know inserted vs updated without extra queries; keep inserted=0 for now.
    return inserted, updated


def ingest_bse_scrip_master(conn, session: requests.Session, *, run_id: int) -> Tuple[int, int]:
    fetched_utc = utc_now_str()
    text = download_text(session, BSE_SCRIP_MASTER_URL, referer="https://www.bseindia.com/corporates/List_Scrips.html")
    record_ingest_source(
        conn,
        run_id=run_id,
        source_code="bse_scrip_master",
        url=BSE_SCRIP_MASTER_URL,
        fetched_utc=fetched_utc,
        http_status=200,
        content_sha256=sha256_text(text),
        row_count=None,
        error=None,
    )

    inserted = 0
    updated = 0

    universe_id = ensure_universe(conn, BSE_UNIVERSE_CODE, description="BSE scrip master (segment=Equity)")
    row_count = 0

    for row in read_csv_rows(text):
        scrip_code = (row.get("Security Code") or "").strip()
        issuer_name = (row.get("Issuer Name") or "").strip()
        security_id = (row.get("Security Id") or "").strip()
        status = (row.get("Status") or "").strip()
        isin = (row.get("ISIN No") or "").strip()

        if not security_id and not isin:
            continue

        row_count += 1

        before = conn.total_changes
        stock_id = upsert_stock(
            conn,
            StockUpsert(
                symbol_bse=security_id or None,
                company_name=issuer_name or None,
                isin=isin or None,
                bse_scrip_code=scrip_code or None,
                status=status.lower() if status else None,
            ),
        )
        upsert_universe_membership(conn, universe_id=universe_id, stock_id=stock_id, included=True)
        after = conn.total_changes
        if after > before:
            updated += 1

    conn.commit()
    record_ingest_source(
        conn,
        run_id=run_id,
        source_code="bse_scrip_master",
        url=BSE_SCRIP_MASTER_URL,
        fetched_utc=fetched_utc,
        http_status=200,
        content_sha256=sha256_text(text),
        row_count=row_count,
        error=None,
    )
    return inserted, updated


def ingest_from_snapshots(conn, *, snapshot_dir: Path) -> Tuple[int, int]:
    """Offline rebuild path using committed ticker snapshot CSVs."""
    inserted = 0
    updated = 0

    nse_path = snapshot_dir / f"{NSE_UNIVERSE_CODE}.csv"
    bse_path = snapshot_dir / f"{BSE_UNIVERSE_CODE}.csv"

    if nse_path.exists():
        universe_id = ensure_universe(conn, NSE_UNIVERSE_CODE, description="NSE listed equities (snapshot)")
        for row in read_snapshot_csv(nse_path):
            before = conn.total_changes
            stock_id = upsert_stock(
                conn,
                StockUpsert(
                    symbol_nse=(row.get("symbol_nse") or "").strip() or None,
                    company_name=(row.get("company_name") or "").strip() or None,
                    isin=(row.get("isin") or "").strip() or None,
                    nse_series=(row.get("nse_series") or "").strip() or None,
                    status=(row.get("status") or "").strip() or None,
                ),
            )
            upsert_universe_membership(conn, universe_id=universe_id, stock_id=stock_id, included=True)
            after = conn.total_changes
            if after > before:
                updated += 1

    if bse_path.exists():
        universe_id = ensure_universe(conn, BSE_UNIVERSE_CODE, description="BSE scrip master (snapshot)")
        for row in read_snapshot_csv(bse_path):
            before = conn.total_changes
            stock_id = upsert_stock(
                conn,
                StockUpsert(
                    symbol_bse=(row.get("symbol_bse") or "").strip() or None,
                    company_name=(row.get("company_name") or "").strip() or None,
                    isin=(row.get("isin") or "").strip() or None,
                    bse_scrip_code=(row.get("bse_scrip_code") or "").strip() or None,
                    status=(row.get("status") or "").strip() or None,
                ),
            )
            upsert_universe_membership(conn, universe_id=universe_id, stock_id=stock_id, included=True)
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
    p.add_argument(
        "--snapshot-dir",
        default=str(repo_root / "config" / "ticker_snapshots"),
        help="Directory for committed ticker snapshot CSVs",
    )
    p.add_argument(
        "--offline",
        action="store_true",
        help="Do not hit network; rebuild using ticker snapshots if present",
    )
    args = p.parse_args(argv)

    session = http_session()
    with connect_db(args.db) as conn:
        init_db(conn)
        seed_taxonomy_from_json(conn, args.taxonomy)

        run_id = start_ingest_run(
            conn,
            command=" ".join((argv or sys.argv)[1:]) if (argv or sys.argv) else None,
            git_sha=None,
        )

        mapped = ingest_mapping_csv(conn, Path(args.mapping))

        notes: list[str] = []
        status = "success"
        try:
            if args.offline:
                ingest_from_snapshots(conn, snapshot_dir=Path(args.snapshot_dir))
                notes.append("offline rebuild from snapshots")
            else:
                if not args.skip_nse:
                    ingest_nse_equity_list(conn, session, run_id=run_id)
                if not args.skip_bse:
                    ingest_bse_scrip_master(conn, session, run_id=run_id)
        except Exception as e:
            status = "failed"
            notes.append(f"ingest failed: {e}")
            # Fall back to snapshots if available.
            ingest_from_snapshots(conn, snapshot_dir=Path(args.snapshot_dir))
            notes.append("fallback rebuild from snapshots")

        # Always export snapshots after a successful ingest/fallback so they stay fresh.
        snap_dir = Path(args.snapshot_dir)
        snap_dir.mkdir(parents=True, exist_ok=True)
        nse_n = export_universe_snapshot_csv(conn, universe_code=NSE_UNIVERSE_CODE, out_path=snap_dir / f"{NSE_UNIVERSE_CODE}.csv")
        bse_n = export_universe_snapshot_csv(conn, universe_code=BSE_UNIVERSE_CODE, out_path=snap_dir / f"{BSE_UNIVERSE_CODE}.csv")
        notes.append(f"snapshot export: {NSE_UNIVERSE_CODE}={nse_n}, {BSE_UNIVERSE_CODE}={bse_n}")

        finish_ingest_run(conn, run_id=run_id, status=status, notes="; ".join(notes) if notes else None)
        conn.commit()

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

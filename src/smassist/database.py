from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stocks (
  stock_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol_nse    TEXT,
  symbol_bse    TEXT,
  company_name  TEXT,
  isin          TEXT,
  nse_series    TEXT,
  bse_scrip_code TEXT,
  status        TEXT,
  updated_utc   TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_stocks_isin ON stocks(isin) WHERE isin IS NOT NULL AND isin <> '';
CREATE UNIQUE INDEX IF NOT EXISTS ux_stocks_symbol_nse ON stocks(symbol_nse) WHERE symbol_nse IS NOT NULL AND symbol_nse <> '';
CREATE UNIQUE INDEX IF NOT EXISTS ux_stocks_symbol_bse ON stocks(symbol_bse) WHERE symbol_bse IS NOT NULL AND symbol_bse <> '';

CREATE TABLE IF NOT EXISTS sectors (
  sector_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  sector_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS subsectors (
  subsector_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  sector_id      INTEGER NOT NULL,
  subsector_name TEXT NOT NULL,
  UNIQUE(sector_id, subsector_name),
  FOREIGN KEY(sector_id) REFERENCES sectors(sector_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stock_sector_map (
  stock_id     INTEGER NOT NULL,
  sector_id    INTEGER NOT NULL,
  subsector_id INTEGER,
  PRIMARY KEY(stock_id),
  FOREIGN KEY(stock_id) REFERENCES stocks(stock_id) ON DELETE CASCADE,
  FOREIGN KEY(sector_id) REFERENCES sectors(sector_id) ON DELETE CASCADE,
  FOREIGN KEY(subsector_id) REFERENCES subsectors(subsector_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS indices (
  index_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  index_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS stock_index_map (
  stock_id INTEGER NOT NULL,
  index_id INTEGER NOT NULL,
  PRIMARY KEY(stock_id, index_id),
  FOREIGN KEY(stock_id) REFERENCES stocks(stock_id) ON DELETE CASCADE,
  FOREIGN KEY(index_id) REFERENCES indices(index_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS industry_mapping (
  mapping_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT NOT NULL,
  source_industry TEXT NOT NULL,
  sector_name     TEXT NOT NULL,
  subsector_name  TEXT,
  UNIQUE(source, source_industry)
);
"""


@dataclass
class StockUpsert:
    symbol_nse: Optional[str] = None
    symbol_bse: Optional[str] = None
    company_name: Optional[str] = None
    isin: Optional[str] = None
    nse_series: Optional[str] = None
    bse_scrip_code: Optional[str] = None
    status: Optional[str] = None


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def _norm(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _find_stock_id(conn: sqlite3.Connection, *, isin: Optional[str], symbol_nse: Optional[str], symbol_bse: Optional[str]) -> Optional[int]:
    if isin:
        row = conn.execute("SELECT stock_id FROM stocks WHERE isin = ?", (isin,)).fetchone()
        if row:
            return int(row["stock_id"])
    if symbol_nse:
        row = conn.execute("SELECT stock_id FROM stocks WHERE symbol_nse = ?", (symbol_nse,)).fetchone()
        if row:
            return int(row["stock_id"])
    if symbol_bse:
        row = conn.execute("SELECT stock_id FROM stocks WHERE symbol_bse = ?", (symbol_bse,)).fetchone()
        if row:
            return int(row["stock_id"])
    return None


def _value_owned_by_other(
    conn: sqlite3.Connection,
    *,
    column: str,
    value: Optional[str],
    current_stock_id: int,
) -> bool:
    if not value:
        return False
    row = conn.execute(
        f"SELECT stock_id FROM stocks WHERE {column} = ? AND stock_id <> ?",
        (value, current_stock_id),
    ).fetchone()
    return bool(row)


def upsert_stock(conn: sqlite3.Connection, s: StockUpsert) -> int:
    isin = _norm(s.isin)
    symbol_nse = _norm(s.symbol_nse)
    symbol_bse = _norm(s.symbol_bse)
    company_name = _norm(s.company_name)
    nse_series = _norm(s.nse_series)
    bse_scrip_code = _norm(s.bse_scrip_code)
    status = _norm(s.status)

    stock_id = _find_stock_id(conn, isin=isin, symbol_nse=symbol_nse, symbol_bse=symbol_bse)
    now = utc_now_str()

    if stock_id is None:
        cur = conn.execute(
            """
            INSERT INTO stocks(symbol_nse, symbol_bse, company_name, isin, nse_series, bse_scrip_code, status, updated_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (symbol_nse, symbol_bse, company_name, isin, nse_series, bse_scrip_code, status, now),
        )
        if cur.lastrowid is None:
            raise RuntimeError("Insert failed: no lastrowid")
        return int(cur.lastrowid)

    # Prevent UNIQUE constraint failures when a new value is already owned by another row.
    if _value_owned_by_other(conn, column="symbol_nse", value=symbol_nse, current_stock_id=stock_id):
        symbol_nse = None
    if _value_owned_by_other(conn, column="symbol_bse", value=symbol_bse, current_stock_id=stock_id):
        symbol_bse = None
    if _value_owned_by_other(conn, column="isin", value=isin, current_stock_id=stock_id):
        isin = None

    conn.execute(
        """
        UPDATE stocks
           SET symbol_nse = COALESCE(?, symbol_nse),
               symbol_bse = COALESCE(?, symbol_bse),
               company_name = COALESCE(?, company_name),
               isin = COALESCE(?, isin),
               nse_series = COALESCE(?, nse_series),
               bse_scrip_code = COALESCE(?, bse_scrip_code),
               status = COALESCE(?, status),
               updated_utc = ?
         WHERE stock_id = ?
        """,
        (symbol_nse, symbol_bse, company_name, isin, nse_series, bse_scrip_code, status, now, stock_id),
    )
    return int(stock_id)


def ensure_sector(conn: sqlite3.Connection, sector_name: str) -> int:
    sector_name = str(sector_name).strip()
    row = conn.execute("SELECT sector_id FROM sectors WHERE sector_name = ?", (sector_name,)).fetchone()
    if row:
        return int(row["sector_id"])
    cur = conn.execute("INSERT INTO sectors(sector_name) VALUES (?)", (sector_name,))
    if cur.lastrowid is None:
        raise RuntimeError("Insert failed: no lastrowid")
    return int(cur.lastrowid)


def ensure_subsector(conn: sqlite3.Connection, sector_id: int, subsector_name: str) -> int:
    subsector_name = str(subsector_name).strip()
    row = conn.execute(
        "SELECT subsector_id FROM subsectors WHERE sector_id = ? AND subsector_name = ?",
        (sector_id, subsector_name),
    ).fetchone()
    if row:
        return int(row["subsector_id"])
    cur = conn.execute(
        "INSERT INTO subsectors(sector_id, subsector_name) VALUES (?, ?)",
        (sector_id, subsector_name),
    )
    if cur.lastrowid is None:
        raise RuntimeError("Insert failed: no lastrowid")
    return int(cur.lastrowid)


def seed_taxonomy_from_json(conn: sqlite3.Connection, taxonomy_path: str | Path) -> None:
    taxonomy = json.loads(Path(taxonomy_path).read_text(encoding="utf-8"))
    sectors = taxonomy.get("sectors", {})
    for sector_name, subsectors in sectors.items():
        sector_id = ensure_sector(conn, sector_name)
        for sub in subsectors or []:
            ensure_subsector(conn, sector_id, sub)
    conn.commit()


def upsert_industry_mapping(
    conn: sqlite3.Connection,
    *,
    source: str,
    source_industry: str,
    sector_name: str,
    subsector_name: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO industry_mapping(source, source_industry, sector_name, subsector_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(source, source_industry) DO UPDATE SET
          sector_name=excluded.sector_name,
          subsector_name=excluded.subsector_name
        """,
        (source, source_industry, sector_name, subsector_name),
    )


def apply_industry_mapping(conn: sqlite3.Connection) -> int:
    """Apply mapping table to build stock_sector_map.

    This is a starter implementation that maps by ISIN only.
    You can extend it once we ingest industry labels reliably.
    """
    # Placeholder: no-op for now (industry fields are not persisted yet)
    return 0

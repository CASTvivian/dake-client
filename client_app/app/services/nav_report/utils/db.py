import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = os.environ.get("DATA_DIR") or str(Path(__file__).resolve().parents[2] / "data_test")
DB_PATH = os.environ.get("DB_PATH") or str(Path(DATA_DIR) / "db.sqlite3")

NAV_SERIES_SQL = """
CREATE TABLE IF NOT EXISTS nav_series(
  product_key TEXT NOT NULL,
  date TEXT NOT NULL,
  nav REAL,
  unit_nav REAL,
  cum_nav REAL,
  PRIMARY KEY(product_key, date)
)
"""


def db_path(data_dir: str) -> str:
    return os.path.join(data_dir, "db.sqlite3")


@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def connect(data_dir: str):
    conn = sqlite3.connect(db_path(data_dir))
    try:
        yield conn
    finally:
        conn.close()


def init_db(data_dir: str):
    os.makedirs(data_dir, exist_ok=True)
    path = db_path(data_dir)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
          product_key TEXT NOT NULL,
          date TEXT NOT NULL,
          nav REAL,
          day_pct REAL,
          week_pct REAL,
          month_pct REAL,
          ytd_pct REAL,
          ytd_mode TEXT,
          source_nav TEXT,
          source_day TEXT,
          source_week TEXT,
          source_month TEXT,
          raw_path TEXT,
          mail_uid TEXT,
          mail_subject TEXT,
          mail_from TEXT,
          received_at TEXT,
          PRIMARY KEY(product_key, date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          date TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          attachments_count INTEGER,
          products_count INTEGER,
          out_xlsx_path TEXT,
          out_img_today_path TEXT,
          out_img_cmp_path TEXT,
          status TEXT,
          error TEXT
        )
        """
    )

    cur.execute(NAV_SERIES_SQL)

    conn.commit()
    conn.close()


def ensure_tables():
    with db_conn() as conn:
        conn.execute(NAV_SERIES_SQL)
        conn.commit()




def _get_nav_before(product_key: str, date: str) -> tuple[str, float] | None:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT date, nav FROM nav_series WHERE product_key=? AND date<? AND nav IS NOT NULL "
            "ORDER BY date DESC LIMIT 1",
            (product_key, date),
        )
        row = cur.fetchone()
        return (row[0], float(row[1])) if row else None


def _get_nav_on_or_before(product_key: str, date: str) -> tuple[str, float] | None:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT date, nav FROM nav_series WHERE product_key=? AND date<=? AND nav IS NOT NULL "
            "ORDER BY date DESC LIMIT 1",
            (product_key, date),
        )
        row = cur.fetchone()
        return (row[0], float(row[1])) if row else None


def get_prev_nav(product_key: str, date: str) -> float | None:
    r = _get_nav_before(product_key, date)
    return r[1] if r else None


def get_last_week_end_nav(product_key: str, date: str) -> float | None:
    d = datetime.strptime(date, "%Y%m%d")
    anchor = (d - timedelta(days=7)).strftime("%Y%m%d")
    r = _get_nav_on_or_before(product_key, anchor)
    return r[1] if r else None


def get_last_month_end_nav(product_key: str, date: str) -> float | None:
    d = datetime.strptime(date, "%Y%m%d")
    first = d.replace(day=1)
    prev_month_end = (first - timedelta(days=1)).strftime("%Y%m%d")
    r = _get_nav_on_or_before(product_key, prev_month_end)
    return r[1] if r else None


def get_year_start_nav_A(product_key: str, date: str) -> float | None:
    year = date[:4]
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nav FROM nav_series WHERE product_key=? AND date>=? AND date<=? AND nav IS NOT NULL "
            "ORDER BY date ASC LIMIT 1",
            (product_key, f"{year}0101", date),
        )
        row = cur.fetchone()
        return float(row[0]) if row else None


def pct_change(today: float | None, base: float | None) -> float | None:
    if today is None or base is None or base == 0:
        return None
    return (today - base) / base



# --- NAV series helpers (append-only safe) ---
def ensure_nav_series(conn):
    conn.execute(NAV_SERIES_SQL)
    conn.commit()

def upsert_nav(conn, product_key: str, date: str, nav: float | None = None, unit_nav: float | None = None, cum_nav: float | None = None):
    if not product_key or not date:
        return 0
    ensure_nav_series(conn)
    try:
        nav_v = float(nav) if nav is not None else None
    except Exception:
        nav_v = None
    try:
        unit_v = float(unit_nav) if unit_nav is not None else None
    except Exception:
        unit_v = None
    try:
        cum_v = float(cum_nav) if cum_nav is not None else None
    except Exception:
        cum_v = None
    if nav_v is None:
        nav_v = unit_v
    if nav_v is None and cum_v is not None:
        nav_v = cum_v
    if nav_v is None and unit_v is None and cum_v is None:
        return 0
    conn.execute(
        """
        INSERT INTO nav_series(product_key, date, nav, unit_nav, cum_nav)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(product_key, date) DO UPDATE SET
          nav=excluded.nav,
          unit_nav=CASE WHEN excluded.unit_nav IS NOT NULL THEN excluded.unit_nav ELSE nav_series.unit_nav END,
          cum_nav=CASE WHEN excluded.cum_nav IS NOT NULL THEN excluded.cum_nav ELSE nav_series.cum_nav END
        """,
        (product_key, date, nav_v, unit_v, cum_v),
    )
    conn.commit()
    return 1

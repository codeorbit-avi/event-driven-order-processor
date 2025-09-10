"""
Store Monitoring API (FastAPI, SQLite)
--------------------------------------
Implements the take‑home: ingest CSVs, compute uptime/downtime within business hours,
extrapolate between irregular polls, and expose two endpoints:

1) POST /trigger_report -> { report_id }
2) GET  /get_report?report_id=... -> Running | Complete + CSV

Quickstart
==========
1) Python 3.10+
2) pip install -r requirements.txt  (see list below)
3) uvicorn app:app --reload --port 8000
4) Trigger generation:  
   curl -X POST http://localhost:8000/trigger_report
5) Poll result:         
   curl "http://localhost:8000/get_report?report_id=<id>" -OJ

Configuration
=============
- DATA_ZIP_URL: HTTPS url to the provided zip (default from problem statement).
- Or set DATA_DIR to a local directory that already contains the three CSVs.
- CURRENT_TIME: By requirement, we hard‑code the current timestamp to the maximum
  timestamp in the status CSV (computed during ingest).

Assumptions & Interpolation
===========================
- Business hours: if missing for a store => 24x7.
- Timezone: if missing => America/Chicago.
- Interpolation of status between polls uses a piecewise‑constant Voronoi logic:
  each observation owns the time interval bounded by midpoints to its neighbors.
  At the window edges, the first/last observation extend to the edge.
- Only time within business hours is counted. We build daily local windows for the
  requested range, convert each to UTC, and intersect with the analysis window.
- If a store has **zero observations** across the analysis horizon (and no neighbors),
  we return 0 uptime and 0 downtime for that horizon (cannot infer either way).  
  This keeps the report schema while avoiding misleading fills.

Requirements (pip)
==================
fastapi
uvicorn
pydantic>=2
sqlalchemy>=2
pandas
python-dateutil
pytz
zoneinfo; sys_platform == 'win32'  # only on Windows if needed

"""

from __future__ import annotations

import csv
import io
import math
import os
import sqlite3
import tempfile
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import pytz
import requests
from dateutil import parser as dtparser
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse, StreamingResponse

# -------------------------
# Config
# -------------------------
DATA_ZIP_URL = os.getenv(
    "DATA_ZIP_URL",
    "https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip",
)
DATA_DIR = os.getenv("DATA_DIR")  # if provided, use CSVs from a local dir
DB_PATH = os.getenv("DB_PATH", os.path.join(tempfile.gettempdir(), "store_monitoring.sqlite3"))
DEFAULT_TZ = "America/Chicago"

# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(title="Store Monitoring API", version="1.0.0")

# -------------------------
# DB helpers
# -------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS status_logs (
            store_id TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,  -- ISO 8601
            status TEXT NOT NULL CHECK(status in ('active','inactive'))
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_status_store_time
        ON status_logs (store_id, timestamp_utc);
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS business_hours (
            store_id TEXT NOT NULL,
            day_of_week INTEGER NOT NULL, -- 0=Monday .. 6=Sunday
            start_time_local TEXT NOT NULL, -- 'HH:MM:SS'
            end_time_local   TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS store_timezones (
            store_id TEXT PRIMARY KEY,
            timezone_str TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            report_id TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK(status in ('Running','Complete','Error')),
            created_at TEXT NOT NULL,
            csv_blob BLOB,       -- filled when complete
            message TEXT         -- error info
        );
        """
    )
    conn.commit()
    conn.close()


# -------------------------
# Ingestion
# -------------------------

def _download_and_extract_zip(url: str, dest_dir: str):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        zf.extractall(dest_dir)


def ingest_data():
    """Load CSVs into SQLite. Idempotent: clears existing tables then inserts.
    Also computes and stores CURRENT_TIME (max UTC timestamp in status_logs)
    into a temp meta table for later use.
    """
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    # Clear tables
    cur.execute("DELETE FROM status_logs")
    cur.execute("DELETE FROM business_hours")
    cur.execute("DELETE FROM store_timezones")

    tempdir = DATA_DIR or tempfile.mkdtemp(prefix="storemon_")
    if not DATA_DIR:
        _download_and_extract_zip(DATA_ZIP_URL, tempdir)

    # Expect file names (from the provided zip):
    # - store_status.csv  (store_id, timestamp_utc, status)
    # - business_hours.csv (store_id, dayOfWeek, start_time_local, end_time_local)
    # - store_timezones.csv (store_id, timezone_str)
    # Some datasets may use slightly different headers; handle with pandas for resilience.

    def _read_any(path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        return df

    # Find candidate CSVs by name contains
    def _find_csv(name_contains: str) -> Optional[str]:
        for fn in os.listdir(tempdir):
            if fn.lower().endswith('.csv') and name_contains in fn.lower():
                return os.path.join(tempdir, fn)
        return None

    status_path = _find_csv("status") or _find_csv("store_status") or _find_csv("status_logs")
    bh_path = _find_csv("business")
    tz_path = _find_csv("timezone") or _find_csv("tz")

    if not status_path or not bh_path or not tz_path:
        raise RuntimeError(f"Could not locate expected CSVs in {tempdir}")

    status_df = _read_any(status_path)
    # Normalize columns
    status_df.rename(columns={
        'timestamp': 'timestamp_utc',
        'store_id': 'store_id',
        'status': 'status',
        'timestamp_utc': 'timestamp_utc'
    }, inplace=True)
    # Keep only needed cols
    status_df = status_df[['store_id', 'timestamp_utc', 'status']]

    # Write to DB
    cur.executemany(
        "INSERT INTO status_logs (store_id, timestamp_utc, status) VALUES (?,?,?)",
        [(str(r.store_id), str(r.timestamp_utc), str(r.status)) for r in status_df.itertuples(index=False)]
    )

    bh_df = _read_any(bh_path)
    # Normalize columns
    ren = {}
    for c in bh_df.columns:
        lc = c.lower()
        if lc.startswith('day'):
            ren[c] = 'day_of_week'
        elif 'start' in lc:
            ren[c] = 'start_time_local'
        elif 'end' in lc:
            ren[c] = 'end_time_local'
        elif 'store' in lc:
            ren[c] = 'store_id'
    bh_df.rename(columns=ren, inplace=True)
    bh_df = bh_df[['store_id', 'day_of_week', 'start_time_local', 'end_time_local']]

    cur.executemany(
        "INSERT INTO business_hours (store_id, day_of_week, start_time_local, end_time_local) VALUES (?,?,?,?)",
        [(str(r.store_id), int(r.day_of_week), str(r.start_time_local), str(r.end_time_local)) for r in bh_df.itertuples(index=False)]
    )

    tz_df = _read_any(tz_path)
    ren = {}
    for c in tz_df.columns:
        lc = c.lower()
        if 'store' in lc:
            ren[c] = 'store_id'
        elif 'timezone' in lc or 'tz' in lc:
            ren[c] = 'timezone_str'
    tz_df.rename(columns=ren, inplace=True)
    tz_df = tz_df[['store_id', 'timezone_str']]

    cur.executemany(
        "INSERT OR REPLACE INTO store_timezones (store_id, timezone_str) VALUES (?,?)",
        [(str(r.store_id), str(r.timezone_str)) for r in tz_df.itertuples(index=False)]
    )

    # Compute CURRENT_TIME as max timestamp in status_logs
    cur.execute("DROP TABLE IF EXISTS meta")
    cur.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    cur.execute("SELECT MAX(timestamp_utc) as mx FROM status_logs")
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError("status_logs is empty; cannot compute CURRENT_TIME")
    current_time_iso = row[0]
    cur.execute("INSERT INTO meta (k,v) VALUES (?,?)", ("CURRENT_TIME_UTC", current_time_iso))

    conn.commit()
    conn.close()


# -------------------------
# Time & schedule helpers
# -------------------------

def parse_iso_utc(s: str) -> datetime:
    dt = dtparser.isoparse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def get_store_tz(conn: sqlite3.Connection, store_id: str) -> pytz.BaseTzInfo:
    cur = conn.execute("SELECT timezone_str FROM store_timezones WHERE store_id=?", (store_id,))
    row = cur.fetchone()
    tzname = row[0] if row and row[0] else DEFAULT_TZ
    try:
        return pytz.timezone(tzname)
    except Exception:
        return pytz.timezone(DEFAULT_TZ)


def get_business_rules(conn: sqlite3.Connection, store_id: str) -> Dict[int, List[Tuple[dtime, dtime]]]:
    """Return mapping of day_of_week -> list of (start_local, end_local) time windows.
    If missing entirely => 24x7 via a single (00:00:00, 23:59:59.999999) daily window.
    Handles overnight ranges (start > end) by splitting into two days.
    """
    cur = conn.execute(
        "SELECT day_of_week, start_time_local, end_time_local FROM business_hours WHERE store_id=?",
        (store_id,)
    )
    rows = cur.fetchall()

    rules: Dict[int, List[Tuple[dtime, dtime]]] = {i: [] for i in range(7)}

    if not rows:
        # 24x7
        for i in range(7):
            rules[i].append((dtime(0, 0, 0), dtime(23, 59, 59, 999999)))
        return rules

    for r in rows:
        dow = int(r[0])
        st = dtparser.parse(r[1]).time() if isinstance(r[1], str) else r[1]
        et = dtparser.parse(r[2]).time() if isinstance(r[2], str) else r[2]
        if st <= et:
            rules[dow].append((st, et))
        else:
            # Overnight: split (dow: st->23:59:59.999999) and ((dow+1)%7: 00:00->et)
            rules[dow].append((st, dtime(23, 59, 59, 999999)))
            rules[(dow + 1) % 7].append((dtime(0, 0, 0), et))
    return rules


def local_business_windows_to_utc(
    tz: pytz.BaseTzInfo,
    rules: Dict[int, List[Tuple[dtime, dtime]]],
    range_start_utc: datetime,
    range_end_utc: datetime,
) -> List[Tuple[datetime, datetime]]:
    """Build a list of UTC intervals when the store is open within [range_start_utc, range_end_utc]."""
    assert range_start_utc.tzinfo and range_end_utc.tzinfo
    out: List[Tuple[datetime, datetime]] = []

    # Iterate each local day overlapping range. Convert range UTC to local dates.
    # We'll step local day by day from start_local_date to end_local_date.
    start_local = range_start_utc.astimezone(tz)
    end_local = range_end_utc.astimezone(tz)

    day_cursor = start_local.date()
    while day_cursor <= end_local.date():
        dow = day_cursor.weekday()  # 0=Mon .. 6=Sun
        for (st_local_t, et_local_t) in rules.get(dow, []):
            st_local_dt = datetime.combine(day_cursor, st_local_t)
            et_local_dt = datetime.combine(day_cursor, et_local_t)
            st_utc = tz.localize(st_local_dt, is_dst=None).astimezone(timezone.utc)
            et_utc = tz.localize(et_local_dt, is_dst=None).astimezone(timezone.utc)
            # Intersect with global range
            a = max(st_utc, range_start_utc)
            b = min(et_utc, range_end_utc)
            if a < b:
                out.append((a, b))
        # next day
        day_cursor = day_cursor + timedelta(days=1)

    # Merge adjacent/overlapping UTC windows for neatness
    out.sort(key=lambda x: x[0])
    merged: List[Tuple[datetime, datetime]] = []
    for s, e in out:
        if not merged or s > merged[-1][1]:
            merged.append((s, e))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    return merged


# -------------------------
# Status interpolation and aggregation
# -------------------------

def fetch_obs_with_neighbors(conn: sqlite3.Connection, store_id: str, start_utc: datetime, end_utc: datetime) -> List[Tuple[datetime, str]]:
    """Fetch observations inside [start,end], plus one neighbor before and one after if available."""
    cur = conn.execute(
        """
        SELECT timestamp_utc, status FROM status_logs
        WHERE store_id=? AND timestamp_utc < ?
        ORDER BY timestamp_utc DESC LIMIT 1
        """,
        (store_id, end_utc.isoformat())
    )
    # The above just positions the index; we'll fetch properly below.

    # explicit queries
    prev = conn.execute(
        "SELECT timestamp_utc, status FROM status_logs WHERE store_id=? AND timestamp_utc < ? ORDER BY timestamp_utc DESC LIMIT 1",
        (store_id, start_utc.isoformat()),
    ).fetchone()
    rows = conn.execute(
        "SELECT timestamp_utc, status FROM status_logs WHERE store_id=? AND timestamp_utc >= ? AND timestamp_utc <= ? ORDER BY timestamp_utc ASC",
        (store_id, start_utc.isoformat(), end_utc.isoformat()),
    ).fetchall()
    nxt = conn.execute(
        "SELECT timestamp_utc, status FROM status_logs WHERE store_id=? AND timestamp_utc > ? ORDER BY timestamp_utc ASC LIMIT 1",
        (store_id, end_utc.isoformat()),
    ).fetchone()

    obs: List[Tuple[datetime, str]] = []
    if prev:
        obs.append((parse_iso_utc(prev[0]), prev[1]))
    for r in rows:
        obs.append((parse_iso_utc(r[0]), r[1]))
    if nxt:
        obs.append((parse_iso_utc(nxt[0]), nxt[1]))
    return obs


def aggregate_uptime_for_window(
    conn: sqlite3.Connection,
    store_id: str,
    window_start: datetime,
    window_end: datetime,
) -> Tuple[float, float]:
    """Return (uptime_seconds, downtime_seconds) within business hours for [window_start, window_end]."""
    tz = get_store_tz(conn, store_id)
    rules = get_business_rules(conn, store_id)

    # Build open intervals in UTC intersected with [window_start, window_end]
    open_utc_windows = local_business_windows_to_utc(tz, rules, window_start, window_end)
    if not open_utc_windows:
        return 0.0, 0.0

    # For interpolation, fetch observations spanning the union of open windows
    # We'll process each open window independently for simplicity.
    total_up = 0.0
    total_down = 0.0

    for (a, b) in open_utc_windows:
        obs = fetch_obs_with_neighbors(conn, store_id, a, b)
        if not obs:
            # No data at all -> cannot infer; count neither
            continue
        # Build midpoints
        times = [t for (t, s) in obs]
        statuses = [s for (t, s) in obs]
        mids: List[datetime] = []
        for i in range(len(times) - 1):
            dt_mid = times[i] + (times[i + 1] - times[i]) / 2
            mids.append(dt_mid)
        # Define segments for each observation i
        seg_starts: List[datetime] = []
        seg_ends: List[datetime] = []
        for i in range(len(times)):
            left = a if i == 0 else max(a, mids[i - 1])
            right = b if i == len(times) - 1 else min(b, mids[i])
            if left < right:
                seg_starts.append(left)
                seg_ends.append(right)
            else:
                seg_starts.append(None)  # placeholder
                seg_ends.append(None)
        for i in range(len(times)):
            if seg_starts[i] is None:
                continue
            dur = (seg_ends[i] - seg_starts[i]).total_seconds()
            if statuses[i] == 'active':
                total_up += dur
            else:
                total_down += dur

    return total_up, total_down


# -------------------------
# Report generation
# -------------------------

def get_current_time(conn: sqlite3.Connection) -> datetime:
    row = conn.execute("SELECT v FROM meta WHERE k='CURRENT_TIME_UTC'").fetchone()
    if not row:
        raise RuntimeError("CURRENT_TIME_UTC not set; did you ingest?")
    return parse_iso_utc(row[0])


def list_store_ids(conn: sqlite3.Connection) -> List[str]:
    ids = set()
    for row in conn.execute("SELECT DISTINCT store_id FROM status_logs"):
        ids.add(row[0])
    for row in conn.execute("SELECT DISTINCT store_id FROM business_hours"):
        ids.add(row[0])
    for row in conn.execute("SELECT DISTINCT store_id FROM store_timezones"):
        ids.add(row[0])
    return sorted(ids)


def seconds_to_minutes_round(x: float) -> int:
    return int(round(x / 60.0))


def seconds_to_hours_float(x: float) -> float:
    return round(x / 3600.0, 2)


def build_report_csv() -> bytes:
    conn = get_conn()
    try:
        now_utc = get_current_time(conn)

        # Analysis windows (in UTC, relative to now_utc)
        last_hour_start = now_utc - timedelta(hours=1)
        last_day_start = now_utc - timedelta(days=1)
        last_week_start = now_utc - timedelta(weeks=1)

        headers = [
            "store_id",
            "uptime_last_hour_mins",
            "uptime_last_day_hours",
            "uptime_last_week_hours",
            "downtime_last_hour_mins",
            "downtime_last_day_hours",
            "downtime_last_week_hours",
        ]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)

        for store_id in list_store_ids(conn):
            # hour
            up_h, down_h = aggregate_uptime_for_window(conn, store_id, last_hour_start, now_utc)
            # day
            up_d, down_d = aggregate_uptime_for_window(conn, store_id, last_day_start, now_utc)
            # week
            up_w, down_w = aggregate_uptime_for_window(conn, store_id, last_week_start, now_utc)

            row = [
                store_id,
                seconds_to_minutes_round(up_h),
                seconds_to_hours_float(up_d),
                seconds_to_hours_float(up_w),
                seconds_to_minutes_round(down_h),
                seconds_to_hours_float(down_d),
                seconds_to_hours_float(down_w),
            ]
            writer.writerow(row)

        return output.getvalue().encode('utf-8')
    finally:
        conn.close()


# -------------------------
# Background job orchestration
# -------------------------

def _set_report_status(report_id: str, status: str, csv_blob: Optional[bytes] = None, message: Optional[str] = None):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO reports(report_id, status, created_at, csv_blob, message) VALUES(?,?,?,?,?)\n            ON CONFLICT(report_id) DO UPDATE SET status=excluded.status, csv_blob=excluded.csv_blob, message=excluded.message",
            (report_id, status, datetime.utcnow().isoformat(), sqlite3.Binary(csv_blob) if csv_blob else None, message),
        )
        conn.commit()
    finally:
        conn.close()


def _generate_and_store_report(report_id: str):
    try:
        # Ensure data is ingested (idempotent)
        ingest_data()
        csv_bytes = build_report_csv()
        _set_report_status(report_id, "Complete", csv_blob=csv_bytes)
    except Exception as e:
        _set_report_status(report_id, "Error", message=str(e))


# -------------------------
# API endpoints
# -------------------------

@app.post("/trigger_report")
def trigger_report():
    """Kick off report generation and return a report_id to poll."""
    report_id = uuid.uuid4().hex
    _set_report_status(report_id, "Running")

    # Start background thread (simple for take‑home; a real system might use Celery/RQ)
    t = threading.Thread(target=_generate_and_store_report, args=(report_id,), daemon=True)
    t.start()

    return {"report_id": report_id}


@app.get("/get_report")
def get_report(report_id: str = Query(..., description="Report id returned by /trigger_report")):
    conn = get_conn()
    try:
        row = conn.execute("SELECT status, csv_blob, message FROM reports WHERE report_id=?", (report_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Unknown report_id")
        status = row[0]
        if status == "Running":
            return JSONResponse({"status": "Running"})
        if status == "Error":
            return JSONResponse({"status": "Error", "message": row[2]}, status_code=500)
        csv_blob = row[1]
        if not csv_blob:
            return JSONResponse({"status": "Error", "message": "No CSV found"}, status_code=500)
        filename = f"report_{report_id}.csv"
        return StreamingResponse(io.BytesIO(csv_blob), media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })
    finally:
        conn.close()


# Root for health/info
@app.get("/")
def root():
    return {
        "name": "Store Monitoring API",
        "version": "1.0.0",
        "endpoints": ["POST /trigger_report", "GET /get_report?report_id=..."],
        "config": {
            "DATA_ZIP_URL": DATA_ZIP_URL,
            "DB_PATH": DB_PATH,
            "DEFAULT_TZ": DEFAULT_TZ,
        },
    }


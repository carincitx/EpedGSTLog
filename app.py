# app.py
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

import pyodbc
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

app = FastAPI(title="SpedBusMD API", version="1.0.0")

# ----------------------------
# ENV / CONFIG
# ----------------------------
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
SQL_SERVER = os.getenv("SQL_SERVER", "").strip()          # e.g. spedbusmd-sql-german01.database.windows.net
SQL_DATABASE = os.getenv("SQL_DATABASE", "").strip()      # e.g. spedbusdb
SQL_USERNAME = os.getenv("SQL_USERNAME", "").strip()      # e.g. spedbusmd-sql-german01-admin
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "").strip()
TRUST_SERVER_CERT = os.getenv("TRUST_SERVER_CERT", "true").lower() in ("1", "true", "yes", "y")
ENCRYPT = os.getenv("ENCRYPT", "true").lower() in ("1", "true", "yes", "y")

# Minutes to wait after ARRIVED before auto-marking as NO_CALL_NO_SHOW
NO_SHOW_MINUTES = int(os.getenv("NO_SHOW_MINUTES", "5"))

# Optional: full connection string override (recommended for Azure app settings)
ODBC_CONN_STR = os.getenv("ODBC_CONN_STR", "").strip()

# ----------------------------
# Event Types (FINAL)
# ----------------------------
# Only these are stored in DB:
VALID_EVENT_TYPES = {"ARRIVED", "RIDE", "NO_CALL_NO_SHOW"}

# Friendly inputs -> stored values
EVENT_TYPE_ALIASES = {
    # arrived/pending
    "arrived": "ARRIVED",
    "arrival": "ARRIVED",
    "waiting": "ARRIVED",
    "wait": "ARRIVED",

    # ride
    "ride": "RIDE",
    "board": "RIDE",
    "boarded": "RIDE",

    # no-call/no-show
    "no_call_no_show": "NO_CALL_NO_SHOW",
    "no_call_noshow": "NO_CALL_NO_SHOW",
    "no_call": "NO_CALL_NO_SHOW",
    "nocall": "NO_CALL_NO_SHOW",
    "no_show": "NO_CALL_NO_SHOW",
    "noshow": "NO_CALL_NO_SHOW",
    "no-show": "NO_CALL_NO_SHOW",
    "no call no show": "NO_CALL_NO_SHOW",
}

# ----------------------------
# Models
# ----------------------------
class ScanRequest(BaseModel):
    student_code: str = Field(..., description="StudentCode from barcode/QR")
    event_type: str = Field(..., description="arrived | ride | no call no show (friendly inputs accepted)")
    driver_code: Optional[str] = None
    aide_code: Optional[str] = None
    stop_code: Optional[str] = None
    notes: Optional[str] = None


# ----------------------------
# DB helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_conn_str() -> str:
    if ODBC_CONN_STR:
        return ODBC_CONN_STR

    if not (SQL_SERVER and SQL_DATABASE and SQL_USERNAME and SQL_PASSWORD):
        raise RuntimeError(
            "Missing DB config. Set ODBC_CONN_STR OR SQL_SERVER/SQL_DATABASE/SQL_USERNAME/SQL_PASSWORD."
        )

    server = SQL_SERVER
    if not server.lower().startswith("tcp:"):
        server = f"tcp:{server}"

    return (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server={server},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        f"Encrypt={'yes' if ENCRYPT else 'no'};"
        f"TrustServerCertificate={'yes' if TRUST_SERVER_CERT else 'no'};"
        "Connection Timeout=30;"
    )


def get_conn() -> pyodbc.Connection:
    return pyodbc.connect(build_conn_str(), autocommit=False)


def normalize_student_code(code: str) -> str:
    code = (code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="student_code is required")
    return code


def normalize_event_type(raw: str) -> str:
    raw = (raw or "").strip().lower()

    # normalize separators
    raw = raw.replace("-", "_").replace(" ", "_")
    raw = re.sub(r"[^a-z_]", "", raw)

    event = EVENT_TYPE_ALIASES.get(raw)
    if event:
        return event

    # allow direct already-correct values
    upper = raw.upper()
    if upper in VALID_EVENT_TYPES:
        return upper

    raise HTTPException(
        status_code=400,
        detail="Invalid event_type. Use: arrived | ride | no call no show",
    )


def finalize_expired_arrivals(conn: pyodbc.Connection, minutes: int = NO_SHOW_MINUTES) -> int:
    """
    Convert ARRIVED logs older than N minutes into NO_CALL_NO_SHOW
    if the student's latest event is still ARRIVED.
    Returns number inserted.
    """
    cutoff = utc_now() - timedelta(minutes=minutes)
    cur = conn.cursor()

    # Find students whose latest EventType is ARRIVED and EventTimeUtc <= cutoff
    cur.execute(
        """
        WITH Latest AS (
          SELECT StudentCode, MAX(EventTimeUtc) AS MaxTime
          FROM dbo.ScanLogs
          GROUP BY StudentCode
        ),
        LatestRows AS (
          SELECT s.*
          FROM dbo.ScanLogs s
          JOIN Latest l
            ON s.StudentCode = l.StudentCode
           AND s.EventTimeUtc = l.MaxTime
        )
        SELECT TOP (500)
          LogId, StudentCode, StudentName, DOB, BusNumber, EventTimeUtc, DriverCode, AideCode, StopCode
        FROM LatestRows
        WHERE EventType = 'ARRIVED'
          AND EventTimeUtc <= ?
        ORDER BY EventTimeUtc ASC;
        """,
        cutoff,
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    inserted = 0

    # Double-check latest is still ARRIVED before inserting final event
    check_latest_sql = """
    SELECT TOP (1) EventType, EventTimeUtc, StudentName, DOB, BusNumber, DriverCode, AideCode, StopCode
    FROM dbo.ScanLogs
    WHERE StudentCode = ?
    ORDER BY EventTimeUtc DESC, LogId DESC;
    """

    insert_sql = """
    INSERT INTO dbo.ScanLogs
      (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
    VALUES
      (?, ?, ?, ?, 'NO_CALL_NO_SHOW', SYSUTCDATETIME(), ?, ?, ?, ?);
    """

    for r in rows:
        _, student_code, student_name, dob, bus_number, _, driver_code, aide_code, stop_code = r

        cur.execute(check_latest_sql, student_code)
        latest = cur.fetchone()
        if not latest:
            continue

        latest_type, latest_time, *_rest = latest
        if latest_type != "ARRIVED":
            continue
        if latest_time is None or latest_time > cutoff:
            continue

        cur.execute(
            insert_sql,
            student_code,
            student_name,
            dob,
            bus_number,
            driver_code,
            aide_code,
            stop_code,
            f"Auto NO_CALL_NO_SHOW after {minutes} min wait",
        )
        inserted += 1

    return inserted


def schedule_auto_finalize(student_code: str, minutes: int = NO_SHOW_MINUTES):
    """
    Background fallback: waits N minutes then tries to convert ARRIVED -> NO_CALL_NO_SHOW.
    (We still also run finalize_expired_arrivals() on every request as a safety net.)
    """
    time.sleep(max(0, minutes * 60))
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            # Convert any expired arrivals (covers this student)
            inserted = finalize_expired_arrivals(conn, minutes)
            if inserted > 0:
                conn.commit()
            else:
                conn.rollback()
        finally:
            conn.close()
    except Exception:
        # Don't crash server because a background task fails
        pass


# ----------------------------
# Routes
# ----------------------------
@app.get("/health")
def health():
    info: Dict[str, Any] = {
        "ok": True,
        "time_utc": utc_now().isoformat(),
        "db": "unknown",
        "trust_server_cert": TRUST_SERVER_CERT,
        "driver": ODBC_DRIVER,
        "no_show_minutes": NO_SHOW_MINUTES,
    }
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
            info["db"] = "ok"
            conn.rollback()
        finally:
            conn.close()
    except Exception as e:
        info["db"] = "error"
        info["detail"] = str(e)
    return info


@app.get("/students/{student_code}")
def get_student(student_code: str):
    student_code = normalize_student_code(student_code)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (1) StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM dbo.Students
            WHERE StudentCode = ?;
            """,
            student_code,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        return {
            "ok": True,
            "student": {
                "StudentId": int(row[0]),
                "StudentCode": row[1],
                "StudentName": row[2],
                "DOB": str(row[3]) if row[3] is not None else None,
                "BusNumber": row[4],
                "ParentPhone": row[5],
            },
        }
    finally:
        conn.close()


@app.get("/logs/recent")
def recent_logs(limit: int = 50):
    limit = max(1, min(limit, 200))
    conn = get_conn()
    try:
        # safety net: finalize expired arrivals whenever anyone calls the API
        inserted = finalize_expired_arrivals(conn, NO_SHOW_MINUTES)
        if inserted:
            conn.commit()
        else:
            conn.rollback()

        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP ({limit})
              LogId, StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc,
              DriverCode, AideCode, StopCode, Notes
            FROM dbo.ScanLogs
            ORDER BY EventTimeUtc DESC, LogId DESC;
            """
        )
        rows = cur.fetchall()

        logs: List[Dict[str, Any]] = []
        for r in rows:
            logs.append(
                {
                    "LogId": int(r[0]),
                    "StudentCode": r[1],
                    "StudentName": r[2],
                    "DOB": str(r[3]) if r[3] is not None else None,
                    "BusNumber": r[4],
                    "EventType": r[5],
                    "EventTimeUtc": str(r[6]),
                    "DriverCode": r[7],
                    "AideCode": r[8],
                    "StopCode": r[9],
                    "Notes": r[10],
                }
            )

        return {"ok": True, "count": len(logs), "logs": logs}
    finally:
        conn.close()


@app.post("/scan")
def scan_student(scan_req: ScanRequest, background: BackgroundTasks):
    student_code = normalize_student_code(scan_req.student_code)
    event_type = normalize_event_type(scan_req.event_type)

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Safety net first
        inserted = finalize_expired_arrivals(conn, NO_SHOW_MINUTES)
        if inserted:
            conn.commit()
        else:
            conn.rollback()

        # Pull student record in background
        cur.execute(
            """
            SELECT TOP (1) StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM dbo.Students
            WHERE StudentCode = ?;
            """,
            student_code,
        )
        s = cur.fetchone()
        if not s:
            raise HTTPException(status_code=404, detail="Student not found")

        student_name = s[1]
        dob = s[2]
        bus_number = s[3]
        parent_phone = s[4]

        # Insert into ScanLogs (your exact schema)
        cur.execute(
            """
            INSERT INTO dbo.ScanLogs
              (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
            OUTPUT INSERTED.LogId
            VALUES
              (?, ?, ?, ?, ?, SYSUTCDATETIME(), ?, ?, ?, ?);
            """,
            student_code,
            student_name,
            dob,
            bus_number,
            event_type,
            scan_req.driver_code,
            scan_req.aide_code,
            scan_req.stop_code,
            scan_req.notes,
        )
        new_id = int(cur.fetchone()[0])
        conn.commit()

        # If ARRIVED: schedule background conversion after 5 minutes (fallback)
        if event_type == "ARRIVED":
            background.add_task(schedule_auto_finalize, student_code, NO_SHOW_MINUTES)

        return {
            "ok": True,
            "log": {
                "LogId": new_id,
                "StudentCode": student_code,
                "StudentName": student_name,
                "DOB": str(dob) if dob is not None else None,
                "BusNumber": bus_number,
                "EventType": event_type,
                "DriverCode": scan_req.driver_code,
                "AideCode": scan_req.aide_code,
                "StopCode": scan_req.stop_code,
                "Notes": scan_req.notes,
            },
            "actions": {
                "can_call_parent": bool(parent_phone),
                "parent_phone": parent_phone,
            },
        }
    finally:
        conn.close()


@app.post("/finalize-expired-arrivals")
def finalize_now():
    """
    Admin/debug: force conversion ARRIVED -> NO_CALL_NO_SHOW for any expired records.
    """
    conn = get_conn()
    try:
        inserted = finalize_expired_arrivals(conn, NO_SHOW_MINUTES)
        if inserted:
            conn.commit()
        else:
            conn.rollback()
        return {"ok": True, "inserted_no_call_no_show": inserted}
    finally:
        conn.close()

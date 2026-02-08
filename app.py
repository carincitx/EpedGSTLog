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
# Configuration
# ----------------------------
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
SQL_SERVER = os.getenv("SQL_SERVER", "")          # e.g. spedbusmd-sql-german01.database.windows.net
SQL_DATABASE = os.getenv("SQL_DATABASE", "")      # e.g. spedbusdb
SQL_USERNAME = os.getenv("SQL_USERNAME", "")      # e.g. spedbusmd-sql-german01-admin
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")      # password
TRUST_SERVER_CERT = os.getenv("TRUST_SERVER_CERT", "true").lower() in ("1", "true", "yes", "y")
ENCRYPT = os.getenv("ENCRYPT", "true").lower() in ("1", "true", "yes", "y")

# 5 minutes (can override)
NO_SHOW_MINUTES = int(os.getenv("NO_SHOW_MINUTES", "5"))

# If you prefer passing a full ODBC connection string:
# Example:
# ODBC_CONN_STR="Driver={ODBC Driver 18 for SQL Server};Server=tcp:...;Database=...;Uid=...;Pwd=...;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
ODBC_CONN_STR = os.getenv("ODBC_CONN_STR", "").strip()

# ----------------------------
# Event Types
# ----------------------------
# Step 3 adds ARRIVED (pending/waiting).
VALID_EVENT_TYPES = {"ARRIVED", "RIDE", "NO_CALL", "NO_SHOW"}

# Friendly inputs -> stored values
EVENT_TYPE_ALIASES = {
    "arrived": "ARRIVED",
    "arrival": "ARRIVED",
    "waiting": "ARRIVED",
    "wait": "ARRIVED",
    "ride": "RIDE",
    "boarded": "RIDE",
    "onboard": "RIDE",
    "no_call": "NO_CALL",
    "nocall": "NO_CALL",
    "no_show": "NO_SHOW",
    "noshow": "NO_SHOW",
}

# ----------------------------
# Models
# ----------------------------
class ScanRequest(BaseModel):
    student_code: str = Field(..., description="Barcode/QR code value for the student (StudentCode)")
    event_type: str = Field(..., description="arrived | ride | no_call | no_show (friendly inputs allowed)")
    driver_code: Optional[str] = Field(None, description="Driver identifier/code")
    aide_code: Optional[str] = Field(None, description="Aide identifier/code")
    stop_code: Optional[str] = Field(None, description="Stop identifier/code")
    notes: Optional[str] = Field(None, description="Optional notes")


class StudentOut(BaseModel):
    StudentId: int
    StudentCode: str
    StudentName: str
    DOB: str
    BusNumber: str
    ParentPhone: Optional[str] = None


class ScanLogOut(BaseModel):
    LogId: int
    StudentCode: str
    StudentName: str
    DOB: str
    BusNumber: str
    EventType: str
    EventTimeUtc: str
    DriverCode: Optional[str] = None
    AideCode: Optional[str] = None
    StopCode: Optional[str] = None
    Notes: Optional[str] = None


# ----------------------------
# DB Helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _build_conn_str() -> str:
    if ODBC_CONN_STR:
        return ODBC_CONN_STR

    if not (SQL_SERVER and SQL_DATABASE and SQL_USERNAME and SQL_PASSWORD):
        raise RuntimeError(
            "Missing DB config. Set ODBC_CONN_STR or SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD."
        )

    # Note: for Azure SQL, Encrypt=yes is recommended. TrustServerCertificate can be true for dev.
    encrypt_str = "yes" if ENCRYPT else "no"
    trust_str = "yes" if TRUST_SERVER_CERT else "no"

    # Use tcp: prefix for Azure SQL server names
    server = SQL_SERVER
    if not server.lower().startswith("tcp:"):
        server = f"tcp:{server}"

    return (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server={server};"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        f"Encrypt={encrypt_str};"
        f"TrustServerCertificate={trust_str};"
        f"Connection Timeout=30;"
    )


def get_conn() -> pyodbc.Connection:
    conn_str = _build_conn_str()
    # autocommit=False so we can control transactions
    return pyodbc.connect(conn_str, autocommit=False)


def normalize_student_code(code: str) -> str:
    code = (code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="student_code is required")
    return code


def normalize_event_type(raw: str) -> str:
    raw = (raw or "").strip().lower()
    raw = raw.replace(" ", "_").replace("-", "_")
    raw = re.sub(r"[^a-z_]", "", raw)

    # direct alias
    event = EVENT_TYPE_ALIASES.get(raw)
    if event:
        return event

    # maybe user typed already like NO_SHOW
    upper = raw.upper()
    if upper in VALID_EVENT_TYPES:
        return upper

    raise HTTPException(
        status_code=400,
        detail=f"Invalid event_type. Use: arrived | ride | no_call | no_show",
    )


def row_to_dict(cursor, row) -> Dict[str, Any]:
    cols = [c[0] for c in cursor.description]
    return {cols[i]: row[i] for i in range(len(cols))}


# ----------------------------
# Core Logic: ARRIVED -> auto NO_SHOW after N minutes
# ----------------------------
def finalize_expired_arrivals(conn: pyodbc.Connection, minutes: int = NO_SHOW_MINUTES) -> int:
    """
    Converts ARRIVED logs older than `minutes` into NO_SHOW, but only if no later terminal event exists.
    Terminal events: RIDE, NO_CALL, NO_SHOW
    Returns number of NO_SHOW logs inserted.
    """
    cutoff = utc_now() - timedelta(minutes=minutes)

    cur = conn.cursor()

    # 1) Find candidate students where latest event is ARRIVED and it's older than cutoff
    # We'll treat "latest" by EventTimeUtc, and tie-break by LogId.
    candidates_sql = """
    WITH Latest AS (
      SELECT
        StudentCode,
        MAX(EventTimeUtc) AS MaxTime
      FROM dbo.ScanLogs
      GROUP BY StudentCode
    ),
    LatestRows AS (
      SELECT s.*
      FROM dbo.ScanLogs s
      INNER JOIN Latest l
        ON s.StudentCode = l.StudentCode
       AND s.EventTimeUtc = l.MaxTime
    ),
    PickOne AS (
      SELECT TOP (1000) *
      FROM LatestRows
      WHERE EventType = 'ARRIVED'
        AND EventTimeUtc <= ?
      ORDER BY EventTimeUtc ASC
    )
    SELECT
      LogId, StudentCode, StudentName, DOB, BusNumber, EventTimeUtc, DriverCode, AideCode, StopCode
    FROM PickOne;
    """

    cur.execute(candidates_sql, cutoff)
    rows = cur.fetchall()
    if not rows:
        return 0

    inserted = 0

    # 2) For each candidate, ensure there is no newer terminal event after that ARRIVED (safety),
    # then insert NO_SHOW.
    check_sql = """
    SELECT TOP (1) LogId, EventType, EventTimeUtc
    FROM dbo.ScanLogs
    WHERE StudentCode = ?
    ORDER BY EventTimeUtc DESC, LogId DESC;
    """

    insert_sql = """
    INSERT INTO dbo.ScanLogs
      (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
    VALUES
      (?, ?, ?, ?, 'NO_SHOW', ?, ?, ?, ?, ?);
    """

    for r in rows:
        log_id, student_code, student_name, dob, bus_number, arrived_time, driver_code, aide_code, stop_code = r

        cur.execute(check_sql, student_code)
        latest = cur.fetchone()
        if not latest:
            continue

        latest_type = latest[1]
        latest_time = latest[2]

        # Only convert if latest is still ARRIVED and older than cutoff
        if latest_type != "ARRIVED":
            continue
        if latest_time is None or latest_time > cutoff:
            continue

        note = f"Auto NO_SHOW after {minutes} min wait"
        cur.execute(
            insert_sql,
            student_code,
            student_name,
            dob,
            bus_number,
            utc_now(),
            driver_code,
            aide_code,
            stop_code,
            note,
        )
        inserted += 1

    return inserted


def schedule_auto_no_show(student_code: str, arrived_time_utc: datetime, minutes: int = NO_SHOW_MINUTES):
    """
    Background fallback: waits N minutes then attempts to finalize if still ARRIVED.
    Also protected by finalize_expired_arrivals() being called on each request.
    """
    # Sleep in seconds
    time.sleep(max(0, minutes * 60))

    try:
        conn = get_conn()
        try:
            # Convert only if student still has ARRIVED as latest and it's older than cutoff
            cur = conn.cursor()
            cur.execute(
                """
                SELECT TOP (1) LogId, EventType, EventTimeUtc, StudentName, DOB, BusNumber, DriverCode, AideCode, StopCode
                FROM dbo.ScanLogs
                WHERE StudentCode = ?
                ORDER BY EventTimeUtc DESC, LogId DESC;
                """,
                student_code,
            )
            latest = cur.fetchone()
            if not latest:
                conn.rollback()
                return

            _, latest_type, latest_time, student_name, dob, bus_number, driver_code, aide_code, stop_code = latest
            cutoff = utc_now() - timedelta(minutes=minutes)

            if latest_type == "ARRIVED" and latest_time is not None and latest_time <= cutoff:
                cur.execute(
                    """
                    INSERT INTO dbo.ScanLogs
                      (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
                    VALUES
                      (?, ?, ?, ?, 'NO_SHOW', ?, ?, ?, ?, ?);
                    """,
                    student_code,
                    student_name,
                    dob,
                    bus_number,
                    utc_now(),
                    driver_code,
                    aide_code,
                    stop_code,
                    f"Auto NO_SHOW after {minutes} min wait (bg)",
                )
                conn.commit()
            else:
                conn.rollback()
        finally:
            conn.close()
    except Exception:
        # Don't crash the server because a background job failed
        pass


# ----------------------------
# Routes
# ----------------------------
@app.get("/health")
def health():
    """
    Basic health + DB connectivity check.
    """
    info = {
        "ok": True,
        "time_utc": utc_now().isoformat(),
        "db": "unknown",
        "trust_server_cert": TRUST_SERVER_CERT,
        "driver": ODBC_DRIVER,
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
        info["db"] = f"error: {str(e)}"

    return info


@app.get("/students/{student_code}", response_model=StudentOut)
def get_student(student_code: str):
    student_code = normalize_student_code(student_code)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (1)
              StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM dbo.Students
            WHERE StudentCode = ?;
            """,
            student_code,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        # ensure string formatting
        student = {
            "StudentId": int(row[0]),
            "StudentCode": row[1],
            "StudentName": row[2],
            "DOB": str(row[3]),
            "BusNumber": str(row[4]),
            "ParentPhone": row[5],
        }
        conn.rollback()
        return student
    finally:
        conn.close()


@app.get("/logs/recent", response_model=List[ScanLogOut])
def recent_logs(limit: int = 50):
    limit = max(1, min(limit, 200))

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Finalize any expired ARRIVED first (Step 3 safety net)
        finalize_expired_arrivals(conn, NO_SHOW_MINUTES)

        cur.execute(
            f"""
            SELECT TOP ({limit})
              LogId, StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes
            FROM dbo.ScanLogs
            ORDER BY EventTimeUtc DESC, LogId DESC;
            """
        )
        rows = cur.fetchall()
        conn.rollback()

        out = []
        for r in rows:
            out.append(
                {
                    "LogId": int(r[0]),
                    "StudentCode": r[1],
                    "StudentName": r[2],
                    "DOB": str(r[3]),
                    "BusNumber": str(r[4]),
                    "EventType": r[5],
                    "EventTimeUtc": r[6].replace(tzinfo=timezone.utc).isoformat() if hasattr(r[6], "replace") else str(r[6]),
                    "DriverCode": r[7],
                    "AideCode": r[8],
                    "StopCode": r[9],
                    "Notes": r[10],
                }
            )
        return out
    finally:
        conn.close()


@app.post("/scan", response_model=ScanLogOut)
def scan_student(scan_req: ScanRequest, background: BackgroundTasks):
    student_code = normalize_student_code(scan_req.student_code)
    event_type = normalize_event_type(scan_req.event_type)

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Step 3 safety net: finalize any expired ARRIVED before writing new logs
        finalize_expired_arrivals(conn, NO_SHOW_MINUTES)

        # Pull student record (your “scan pulls student info in background” requirement)
        cur.execute(
            """
            SELECT TOP (1)
              StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM dbo.Students
            WHERE StudentCode = ?;
            """,
            student_code,
        )
        s = cur.fetchone()
        if not s:
            raise HTTPException(status_code=404, detail="Student not found")

        student_name = s[2]
        dob = str(s[3])
        bus_number = str(s[4])

        now_utc = utc_now()

        # Insert scan log
        cur.execute(
            """
            INSERT INTO dbo.ScanLogs
              (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
            OUTPUT INSERTED.LogId
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            student_code,
            student_name,
            dob,
            bus_number,
            event_type,
            now_utc,
            scan_req.driver_code,
            scan_req.aide_code,
            scan_req.stop_code,
            scan_req.notes,
        )
        new_id = cur.fetchone()[0]
        conn.commit()

        # If ARRIVED, schedule auto NO_SHOW after N minutes (fallback behavior)
        if event_type == "ARRIVED":
            background.add_task(schedule_auto_no_show, student_code, now_utc, NO_SHOW_MINUTES)

        return {
            "LogId": int(new_id),
            "StudentCode": student_code,
            "StudentName": student_name,
            "DOB": dob,
            "BusNumber": bus_number,
            "EventType": event_type,
            "EventTimeUtc": now_utc.isoformat(),
            "DriverCode": scan_req.driver_code,
            "AideCode": scan_req.aide_code,
            "StopCode": scan_req.stop_code,
            "Notes": scan_req.notes,
        }
    finally:
        conn.close()


@app.post("/finalize-expired-arrivals")
def finalize_now():
    """
    Optional admin endpoint: force conversion of expired ARRIVED -> NO_SHOW.
    You can protect this later with login.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        inserted = finalize_expired_arrivals(conn, NO_SHOW_MINUTES)
        conn.commit()
        return {"ok": True, "inserted_no_show": inserted}
    finally:
        conn.close()


# ----------------------------
# IMPORTANT: SQL CHECK constraint update for Step 3
# ----------------------------
# If you already added a CHECK constraint that only allowed ('RIDE','NO_CALL','NO_SHOW'),
# you MUST update it to include 'ARRIVED' too:
#
# ALTER TABLE dbo.ScanLogs DROP CONSTRAINT CK_ScanLogs_EventType;
# ALTER TABLE dbo.ScanLogs
#   ADD CONSTRAINT CK_ScanLogs_EventType
#   CHECK (EventType IN ('ARRIVED','RIDE','NO_CALL','NO_SHOW'));
#
# ----------------------------

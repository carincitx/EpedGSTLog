# app.py
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import pyodbc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="SpedBusMD API", version="1.0.0")


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def build_connection_string() -> str:
    """
    Required env vars:
      SQL_SERVER   e.g. "spedbusmd-sql-german01.database.windows.net"
      SQL_DATABASE e.g. "spedbusdb"
      SQL_USER     e.g. "spedbusmd-sql-german01-admin"
      SQL_PASSWORD e.g. "yourStrongPassword!"
    Optional:
      ODBC_DRIVER (default: ODBC Driver 18 for SQL Server)
      TRUST_SERVER_CERT (default: true)  -> TrustServerCertificate=yes/no
      ENCRYPT (default: true)            -> Encrypt=yes/no
      SQL_TIMEOUT (default: 30)
    """
    driver = get_env("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    server = get_env("SQL_SERVER", required=True)
    database = get_env("SQL_DATABASE", required=True)
    username = get_env("SQL_USER", required=True)
    password = get_env("SQL_PASSWORD", required=True)

    trust_server_cert = get_env("TRUST_SERVER_CERT", "true").strip().lower() in ("1", "true", "yes", "y")
    encrypt = get_env("ENCRYPT", "true").strip().lower() in ("1", "true", "yes", "y")
    timeout = int(get_env("SQL_TIMEOUT", "30"))

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt={'yes' if encrypt else 'no'};"
        f"TrustServerCertificate={'yes' if trust_server_cert else 'no'};"
        f"Connection Timeout={timeout};"
    )


def get_db() -> pyodbc.Connection:
    return pyodbc.connect(build_connection_string(), autocommit=False)


def row_to_dict(cursor: pyodbc.Cursor, row: pyodbc.Row) -> Dict[str, Any]:
    cols = [c[0] for c in cursor.description]
    return {cols[i]: row[i] for i in range(len(cols))}


def normalize_student(stu: Dict[str, Any]) -> Dict[str, Any]:
    # Convert DOB to string for JSON safety
    if "DOB" in stu and stu["DOB"] is not None:
        stu["DOB"] = str(stu["DOB"])
    return stu


# -----------------------------
# Models
# -----------------------------
class ScanRequest(BaseModel):
    code: str = Field(..., description="StudentCode from QR/barcode, e.g. STU-1001")
    event_type: str = Field(..., description="ride | no-call | no-show")
    driver_code: Optional[str] = None
    aide_code: Optional[str] = None
    stop_code: Optional[str] = None
    notes: Optional[str] = None


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "SpedBusMD API", "time_utc": utc_now_iso()}


@app.get("/health")
def health():
    driver = get_env("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_server_cert = get_env("TRUST_SERVER_CERT", "true").strip().lower() in ("1", "true", "yes", "y")

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return {
            "ok": True,
            "time_utc": utc_now_iso(),
            "db": "ok",
            "trust_server_cert": trust_server_cert,
            "driver": driver,
        }
    except Exception as e:
        return {
            "ok": False,
            "time_utc": utc_now_iso(),
            "db": "error",
            "trust_server_cert": trust_server_cert,
            "driver": driver,
            "detail": str(e),
        }


@app.get("/students/{student_code}")
def get_student(student_code: str):
    """
    Tables:
      Students(StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone)
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM Students
            WHERE StudentCode = ?
            """,
            student_code,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        stu = normalize_student(row_to_dict(cur, row))
        return {"ok": True, "student": stu}
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/scan")
def scan(scan_req: ScanRequest):
    """
    Tables:
      Students(StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone)

      ScanLogs(
        LogId, StudentCode, StudentName, DOB, BusNumber,
        EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes
      )
    """
    event_type = scan_req.event_type.strip().lower()
    if event_type not in ("ride", "no-call", "no-show"):
        raise HTTPException(status_code=400, detail="Invalid event_type. Use: ride | no-call | no-show")

    conn = get_db()
    try:
        cur = conn.cursor()

        # Pull student info in the background based on scanned code
        cur.execute(
            """
            SELECT StudentCode, StudentName, DOB, BusNumber, ParentPhone
            FROM Students
            WHERE StudentCode = ?
            """,
            scan_req.code,
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        stu = normalize_student(row_to_dict(cur, row))

        # Insert into ScanLogs with your exact columns
        cur.execute(
            """
            INSERT INTO ScanLogs
              (StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc, DriverCode, AideCode, StopCode, Notes)
            VALUES
              (?, ?, ?, ?, ?, SYSUTCDATETIME(), ?, ?, ?, ?)
            """,
            stu["StudentCode"],
            stu["StudentName"],
            None if stu.get("DOB") is None else stu.get("DOB"),  # safe for SQL
            stu.get("BusNumber"),
            event_type,
            scan_req.driver_code,
            scan_req.aide_code,
            scan_req.stop_code,
            scan_req.notes,
        )

        conn.commit()

        return {
            "ok": True,
            "student": {
                "StudentCode": stu.get("StudentCode"),
                "StudentName": stu.get("StudentName"),
                "DOB": stu.get("DOB"),
                "BusNumber": stu.get("BusNumber"),
            },
            "log": {
                "EventType": event_type,
                "DriverCode": scan_req.driver_code,
                "AideCode": scan_req.aide_code,
                "StopCode": scan_req.stop_code,
                "EventTimeUtc": utc_now_iso(),
            },
            "actions": {
                "can_call_parent": bool(stu.get("ParentPhone")),
                "parent_phone": stu.get("ParentPhone"),
            },
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/logs/recent")
def recent_logs(limit: int = 50):
    """
    Quick endpoint to verify writes.
    """
    limit = max(1, min(limit, 500))
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT TOP ({limit})
              LogId, StudentCode, StudentName, DOB, BusNumber, EventType, EventTimeUtc,
              DriverCode, AideCode, StopCode, Notes
            FROM ScanLogs
            ORDER BY EventTimeUtc DESC
            """
        )
        rows = cur.fetchall()
        logs = []
        for r in rows:
            d = row_to_dict(cur, r)
            if d.get("DOB") is not None:
                d["DOB"] = str(d["DOB"])
            if d.get("EventTimeUtc") is not None:
                d["EventTimeUtc"] = str(d["EventTimeUtc"])
            logs.append(d)
        return {"ok": True, "count": len(logs), "logs": logs}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# Local run:
#   pip install -r requirements.txt
#   uvicorn app:app --reload --port 8000

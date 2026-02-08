# app.py
import os
from datetime import datetime, timezone
from typing import Optional, Literal, Dict, Any

import pyodbc
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# -----------------------------
# Config (set these in Azure App Settings / local shell)
# -----------------------------
SQL_SERVER = os.getenv("SQL_SERVER", "").strip()         # e.g. "spedbusmd-sql-german01.database.windows.net"
SQL_DATABASE = os.getenv("SQL_DATABASE", "").strip()     # e.g. "spedbusdb"
SQL_USERNAME = os.getenv("SQL_USERNAME", "").strip()     # e.g. "spedbusmd-sql-german01-admin"
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "").strip()

# Local-dev SSL helper:
# - For local development you typically want TrustServerCertificate=yes to avoid the macOS certificate chain error.
# - In production you should set this to "no" and rely on proper cert trust.
TRUST_SERVER_CERT = os.getenv("TRUST_SERVER_CERT", "yes").strip().lower() in ("1", "true", "yes", "y")

# Optional: allow changing driver name if needed
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server").strip()

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="SpedBusMD API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_conn_str() -> str:
    if not (SQL_SERVER and SQL_DATABASE and SQL_USERNAME and SQL_PASSWORD):
        # This is still OK for /health, but DB endpoints should fail clearly.
        return ""

    # NOTE: Azure SQL requires encryption. Driver 18 enforces it by default.
    # For local dev, TrustServerCertificate helps avoid certificate chain issues on macOS.
    trust = "yes" if TRUST_SERVER_CERT else "no"

    return (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        "Encrypt=yes;"
        f"TrustServerCertificate={trust};"
        "Connection Timeout=30;"
    )


def get_connection():
    conn_str = _build_conn_str()
    if not conn_str:
        raise RuntimeError(
            "Missing DB env vars. Set SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD."
        )
    # For simple apps, open/close per request is fine.
    # If you want pooling later, we can move to SQLAlchemy.
    return pyodbc.connect(conn_str)


def ensure_schema():
    """
    Creates tables if they don't exist.
    Safe to run on startup.
    """
    with get_connection() as cnxn:
        cur = cnxn.cursor()

        # Students master
        cur.execute(
            """
            IF OBJECT_ID('dbo.Students', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Students (
                    StudentId INT IDENTITY(1,1) PRIMARY KEY,
                    StudentCode NVARCHAR(50) NOT NULL UNIQUE,  -- barcode/QR value, e.g., STU-1001
                    StudentName NVARCHAR(200) NOT NULL,
                    DOB DATE NULL,
                    BusNumber NVARCHAR(50) NULL,
                    ParentPhone NVARCHAR(50) NULL,
                    IsActive BIT NOT NULL DEFAULT(1),
                    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END
            """
        )

        # Scan logs
        cur.execute(
            """
            IF OBJECT_ID('dbo.ScanLogs', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.ScanLogs (
                    ScanLogId INT IDENTITY(1,1) PRIMARY KEY,
                    StudentId INT NOT NULL,
                    StudentCode NVARCHAR(50) NOT NULL,
                    EventType NVARCHAR(20) NOT NULL,  -- RIDE, NO_CALL, NO_SHOW
                    BusNumber NVARCHAR(50) NULL,
                    DriverCode NVARCHAR(50) NULL,
                    AideCode NVARCHAR(50) NULL,
                    Notes NVARCHAR(500) NULL,
                    ParentContactRequested BIT NOT NULL DEFAULT(0),
                    ParentContacted BIT NOT NULL DEFAULT(0),
                    ContactMethod NVARCHAR(50) NULL,
                    ContactResult NVARCHAR(100) NULL,
                    EventTimeUtc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );

                ALTER TABLE dbo.ScanLogs
                    ADD CONSTRAINT FK_ScanLogs_Students
                    FOREIGN KEY (StudentId) REFERENCES dbo.Students(StudentId);
            END
            """
        )

        cnxn.commit()


@app.on_event("startup")
def on_startup():
    # If DB env vars aren't set yet, don't crash the app; /health will show it.
    try:
        ensure_schema()
    except Exception:
        # We'll surface in /health.
        pass


# -----------------------------
# Request/Response models
# -----------------------------
EventType = Literal["RIDE", "NO_CALL", "NO_SHOW"]


class ScanIn(BaseModel):
    student_code: str = Field(..., description="Barcode/QR code value (e.g., STU-1001)")
    event_type: EventType = Field(..., description="RIDE | NO_CALL | NO_SHOW")
    bus_number: Optional[str] = None
    driver_code: Optional[str] = None
    aide_code: Optional[str] = None
    notes: Optional[str] = None

    # Optional parent contact fields
    parent_contact_requested: bool = False
    parent_contacted: bool = False
    contact_method: Optional[str] = None        # e.g., "call", "text"
    contact_result: Optional[str] = None        # e.g., "no answer", "left voicemail"


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    """
    Health check.
    Shows DB connectivity status and a timestamp.
    """
    info: Dict[str, Any] = {"ok": True, "time_utc": utc_now_iso()}

    try:
        with get_connection() as cnxn:
            cur = cnxn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        info["db"] = "ok"
        info["trust_server_cert"] = TRUST_SERVER_CERT
        info["driver"] = ODBC_DRIVER
    except Exception as e:
        info["db"] = "error"
        info["detail"] = str(e)

    return info


@app.get("/students/{student_code}")
def get_student(student_code: str):
    """
    Returns student info by StudentCode (barcode/QR value).
    """
    try:
        with get_connection() as cnxn:
            cur = cnxn.cursor()
            cur.execute(
                """
                SELECT StudentCode, StudentName, DOB, BusNumber, ParentPhone, IsActive
                FROM dbo.Students
                WHERE StudentCode = ?
                """,
                (student_code,),
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Student not found")

            return {
                "ok": True,
                "student_code": row.StudentCode,
                "student_name": row.StudentName,
                "dob": row.DOB.isoformat() if row.DOB else None,
                "bus_number": row.BusNumber,
                "parent_phone": row.ParentPhone,
                "is_active": bool(row.IsActive),
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scan")
def create_scan(scan: ScanIn):
    """
    Creates a scan/log entry.
    - Looks up student by student_code.
    - Stores scan event in dbo.ScanLogs with UTC timestamp.
    """
    try:
        with get_connection() as cnxn:
            cur = cnxn.cursor()

            # Find student
            cur.execute(
                """
                SELECT StudentId, StudentCode, StudentName, DOB, BusNumber, ParentPhone, IsActive
                FROM dbo.Students
                WHERE StudentCode = ?
                """,
                (scan.student_code,),
            )
            student = cur.fetchone()
            if not student:
                raise HTTPException(status_code=404, detail="Student not found")

            if not bool(student.IsActive):
                raise HTTPException(status_code=400, detail="Student is not active")

            # Prefer student's assigned bus if request doesn't include one
            bus_number = scan.bus_number or student.BusNumber

            cur.execute(
                """
                INSERT INTO dbo.ScanLogs (
                    StudentId, StudentCode, EventType, BusNumber, DriverCode, AideCode,
                    Notes, ParentContactRequested, ParentContacted, ContactMethod, ContactResult
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    student.StudentId,
                    student.StudentCode,
                    scan.event_type,
                    bus_number,
                    scan.driver_code,
                    scan.aide_code,
                    scan.notes,
                    1 if scan.parent_contact_requested else 0,
                    1 if scan.parent_contacted else 0,
                    scan.contact_method,
                    scan.contact_result,
                ),
            )

            # Return the inserted row id
            cur.execute("SELECT SCOPE_IDENTITY() AS NewId;")
            new_id = cur.fetchone().NewId
            cnxn.commit()

            return {
                "ok": True,
                "scan_log_id": int(new_id),
                "student_code": student.StudentCode,
                "student_name": student.StudentName,
                "dob": student.DOB.isoformat() if student.DOB else None,
                "bus_number": bus_number,
                "parent_phone": student.ParentPhone,
                "event_type": scan.event_type,
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scanlogs/recent")
def recent_scans(limit: int = 50):
    """
    Returns the most recent scan logs (for admin/debug).
    """
    limit = max(1, min(limit, 200))
    try:
        with get_connection() as cnxn:
            cur = cnxn.cursor()
            cur.execute(
                f"""
                SELECT TOP ({limit})
                    ScanLogId, StudentCode, EventType, BusNumber, DriverCode, AideCode,
                    ParentContactRequested, ParentContacted, ContactMethod, ContactResult,
                    Notes, EventTimeUtc
                FROM dbo.ScanLogs
                ORDER BY EventTimeUtc DESC
                """
            )
            rows = cur.fetchall()

            data = []
            for r in rows:
                data.append(
                    {
                        "scan_log_id": r.ScanLogId,
                        "student_code": r.StudentCode,
                        "event_type": r.EventType,
                        "bus_number": r.BusNumber,
                        "driver_code": r.DriverCode,
                        "aide_code": r.AideCode,
                        "parent_contact_requested": bool(r.ParentContactRequested),
                        "parent_contacted": bool(r.ParentContacted),
                        "contact_method": r.ContactMethod,
                        "contact_result": r.ContactResult,
                        "notes": r.Notes,
                        "event_time_utc": r.EventTimeUtc.isoformat() if r.EventTimeUtc else None,
                    }
                )

            return {"ok": True, "count": len(data), "items": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

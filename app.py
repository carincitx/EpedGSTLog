# app.py — CLEAN FINAL VERSION (NO MERGE CONFLICTS)

from __future__ import annotations
import os, re, time
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, List, Optional

import pyodbc
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

app = FastAPI(title="SPEDSCAN")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(APP_DIR, "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

CT_TZ = ZoneInfo("America/Chicago") if ZoneInfo else None

ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
SQL_SERVER = os.getenv("SQL_SERVER", "").strip()
SQL_DATABASE = os.getenv("SQL_DATABASE", "").strip()
SQL_USERNAME = os.getenv("SQL_USERNAME", "").strip()
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "").strip()
NO_SHOW_MINUTES = int(os.getenv("NO_SHOW_MINUTES", "5"))

VALID_EVENT_TYPES = {"ARRIVED", "RIDE", "NO_CALL_NO_SHOW"}

EVENT_TYPE_ALIASES = {
    "arrived": "ARRIVED",
    "arrival": "ARRIVED",
    "ride": "RIDE",
    "boarded": "RIDE",
    "no_call_no_show": "NO_CALL_NO_SHOW",
    "noshow": "NO_CALL_NO_SHOW",
    "no_show": "NO_CALL_NO_SHOW",
    "no call no show": "NO_CALL_NO_SHOW",
}

class ScanRequest(BaseModel):
    student_code: str
    event_type: str
    driver_code: Optional[str] = None
    aide_code: Optional[str] = None
    stop_code: Optional[str] = None
    notes: Optional[str] = None

def utc_now():
    return datetime.now(timezone.utc)

def build_conn_str():
    return (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )

def get_conn():
    return pyodbc.connect(build_conn_str())

def to_ct(dt_utc: datetime) -> datetime:
    if CT_TZ:
        return dt_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(CT_TZ).replace(tzinfo=None)
    return dt_utc - timedelta(hours=6)

def today_ct() -> date:
    return to_ct(utc_now()).date()

def normalize_event_type(raw: str) -> str:
    raw = raw.strip().lower().replace(" ", "_").replace("-", "_")
    if raw in EVENT_TYPE_ALIASES:
        return EVENT_TYPE_ALIASES[raw]
    raw = raw.upper()
    if raw in VALID_EVENT_TYPES:
        return raw
    raise HTTPException(status_code=400, detail="Invalid event_type")

def serve_html(name: str):
    path = os.path.join(STATIC_DIR, name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Missing page")
    return FileResponse(path)

@app.get("/")
def index(): return serve_html("index.html")

@app.get("/qrcode")
@app.get("/qr")
@app.get("/qrcodes")
def qrcode(): return serve_html("qrcode.html")

@app.get("/today")
def today(): return serve_html("today.html")

@app.get("/health")
def health():
    try:
        with get_conn() as c:
            c.cursor().execute("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/students")
def students(bus: Optional[str] = None):
    with get_conn() as c:
        cur = c.cursor()
        if bus:
            rows = cur.execute("SELECT StudentCode, StudentName, BusNumber, DOB FROM dbo.Students WHERE BusNumber=?", bus).fetchall()
        else:
            rows = cur.execute("SELECT StudentCode, StudentName, BusNumber, DOB FROM dbo.Students").fetchall()
        return [dict(row._asdict()) for row in rows]

@app.get("/api/student/{code}")
def student(code: str):
    with get_conn() as c:
        cur = c.cursor()
        row = cur.execute("SELECT TOP 1 StudentCode, StudentName, BusNumber, DOB FROM dbo.Students WHERE StudentCode=?", code).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        return dict(row._asdict())

@app.get("/logs/today")
def logs_today():
    with get_conn() as c:
        cur = c.cursor()
        rows = cur.execute("SELECT StudentCode, EventType, BusNumber, EventTimeUTC FROM dbo.ScanEvents ORDER BY EventTimeUTC DESC").fetchall()
        out = []
        for r in rows:
            dt_ct = to_ct(r.EventTimeUTC)
            out.append({
                "StudentCode": r.StudentCode,
                "EventType": r.EventType,
                "BusNumber": r.BusNumber,
                "TimeCT": dt_ct.strftime("%I:%M %p").lstrip("0")
            })
        return out

@app.post("/scan")
def scan(req: ScanRequest):
    ev = normalize_event_type(req.event_type)
    with get_conn() as c:
        cur = c.cursor()
        cur.execute(
            "INSERT INTO dbo.ScanEvents (StudentCode, EventType, EventTimeUTC, DriverCode, AideCode, Notes) VALUES (?, ?, SYSUTCDATETIME(), ?, ?, ?)",
            req.student_code, ev, req.driver_code, req.aide_code, req.notes
        )
        c.commit()
    return {"ok": True}

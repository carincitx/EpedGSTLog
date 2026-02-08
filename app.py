import os
import pyodbc
from datetime import date
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Sped Bus Log API")

# Expected env var:
# SQL_CONN="Driver={ODBC Driver 18 for SQL Server};Server=tcp:YOURSERVER.database.windows.net,1433;Database=YOURDB;Uid=YOURUSER;Pwd=YOURPASS;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
SQL_CONN = os.getenv("SQL_CONN", "").strip()


def get_conn():
    if not SQL_CONN:
        raise RuntimeError("SQL_CONN environment variable is missing.")
    return pyodbc.connect(SQL_CONN)


class ScanIn(BaseModel):
    student_code: str = Field(..., alias="studentCode")
    event_type: str = Field(..., alias="eventType")  # RIDE or NO_SHOW
    driver_code: str = Field(..., alias="driverCode")
    aide_code: str = Field(..., alias="aideCode")
    stop_code: str | None = Field(None, alias="stopCode")
    notes: str | None = None


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/students/{student_code}")
def get_student(student_code: str):
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT StudentCode, StudentName, DOB, BusNumber, ParentPhone
                FROM dbo.Students
                WHERE StudentCode = ?
                """,
                (student_code,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Student not found")

            return {
                "studentCode": row.StudentCode,
                "studentName": row.StudentName,
                "dob": row.DOB.isoformat() if isinstance(row.DOB, date) else str(row.DOB),
                "busNumber": row.BusNumber,
                "parentPhone": row.ParentPhone,
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@app.post("/scan")
def create_scan(payload: ScanIn):
    event = payload.event_type.strip().upper()
    if event not in {"RIDE", "NO_SHOW"}:
        raise HTTPException(status_code=400, detail="eventType must be RIDE or NO_SHOW")

    # Pull student info (so ScanLogs stores snapshot fields too)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT StudentCode, StudentName, DOB, BusNumber
                FROM dbo.Students
                WHERE StudentCode = ?
                """,
                (payload.student_code,),
            )
            s = cur.fetchone()
            if not s:
                raise HTTPException(status_code=404, detail="Student not found")

            cur.execute(
                """
                INSERT INTO dbo.ScanLogs
                (StudentCode, StudentName, DOB, BusNumber, EventType, DriverCode, AideCode, StopCode, Notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    s.StudentCode,
                    s.StudentName,
                    s.DOB,
                    s.BusNumber,
                    event,
                    payload.driver_code,
                    payload.aide_code,
                    payload.stop_code,
                    payload.notes,
                ),
            )
            conn.commit()

            return {
                "ok": True,
                "studentCode": s.StudentCode,
                "eventType": event,
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


# Azure Container Apps expects port 8000 in your container setup
# Start command in Dockerfile should be:
# CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8000"]

"""
Jobs Monitor Dashboard endpoint.

GET /api/v1/jobs/monitor  — self-contained live HTML dashboard
"""

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.reports.templates.jobs_monitor import JobsMonitorPage

router = APIRouter(prefix="/jobs", tags=["Job Monitor"])


@router.get("/monitor", response_class=HTMLResponse)
async def jobs_monitor_dashboard(db: Session = Depends(get_db)):
    """Live jobs monitoring dashboard with SSE streaming and polling."""
    page = JobsMonitorPage()
    data = page.gather_data(db)
    return HTMLResponse(content=page.render_html(data))

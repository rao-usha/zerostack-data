"""
Jobs Monitor Dashboard — live job queue visibility.

Renders a self-contained HTML page with:
  1. KPI strip  — active, pending, success, failed counts
  2. Active jobs panel — live-updating rows with progress bars
  3. Job detail / log panel — terminal-style SSE event viewer
  4. Recent history table — filterable, paginated
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob
from app.core.models_queue import JobQueue, QueueJobStatus
from app.reports.design_system import (
    _esc,
    html_document,
    page_header,
    kpi_strip,
    kpi_card,
    section_start,
    section_end,
    data_table,
    callout,
    pill_badge,
    page_footer,
    DESIGN_SYSTEM_CSS,
    DARK_MODE_JS,
    CHART_RUNTIME_JS,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _enum_val(v) -> str:
    return v if isinstance(v, str) else v.value


def _duration(started, completed) -> Optional[int]:
    if started and completed:
        return round((completed - started).total_seconds())
    return None


def _fmt_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _status_pill(status: str) -> str:
    mapping = {
        "running": ("Running", "public"),
        "claimed": ("Claimed", "pe"),
        "pending": ("Pending", "sub"),
        "success": ("Success", "private"),
        "failed": ("Failed", "default"),
    }
    label, variant = mapping.get(status, (status.title(), "default"))
    if status == "failed":
        return f'<span class="pill" style="background:#fed7d7;color:#9b2c2c">{_esc(label)}</span>'
    return pill_badge(label, variant)


# ── main class ───────────────────────────────────────────────────────────────

class JobsMonitorPage:
    """Live jobs monitoring dashboard."""

    name = "jobs_monitor"
    description = "Real-time job queue monitoring dashboard"

    def gather_data(self, db: Session) -> Dict[str, Any]:
        """Query job_queue + ingestion_jobs for dashboard metrics."""

        now = datetime.utcnow()
        since_24h = now - timedelta(hours=24)

        # ── KPI counts from job_queue ────────────────────────────────────
        q_status_counts = dict(
            db.query(JobQueue.status, func.count(JobQueue.id))
            .group_by(JobQueue.status)
            .all()
        )
        active_count = sum(
            q_status_counts.get(s, 0)
            for s in (QueueJobStatus.CLAIMED, QueueJobStatus.RUNNING)
        )
        pending_count = q_status_counts.get(QueueJobStatus.PENDING, 0)

        # 24h success/failed from job_queue
        q_24h = dict(
            db.query(JobQueue.status, func.count(JobQueue.id))
            .filter(JobQueue.created_at >= since_24h)
            .group_by(JobQueue.status)
            .all()
        )
        q_success_24h = q_24h.get(QueueJobStatus.SUCCESS, 0)
        q_failed_24h = q_24h.get(QueueJobStatus.FAILED, 0)

        # 24h success/failed from ingestion_jobs
        try:
            i_24h_rows = (
                db.query(IngestionJob.status, func.count(IngestionJob.id))
                .filter(IngestionJob.created_at >= since_24h)
                .group_by(IngestionJob.status)
                .all()
            )
            i_24h = {_enum_val(s): c for s, c in i_24h_rows}
        except Exception:
            i_24h = {}

        success_24h = q_success_24h + i_24h.get("success", 0)
        failed_24h = q_failed_24h + i_24h.get("failed", 0)

        # ── Active jobs ──────────────────────────────────────────────────
        active_jobs_orm = (
            db.query(JobQueue)
            .filter(
                JobQueue.status.in_([QueueJobStatus.CLAIMED, QueueJobStatus.RUNNING])
            )
            .order_by(JobQueue.created_at.desc())
            .all()
        )
        active_jobs = [
            {
                "id": j.id,
                "job_type": _enum_val(j.job_type),
                "status": _enum_val(j.status),
                "worker_id": j.worker_id,
                "priority": j.priority,
                "progress_pct": j.progress_pct or 0,
                "progress_message": j.progress_message or "",
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "started_at": j.started_at.isoformat() if j.started_at else None,
            }
            for j in active_jobs_orm
        ]

        # ── Recent history (last 50, merged) ─────────────────────────────
        queue_jobs = (
            db.query(JobQueue).order_by(JobQueue.created_at.desc()).limit(50).all()
        )
        ingest_jobs = (
            db.query(IngestionJob)
            .order_by(IngestionJob.created_at.desc())
            .limit(50)
            .all()
        )

        history: List[Dict[str, Any]] = []
        for j in queue_jobs:
            history.append({
                "id": j.id,
                "table": "job_queue",
                "job_type": _enum_val(j.job_type),
                "status": _enum_val(j.status),
                "progress_pct": j.progress_pct,
                "progress_message": j.progress_message,
                "rows_inserted": None,
                "error_message": j.error_message,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "completed_at": j.completed_at,
                "duration_seconds": _duration(j.started_at, j.completed_at),
            })
        for j in ingest_jobs:
            history.append({
                "id": j.id,
                "table": "ingestion_jobs",
                "job_type": j.source,
                "status": _enum_val(j.status),
                "progress_pct": 100.0 if _enum_val(j.status) == "success" else None,
                "progress_message": None,
                "rows_inserted": j.rows_inserted,
                "error_message": j.error_message,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "completed_at": j.completed_at,
                "duration_seconds": _duration(j.started_at, j.completed_at),
            })

        epoch = datetime(1970, 1, 1)
        history.sort(key=lambda x: x["created_at"] or epoch, reverse=True)
        history = history[:50]

        return {
            "active_count": active_count,
            "pending_count": pending_count,
            "success_24h": success_24h,
            "failed_24h": failed_24h,
            "active_jobs": active_jobs,
            "history": history,
            "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        }

    # ── render ───────────────────────────────────────────────────────────

    def render_html(self, data: Dict[str, Any]) -> str:
        body = ""

        # ── Header ───────────────────────────────────────────────────────
        body += page_header(
            "Jobs Monitor",
            subtitle="Real-time job queue dashboard",
            badge=f"Updated {data['generated_at']}",
        )

        # ── KPI Strip ────────────────────────────────────────────────────
        body += '<div class="container">'
        cards = kpi_card("Active", str(data["active_count"]))
        cards += kpi_card("Pending", str(data["pending_count"]))
        cards += kpi_card("Success (24h)", str(data["success_24h"]), delta_dir="up" if data["success_24h"] else "neutral")
        failed_dir = "down" if data["failed_24h"] else "neutral"
        cards += kpi_card("Failed (24h)", str(data["failed_24h"]), delta_dir=failed_dir)
        body += kpi_strip(cards)

        # ── Section 1: Active Jobs ───────────────────────────────────────
        body += section_start(1, "Active Jobs", "active-jobs")
        body += '<div id="active-jobs-container">'
        if data["active_jobs"]:
            for job in data["active_jobs"]:
                body += self._render_job_row(job)
        else:
            body += '<p style="color:var(--gray-500);font-style:italic;text-align:center;padding:24px 0">No active jobs</p>'
        body += '</div>'
        body += section_end()

        # ── Section 2: Job Detail / Log Panel ────────────────────────────
        body += section_start(2, "Job Detail", "job-detail")
        body += '<div id="job-detail-panel">'
        body += '<p style="color:var(--gray-500);font-style:italic;text-align:center;padding:24px 0">Click an active job to view live log</p>'
        body += '</div>'
        body += section_end()

        # ── Section 3: Recent History ────────────────────────────────────
        body += section_start(3, "Recent History", "history")

        # Filter bar
        body += self._render_filter_bar(data["history"])

        # History table
        headers = ["ID", "Type", "Status", "Duration", "Rows", "Error", "Created"]
        rows = []
        for job in data["history"]:
            created = ""
            if job["created_at"]:
                created = job["created_at"].strftime("%m/%d %H:%M") if hasattr(job["created_at"], "strftime") else str(job["created_at"])[:16]
            rows.append([
                f'{job["table"][:1].upper()}#{job["id"]}',
                f'<span style="font-weight:600">{_esc(job["job_type"] or "—")}</span>',
                _status_pill(job["status"]),
                _fmt_duration(job["duration_seconds"]),
                str(job["rows_inserted"]) if job["rows_inserted"] is not None else "—",
                f'<span style="color:var(--accent-red);font-size:12px" title="{_esc(job["error_message"] or "")}">{_esc((job["error_message"] or "")[:40])}</span>' if job["error_message"] else "—",
                created,
            ])

        body += '<div id="history-table-container">'
        body += data_table(headers, rows, numeric_columns={3, 4})
        body += '</div>'

        body += section_end()

        # ── Footer ───────────────────────────────────────────────────────
        body += page_footer(
            notes=[
                "Active jobs and KPI counts refresh automatically via SSE and polling.",
                "Log events are accumulated from the moment the page opens.",
                "History shows the 50 most recent jobs from both job_queue and ingestion_jobs.",
            ],
            generated_line=f"Nexdata Jobs Monitor — {data['generated_at']}",
        )
        body += '</div>'  # close .container

        # ── Wrap in full document ────────────────────────────────────────
        return self._full_html(body)

    # ── private rendering helpers ────────────────────────────────────────

    def _render_job_row(self, job: Dict[str, Any]) -> str:
        pct = job.get("progress_pct", 0) or 0
        bar_color = "var(--accent-green)" if pct >= 100 else "var(--primary-light)"
        status = job.get("status", "unknown")
        pulse = " pulse" if status in ("running", "claimed") else ""

        return f"""<div class="job-row" data-job-id="{job['id']}" onclick="selectJob({job['id']})">
  <div class="job-row-left">
    <span class="status-dot {_esc(status)}{pulse}"></span>
    <span class="job-type">{_esc(job['job_type'])}</span>
    {_status_pill(status)}
  </div>
  <div class="job-row-right">
    <div class="progress-bar-track">
      <div class="progress-bar-fill" style="width:{pct:.0f}%;background:{bar_color}"></div>
    </div>
    <span class="progress-label">{pct:.0f}%</span>
  </div>
  <div class="job-row-message">{_esc(job.get('progress_message', ''))}</div>
</div>"""

    def _render_filter_bar(self, history: List[Dict]) -> str:
        statuses = sorted(set(j["status"] for j in history))
        types = sorted(set(j["job_type"] for j in history if j["job_type"]))

        status_opts = '<option value="">All Statuses</option>'
        for s in statuses:
            status_opts += f'<option value="{_esc(s)}">{_esc(s.title())}</option>'

        type_opts = '<option value="">All Types</option>'
        for t in types:
            type_opts += f'<option value="{_esc(t)}">{_esc(t)}</option>'

        return f"""<div class="filter-bar">
  <select id="filter-status" onchange="filterHistory()">{status_opts}</select>
  <select id="filter-type" onchange="filterHistory()">{type_opts}</select>
  <button class="load-more-btn" onclick="loadMore()">Load More</button>
</div>"""

    # ── full HTML document ───────────────────────────────────────────────

    def _full_html(self, body_content: str) -> str:
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jobs Monitor — Nexdata</title>
    <style>
{DESIGN_SYSTEM_CSS}
{_EXTRA_CSS}
    </style>
    <script>{DARK_MODE_JS}</script>
</head>
<body>
{body_content}
    <script>{CHART_RUNTIME_JS}</script>
    <script>
{_DASHBOARD_JS}
    </script>
</body>
</html>"""


# ── CSS ──────────────────────────────────────────────────────────────────────

_EXTRA_CSS = """
/* Job rows */
.job-row {
  display: grid;
  grid-template-columns: 1fr 200px;
  grid-template-rows: auto auto;
  gap: 4px 16px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--gray-100);
  cursor: pointer;
  transition: background 0.15s;
  border-left: 3px solid transparent;
}
.job-row:hover { background: var(--gray-50); }
.job-row.selected {
  background: var(--gray-50);
  border-left-color: var(--primary-light);
}
.job-row-left {
  display: flex;
  align-items: center;
  gap: 10px;
}
.job-row-right {
  display: flex;
  align-items: center;
  gap: 8px;
}
.job-row-message {
  grid-column: 1 / -1;
  font-size: 12px;
  color: var(--gray-500);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.job-type {
  font-weight: 600;
  font-size: 14px;
  color: var(--gray-800);
}

/* Status dots */
.status-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}
.status-dot.running { background: #38a169; }
.status-dot.claimed { background: #ed8936; }
.status-dot.pending { background: #a0aec0; }
.status-dot.success { background: #38a169; }
.status-dot.failed { background: #e53e3e; }

.status-dot.pulse {
  animation: pulse-dot 1.5s ease-in-out infinite;
}
@keyframes pulse-dot {
  0%, 100% { box-shadow: 0 0 0 0 rgba(56,161,105,0.4); }
  50% { box-shadow: 0 0 0 6px rgba(56,161,105,0); }
}
.status-dot.claimed.pulse {
  animation-name: pulse-dot-orange;
}
@keyframes pulse-dot-orange {
  0%, 100% { box-shadow: 0 0 0 0 rgba(237,137,54,0.4); }
  50% { box-shadow: 0 0 0 6px rgba(237,137,54,0); }
}

/* Progress bar */
.progress-bar-track {
  flex: 1;
  height: 6px;
  background: var(--gray-100);
  border-radius: 3px;
  overflow: hidden;
}
.progress-bar-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.4s ease;
}
.progress-label {
  font-size: 12px;
  font-weight: 600;
  color: var(--gray-500);
  min-width: 36px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

/* Log viewer */
.log-viewer {
  background: #1a202c;
  color: #a0aec0;
  font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 12px;
  line-height: 1.7;
  padding: 16px;
  border-radius: 8px;
  max-height: 400px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
[data-theme="dark"] .log-viewer {
  background: #0d1117;
  color: #c9d1d9;
}
.log-line {
  display: block;
  padding: 1px 0;
}
.log-line .ts {
  color: #718096;
  margin-right: 8px;
}
.log-line.event-job_started { color: #63b3ed; }
.log-line.event-job_progress { color: #a0aec0; }
.log-line.event-job_completed { color: #68d391; }
.log-line.event-job_failed { color: #fc8181; }

.log-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}
.log-header h3 {
  font-size: 15px;
  font-weight: 600;
  color: var(--gray-800);
}
.log-header .job-config {
  font-size: 12px;
  color: var(--gray-500);
}

.error-callout {
  background: #fed7d7;
  border-left: 4px solid #e53e3e;
  padding: 10px 14px;
  border-radius: 0 6px 6px 0;
  margin-top: 8px;
  font-size: 13px;
  color: #9b2c2c;
  word-break: break-word;
}
[data-theme="dark"] .error-callout {
  background: #3b1a1a;
  color: #fc8181;
}

/* Filter bar */
.filter-bar {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}
.filter-bar select {
  padding: 6px 12px;
  border: 1px solid var(--gray-200);
  border-radius: 6px;
  font-size: 13px;
  background: var(--white);
  color: var(--gray-800);
  cursor: pointer;
}
.filter-bar select:focus {
  outline: none;
  border-color: var(--primary-light);
  box-shadow: 0 0 0 2px rgba(43,108,176,0.15);
}
.load-more-btn {
  padding: 6px 16px;
  border: 1px solid var(--gray-200);
  border-radius: 6px;
  font-size: 13px;
  background: var(--white);
  color: var(--primary-light);
  cursor: pointer;
  font-weight: 600;
  margin-left: auto;
}
.load-more-btn:hover {
  background: var(--gray-50);
}

/* SSE connection indicator */
.sse-indicator {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--gray-500);
  padding: 4px 10px;
  border-radius: 12px;
  background: var(--gray-100);
}
.sse-indicator .sse-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #a0aec0;
}
.sse-indicator.connected .sse-dot { background: #38a169; }
.sse-indicator.error .sse-dot { background: #e53e3e; }
"""

# ── JavaScript ───────────────────────────────────────────────────────────────

_DASHBOARD_JS = """
// ── State ────────────────────────────────────────────────────────────────
var selectedJobId = null;
var jobLogs = {};       // { jobId: [ {ts, event, data} ] }
var sseConnected = false;
var historyOffset = 50;

// ── SSE Connection ───────────────────────────────────────────────────────
function connectSSE() {
    var es = new EventSource('/api/v1/job-queue/stream');

    es.onopen = function() {
        sseConnected = true;
        updateSSEIndicator();
    };

    es.onerror = function() {
        sseConnected = false;
        updateSSEIndicator();
    };

    ['job_started', 'job_progress', 'job_completed', 'job_failed'].forEach(function(eventName) {
        es.addEventListener(eventName, function(e) {
            var data;
            try { data = JSON.parse(e.data); } catch(err) { return; }
            var jobId = data.id || data.job_id;
            if (!jobId) return;

            // Accumulate log
            if (!jobLogs[jobId]) jobLogs[jobId] = [];
            jobLogs[jobId].push({
                ts: new Date().toLocaleTimeString(),
                event: eventName,
                message: data.progress_message || data.message || eventName,
                data: data
            });

            // Update active job row if visible
            updateJobRow(jobId, data, eventName);

            // Update KPI counters
            if (eventName === 'job_started' || eventName === 'job_completed' || eventName === 'job_failed') {
                refreshKPIs();
            }

            // Update log panel if this job is selected
            if (selectedJobId === jobId) {
                renderLogPanel(jobId);
            }
        });
    });
}

function updateSSEIndicator() {
    // The SSE status is reflected in the badge
}

// ── Polling Fallback ─────────────────────────────────────────────────────
function pollActiveJobs() {
    fetch('/api/v1/job-queue/active')
        .then(function(r) { return r.json(); })
        .then(function(jobs) {
            var container = document.getElementById('active-jobs-container');
            if (!container) return;

            if (jobs.length === 0) {
                container.innerHTML = '<p style="color:var(--gray-500);font-style:italic;text-align:center;padding:24px 0">No active jobs</p>';
                return;
            }

            var html = '';
            jobs.forEach(function(job) {
                var pct = job.progress_pct || 0;
                var barColor = pct >= 100 ? 'var(--accent-green)' : 'var(--primary-light)';
                var status = job.status || 'unknown';
                var pulse = (status === 'running' || status === 'claimed') ? ' pulse' : '';
                var selected = (selectedJobId === job.id) ? ' selected' : '';
                var statusPill = makeStatusPill(status);

                html += '<div class="job-row' + selected + '" data-job-id="' + job.id + '" onclick="selectJob(' + job.id + ')">' +
                    '<div class="job-row-left">' +
                    '<span class="status-dot ' + esc(status) + pulse + '"></span>' +
                    '<span class="job-type">' + esc(job.job_type) + '</span>' +
                    statusPill +
                    '</div>' +
                    '<div class="job-row-right">' +
                    '<div class="progress-bar-track"><div class="progress-bar-fill" style="width:' + pct.toFixed(0) + '%;background:' + barColor + '"></div></div>' +
                    '<span class="progress-label">' + pct.toFixed(0) + '%</span>' +
                    '</div>' +
                    '<div class="job-row-message">' + esc(job.progress_message || '') + '</div>' +
                    '</div>';
            });
            container.innerHTML = html;
        })
        .catch(function() {});
}

function refreshKPIs() {
    // Re-fetch active and history summary for KPI counts
    fetch('/api/v1/job-queue/active')
        .then(function(r) { return r.json(); })
        .then(function(jobs) {
            var kpiCards = document.querySelectorAll('.kpi-card .value');
            if (kpiCards.length >= 1) {
                kpiCards[0].textContent = jobs.length;
            }
        })
        .catch(function() {});
}

// ── Job Row Update (from SSE) ────────────────────────────────────────────
function updateJobRow(jobId, data, eventName) {
    var row = document.querySelector('.job-row[data-job-id="' + jobId + '"]');

    if (eventName === 'job_started' && !row) {
        // New job appeared — poll to get the full list
        pollActiveJobs();
        return;
    }

    if (!row) return;

    if (eventName === 'job_completed' || eventName === 'job_failed') {
        // Remove from active, refresh active list
        setTimeout(pollActiveJobs, 500);
        return;
    }

    // Update progress
    if (data.progress_pct !== undefined && data.progress_pct !== null) {
        var fill = row.querySelector('.progress-bar-fill');
        var label = row.querySelector('.progress-label');
        if (fill) fill.style.width = data.progress_pct.toFixed(0) + '%';
        if (label) label.textContent = data.progress_pct.toFixed(0) + '%';
    }

    if (data.progress_message) {
        var msg = row.querySelector('.job-row-message');
        if (msg) msg.textContent = data.progress_message;
    }
}

// ── Job Selection / Log Panel ────────────────────────────────────────────
function selectJob(jobId) {
    selectedJobId = jobId;

    // Highlight selected row
    document.querySelectorAll('.job-row').forEach(function(r) {
        r.classList.toggle('selected', parseInt(r.dataset.jobId) === jobId);
    });

    renderLogPanel(jobId);

    // Scroll to detail panel
    var panel = document.getElementById('job-detail');
    if (panel) panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function renderLogPanel(jobId) {
    var panel = document.getElementById('job-detail-panel');
    if (!panel) return;

    var logs = jobLogs[jobId] || [];

    var html = '<div class="log-header">' +
        '<h3>Job #' + jobId + '</h3>' +
        '<span class="sse-indicator' + (sseConnected ? ' connected' : '') + '">' +
        '<span class="sse-dot"></span>' +
        (sseConnected ? 'Live' : 'Reconnecting...') +
        '</span>' +
        '</div>';

    html += '<div class="log-viewer" id="log-viewer">';
    if (logs.length === 0) {
        html += '<span class="log-line" style="color:#718096">Waiting for events... (events are captured from page load)</span>';
    } else {
        logs.forEach(function(entry) {
            html += '<span class="log-line event-' + entry.event + '">' +
                '<span class="ts">[' + entry.ts + ']</span>' +
                esc(entry.message) +
                '</span>\\n';
        });
    }
    html += '</div>';

    // Show error if last event was a failure
    if (logs.length > 0) {
        var last = logs[logs.length - 1];
        if (last.event === 'job_failed' && last.data && last.data.error_message) {
            html += '<div class="error-callout"><strong>Error:</strong> ' + esc(last.data.error_message) + '</div>';
        }
    }

    panel.innerHTML = html;

    // Auto-scroll log to bottom
    var viewer = document.getElementById('log-viewer');
    if (viewer) viewer.scrollTop = viewer.scrollHeight;
}

// ── History Filtering ────────────────────────────────────────────────────
function filterHistory() {
    var status = document.getElementById('filter-status').value;
    var type = document.getElementById('filter-type').value;
    var table = document.querySelector('#history-table-container .data-table');
    if (!table) return;

    var rows = table.querySelectorAll('tbody tr');
    rows.forEach(function(row) {
        var cells = row.querySelectorAll('td');
        if (cells.length < 3) return;

        var rowType = cells[1].textContent.trim();
        var rowStatus = cells[2].textContent.trim().toLowerCase();

        var showStatus = !status || rowStatus === status;
        var showType = !type || rowType === type;

        row.style.display = (showStatus && showType) ? '' : 'none';
    });
}

function loadMore() {
    fetch('/api/v1/job-queue/history?limit=50&offset=' + historyOffset)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (!data.jobs || data.jobs.length === 0) return;
            historyOffset += data.jobs.length;

            var tbody = document.querySelector('#history-table-container .data-table tbody');
            if (!tbody) return;

            data.jobs.forEach(function(job) {
                var tr = document.createElement('tr');
                var created = job.created_at ? job.created_at.substring(5, 16).replace('T', ' ') : '';
                var dur = formatDuration(job.duration_seconds);
                var rows = job.rows_inserted !== null && job.rows_inserted !== undefined ? String(job.rows_inserted) : '\\u2014';
                var errText = job.error_message ? job.error_message.substring(0, 40) : '\\u2014';
                var errHtml = job.error_message
                    ? '<span style="color:var(--accent-red);font-size:12px" title="' + esc(job.error_message) + '">' + esc(errText) + '</span>'
                    : '\\u2014';

                tr.innerHTML = '<td>' + (job.table || 'Q').charAt(0).toUpperCase() + '#' + job.id + '</td>' +
                    '<td><span style="font-weight:600">' + esc(job.job_type || '\\u2014') + '</span></td>' +
                    '<td>' + makeStatusPill(job.status) + '</td>' +
                    '<td class="right">' + dur + '</td>' +
                    '<td class="right">' + rows + '</td>' +
                    '<td>' + errHtml + '</td>' +
                    '<td>' + created + '</td>';
                tbody.appendChild(tr);
            });
        })
        .catch(function() {});
}

// ── Helpers ──────────────────────────────────────────────────────────────
function esc(s) {
    if (!s) return '';
    var d = document.createElement('div');
    d.appendChild(document.createTextNode(String(s)));
    return d.innerHTML;
}

function makeStatusPill(status) {
    var map = {
        running: ['Running', 'pill-public'],
        claimed: ['Claimed', 'pill-pe'],
        pending: ['Pending', 'pill-sub'],
        success: ['Success', 'pill-private'],
        failed:  ['Failed', '']
    };
    var info = map[status] || [status, 'pill-default'];
    if (status === 'failed') {
        return '<span class="pill" style="background:#fed7d7;color:#9b2c2c">' + info[0] + '</span>';
    }
    return '<span class="pill ' + info[1] + '">' + info[0] + '</span>';
}

function formatDuration(sec) {
    if (sec === null || sec === undefined) return '\\u2014';
    if (sec < 60) return sec + 's';
    var m = Math.floor(sec / 60);
    var s = sec % 60;
    if (m < 60) return m + 'm ' + s + 's';
    var h = Math.floor(m / 60);
    m = m % 60;
    return h + 'h ' + m + 'm';
}

// ── Init ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', function() {
    connectSSE();
    setInterval(pollActiveJobs, 10000);
});
"""

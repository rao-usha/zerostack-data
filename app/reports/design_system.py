"""
Nexdata Report Design System.

Shared CSS, JS, Chart.js helpers, and HTML component functions.
Follows the report-style conventions in .claude/skills/report-style/.
"""

import json as _json
from typing import Optional, List, Dict, Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHART_JS_CDN = "https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"

# Chart color palette — matches .claude/skills/report-style/chartjs_conventions.md
BLUE = "#2b6cb0"
BLUE_LIGHT = "#63b3ed"
BLUE_DARK = "#2c5282"
ORANGE = "#ed8936"
GREEN = "#38a169"
RED = "#e53e3e"
GRAY = "#a0aec0"
PURPLE = "#805ad5"
TEAL = "#319795"
PINK = "#d53f8c"

CHART_COLORS = [BLUE, BLUE_LIGHT, ORANGE, GREEN, GRAY, PURPLE, TEAL, PINK, BLUE_DARK, RED]

# ---------------------------------------------------------------------------
# CSS — from .claude/skills/report-style/css_reference.md + Nexdata extras
# ---------------------------------------------------------------------------

DESIGN_SYSTEM_CSS = """
:root {
  --primary: #1a365d;
  --primary-light: #2b6cb0;
  --accent: #ed8936;
  --accent-red: #e53e3e;
  --accent-green: #38a169;
  --gray-50: #f7fafc;
  --gray-100: #edf2f7;
  --gray-200: #e2e8f0;
  --gray-300: #cbd5e0;
  --gray-500: #718096;
  --gray-700: #4a5568;
  --gray-800: #2d3748;
  --gray-900: #1a202c;
  --white: #ffffff;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  color: var(--gray-800);
  background: var(--gray-50);
  line-height: 1.6;
}

.page-header {
  background: linear-gradient(135deg, var(--primary) 0%, var(--primary-light) 100%);
  color: var(--white);
  padding: 40px 0;
}
.page-header .container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
}
.page-header h1 { font-size: 28px; font-weight: 700; }
.page-header .subtitle { font-size: 16px; opacity: 0.85; margin-top: 4px; }
.page-header .badge {
  background: rgba(255,255,255,0.2);
  border: 1px solid rgba(255,255,255,0.3);
  padding: 8px 20px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
}

.container { max-width: 1200px; margin: 0 auto; padding: 0 24px; }

/* KPI Strip */
.kpi-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px;
  margin: -32px 0 32px 0;
  position: relative;
  z-index: 10;
}
.kpi-card {
  background: var(--white);
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 4px 16px rgba(0,0,0,0.08);
  text-align: center;
}
.kpi-card .label {
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--gray-500);
  font-weight: 600;
}
.kpi-card .value {
  font-size: 28px;
  font-weight: 700;
  color: var(--primary);
  margin: 4px 0;
}
.kpi-card .delta {
  font-size: 13px;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 3px;
}
.delta.up { color: var(--accent-green); }
.delta.down { color: var(--accent-red); }
.delta.neutral { color: var(--gray-500); }

/* Table of Contents */
.toc {
  background: var(--white);
  border-radius: 10px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  margin-bottom: 24px;
  padding: 20px 24px;
}
.toc h2 {
  font-size: 14px;
  font-weight: 700;
  color: var(--gray-500);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}
.toc-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 6px 24px;
}
.toc a {
  display: flex;
  align-items: center;
  gap: 10px;
  text-decoration: none;
  color: var(--gray-700);
  font-size: 14px;
  padding: 6px 8px;
  border-radius: 6px;
  transition: background 0.15s;
}
.toc a:hover { background: var(--gray-100); }
.toc a .toc-num {
  background: var(--primary);
  color: var(--white);
  width: 22px;
  height: 22px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  font-weight: 700;
  flex-shrink: 0;
}

/* Sections */
.section {
  background: var(--white);
  border-radius: 10px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  margin-bottom: 24px;
  overflow: hidden;
}
.section-header {
  padding: 20px 24px 0 24px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.section-header h2 {
  font-size: 18px;
  font-weight: 700;
  color: var(--primary);
}
.section-number {
  background: var(--primary);
  color: var(--white);
  width: 28px;
  height: 28px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 13px;
  font-weight: 700;
  flex-shrink: 0;
}
.section-body { padding: 16px 24px 24px 24px; }
.section-body p { color: var(--gray-700); font-size: 14px; margin-bottom: 12px; }
.section-body .count-badge {
  background: var(--gray-100);
  color: var(--gray-500);
  font-size: 12px;
  font-weight: 600;
  padding: 2px 10px;
  border-radius: 12px;
  margin-left: 8px;
  vertical-align: middle;
}

/* Charts */
.chart-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
  margin: 12px 0;
}
@media (max-width: 768px) { .chart-row { grid-template-columns: 1fr; } }
.chart-container { position: relative; width: 100%; }
.chart-container.tall { height: 360px; }
.chart-container.medium { height: 300px; }
.chart-container.short { height: 240px; }
.chart-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--gray-700);
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.3px;
}
.chart-fallback { display: none; padding: 12px 0; }
.chart-fallback .fb-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  font-size: 13px;
  color: var(--gray-700);
}
.chart-fallback .fb-label { min-width: 160px; }
.chart-fallback .fb-bar-track {
  flex: 1; height: 8px;
  background: var(--gray-100);
  border-radius: 4px;
  overflow: hidden;
}
.chart-fallback .fb-bar-fill { height: 100%; border-radius: 4px; }
.chart-fallback .fb-value {
  min-width: 60px;
  text-align: right;
  font-weight: 600;
  color: var(--gray-900);
}

/* Tables */
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  margin-top: 12px;
}
.data-table thead th {
  background: var(--gray-100);
  color: var(--gray-700);
  font-weight: 600;
  padding: 10px 12px;
  text-align: left;
  border-bottom: 2px solid var(--gray-200);
  white-space: nowrap;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.3px;
}
.data-table thead th.right { text-align: right; }
.data-table tbody td {
  padding: 9px 12px;
  border-bottom: 1px solid var(--gray-100);
  vertical-align: middle;
}
.data-table tbody td.right { text-align: right; font-variant-numeric: tabular-nums; }
.data-table tbody td.bold { font-weight: 600; }
.data-table tbody tr:last-child td { border-bottom: none; }
.data-table tbody tr:hover { background: var(--gray-50); }
.data-table tfoot td {
  padding: 10px 12px;
  border-top: 2px solid var(--gray-300);
  font-weight: 700;
  background: var(--gray-50);
}
.data-table tfoot td.right { text-align: right; font-variant-numeric: tabular-nums; }
.company-name { font-weight: 600; color: var(--gray-900); }
.ticker {
  font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 12px;
  color: var(--primary-light);
  font-weight: 500;
}
.change-positive { color: var(--accent-green); font-weight: 600; }
.change-negative { color: var(--accent-red); font-weight: 600; }
.change-neutral { color: var(--gray-500); }

/* Status pills */
.pill {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
  white-space: nowrap;
}
.pill-public  { background: #dbeafe; color: #1d4ed8; }
.pill-pe      { background: #fef3c7; color: #92400e; }
.pill-sub     { background: var(--gray-100); color: var(--gray-700); }
.pill-private { background: #f0fdf4; color: #166534; }
.pill-default { background: var(--gray-100); color: var(--gray-700); }

/* Call-out boxes */
.callout {
  border-left: 4px solid var(--primary-light);
  background: #ebf8ff;
  padding: 12px 16px;
  border-radius: 0 6px 6px 0;
  margin: 12px 0;
  font-size: 13px;
  color: var(--gray-700);
}
.callout.warn {
  border-left-color: var(--accent);
  background: #fffaf0;
}
.callout.good {
  border-left-color: var(--accent-green);
  background: #f0fff4;
}
.callout strong { color: var(--gray-900); }

/* Custom chart legend */
.chart-legend { padding: 8px 0; }
.legend-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 7px 0;
  border-bottom: 1px solid var(--gray-100);
  font-size: 13px;
}
.legend-item:last-child { border-bottom: none; }
.legend-dot { width: 10px; height: 10px; border-radius: 3px; flex-shrink: 0; }
.legend-label { flex: 1; color: var(--gray-700); font-weight: 500; }
.legend-value {
  font-weight: 600; color: var(--gray-900);
  font-variant-numeric: tabular-nums;
  min-width: 40px; text-align: right;
}
.legend-pct { color: var(--gray-500); font-size: 12px; min-width: 36px; text-align: right; }

/* Segment cards */
.segments-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 16px;
  margin-top: 16px;
}
.segment-card {
  background: var(--white);
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}
.segment-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 4px;
}
.segment-name { font-weight: 600; color: var(--gray-900); font-size: 14px; }
.segment-aum  { font-weight: 700; color: var(--primary); font-size: 16px; }
.segment-strategy { color: var(--gray-500); font-size: 12px; margin-bottom: 12px; }
.segment-bar-track {
  height: 6px;
  background: var(--gray-100);
  border-radius: 3px;
  overflow: hidden;
}
.segment-bar-fill {
  height: 100%;
  background: var(--primary-light);
  border-radius: 3px;
}
.segment-pct { font-size: 12px; color: var(--gray-500); margin-top: 6px; }
.segment-total {
  text-align: right;
  font-weight: 700;
  color: var(--gray-900);
  margin-top: 16px;
  font-size: 14px;
}

/* Profile / team cards */
.team-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
}
.profile-card {
  background: var(--white);
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  transition: box-shadow 0.15s ease;
}
.profile-card:hover { box-shadow: 0 4px 16px rgba(0,0,0,0.08); }
.card-header {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 10px;
}
.avatar {
  width: 44px; height: 44px;
  border-radius: 50%;
  background: var(--primary);
  color: var(--white);
  font-size: 14px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.card-name { font-size: 15px; font-weight: 600; color: var(--gray-900); }
.card-name a { color: var(--primary-light); text-decoration: none; }
.card-name a:hover { text-decoration: underline; }
.card-title { color: var(--gray-500); font-size: 13px; margin: 1px 0 0; }
.card-badges { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 8px; }
.card-badge {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
}
.badge-seniority { background: var(--primary); color: var(--white); }
.badge-dept { background: #eff6ff; color: #1e40af; border: 1px solid #bfdbfe; }
.badge-tenure { background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }
.card-bio { color: var(--gray-500); font-size: 13px; line-height: 1.6; margin: 8px 0 4px; }
.card-section { margin-top: 10px; }
.section-label {
  font-size: 11px; font-weight: 700;
  color: var(--gray-500);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.exp-list, .edu-list {
  margin: 4px 0 0 16px; padding: 0;
  font-size: 13px; color: var(--gray-500);
}
.exp-list li, .edu-list li { margin-bottom: 3px; }
.exp-years { color: var(--gray-300); }

/* Footer */
.page-footer {
  padding: 24px 0 40px 0;
  text-align: center;
  color: var(--gray-500);
  font-size: 12px;
}
.page-footer .notes {
  max-width: 800px;
  margin: 0 auto;
  text-align: left;
  background: var(--white);
  border-radius: 10px;
  padding: 20px 24px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  margin-bottom: 16px;
}
.page-footer .notes h3 {
  font-size: 14px;
  color: var(--gray-700);
  margin-bottom: 8px;
}
.page-footer .notes ul { padding-left: 20px; }
.page-footer .notes li { margin-bottom: 4px; font-size: 12px; color: var(--gray-500); }

/* Footnote */
.footnote {
  color: var(--gray-500);
  font-size: 12px;
  margin-top: 10px;
  font-style: italic;
}

/* Utility */
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
@media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
.mt-16 { margin-top: 16px; }
.mb-8 { margin-bottom: 8px; }

/* Theme toggle switch */
.theme-toggle {
  display: flex;
  align-items: center;
  cursor: pointer;
  user-select: none;
}
.theme-toggle .toggle-track {
  width: 48px;
  height: 26px;
  background: rgba(255,255,255,0.2);
  border-radius: 13px;
  position: relative;
  transition: background 0.2s;
}
.theme-toggle .toggle-thumb {
  width: 22px;
  height: 22px;
  background: #fff;
  border-radius: 50%;
  position: absolute;
  top: 2px;
  left: 2px;
  transition: transform 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  line-height: 1;
  color: #ed8936;
}
.theme-toggle .toggle-thumb::after { content: '\\2600\\FE0E'; }

/* ── Dark Mode ── */
[data-theme="dark"] {
  --primary: #63b3ed;
  --primary-light: #90cdf4;
  --accent: #ed8936;
  --accent-red: #fc8181;
  --accent-green: #68d391;
  --gray-50: #1a202c;
  --gray-100: #2d3748;
  --gray-200: #4a5568;
  --gray-300: #718096;
  --gray-500: #a0aec0;
  --gray-700: #e2e8f0;
  --gray-800: #edf2f7;
  --gray-900: #f7fafc;
  --white: #2d3748;
}
[data-theme="dark"] .page-header {
  background: linear-gradient(135deg, #1a365d 0%, #2a4365 100%);
  color: #f7fafc;
}
[data-theme="dark"] .page-header .badge {
  background: rgba(255,255,255,0.1);
  border-color: rgba(255,255,255,0.2);
}
[data-theme="dark"] .kpi-card .value { color: #90cdf4; }
[data-theme="dark"] .section-number { background: #63b3ed; color: #1a202c; }
[data-theme="dark"] .kpi-card,
[data-theme="dark"] .section,
[data-theme="dark"] .toc,
[data-theme="dark"] .page-footer .notes {
  box-shadow: 0 2px 8px rgba(0,0,0,0.3);
}
[data-theme="dark"] .callout {
  border-left-color: #63b3ed;
  background: #2a4365;
  color: #e2e8f0;
}
[data-theme="dark"] .callout.warn {
  border-left-color: var(--accent);
  background: #744210;
}
[data-theme="dark"] .callout.good {
  border-left-color: var(--accent-green);
  background: #22543d;
}
[data-theme="dark"] .callout strong { color: #f7fafc; }
[data-theme="dark"] .toc a .toc-num { background: #63b3ed; color: #1a202c; }
[data-theme="dark"] .pill-public  { background: #1e3a5f; color: #93c5fd; }
[data-theme="dark"] .pill-pe      { background: #422006; color: #fbbf24; }
[data-theme="dark"] .pill-sub     { background: #334155; color: #cbd5e1; }
[data-theme="dark"] .pill-private { background: #14332a; color: #6ee7b7; }
[data-theme="dark"] .pill-default { background: #334155; color: #cbd5e1; }
[data-theme="dark"] .badge-seniority { background: #334155; color: #e2e8f0; }
[data-theme="dark"] .badge-dept { background: #1e3a5f; color: #93c5fd; border-color: #1e3a5f; }
[data-theme="dark"] .badge-tenure { background: #14332a; color: #6ee7b7; border-color: #14332a; }
[data-theme="dark"] .theme-toggle .toggle-track { background: rgba(144,205,244,0.3); }
[data-theme="dark"] .theme-toggle .toggle-thumb { transform: translateX(22px); color: #63b3ed; }
[data-theme="dark"] .theme-toggle .toggle-thumb::after { content: '\\263E\\FE0E'; }

/* ── Print ── */
@media print {
  .theme-toggle { display: none !important; }
  .kpi-card, .section, .toc, .profile-card, .segment-card, .page-footer .notes {
    box-shadow: none !important;
    border: 1px solid #e2e8f0;
  }
  .page-header { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}

/* ── Responsive ── */
@media (max-width: 768px) {
  .kpi-strip { grid-template-columns: repeat(2, 1fr); }
  .team-grid { grid-template-columns: 1fr; }
  .segments-grid { grid-template-columns: 1fr; }
}
"""

# ---------------------------------------------------------------------------
# JavaScript
# ---------------------------------------------------------------------------

# Early theme detection (runs in <head> before paint)
DARK_MODE_JS = """
(function() {
    var stored = localStorage.getItem('nexdata-theme');
    if (stored === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
    } else if (!stored && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        document.documentElement.setAttribute('data-theme', 'dark');
    }
})();
"""

# Chart.js setup, fallback, and dark mode re-theming IIFE
CHART_RUNTIME_JS = """
window.CHARTJS_AVAILABLE = false;

function renderChartOrFallback(canvasId, config) {
    if (window.CHARTJS_AVAILABLE && typeof Chart !== 'undefined') {
        var ctx = document.getElementById(canvasId);
        if (ctx) new Chart(ctx, config);
    } else {
        var wrapper = document.getElementById(canvasId + '_fallback');
        if (wrapper) wrapper.style.display = 'block';
        var canvas = document.getElementById(canvasId);
        if (canvas) canvas.style.display = 'none';
    }
}

function _nexdataSetChartDefaults() {
    if (typeof Chart === 'undefined') return;
    Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.color = '#4a5568';
}

/* Dark mode re-theming IIFE — matches chartjs_conventions.md */
(function() {
    var LIGHT = { text: '#4a5568', grid: '#edf2f7', doughnutBorder: '#ffffff' };
    var DARK  = { text: '#a0aec0', grid: '#4a5568', doughnutBorder: '#2d3748' };

    function applyTheme(isDark) {
        document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
        localStorage.setItem('nexdata-theme', isDark ? 'dark' : 'light');
        var toggle = document.getElementById('themeToggle');
        if (toggle) toggle.setAttribute('aria-checked', isDark ? 'true' : 'false');
        if (typeof Chart === 'undefined') return;
        var palette = isDark ? DARK : LIGHT;
        Chart.defaults.color = palette.text;
        Object.values(Chart.instances).forEach(function(chart) {
            var scales = chart.options.scales || {};
            Object.keys(scales).forEach(function(key) {
                var s = scales[key];
                if (s.grid) s.grid.color = s.grid.display === false ? undefined : palette.grid;
                if (s.ticks) s.ticks.color = palette.text;
            });
            var plugins = chart.options.plugins || {};
            if (plugins.legend && plugins.legend.labels) plugins.legend.labels.color = palette.text;
            if (chart.config.type === 'doughnut') {
                chart.data.datasets.forEach(function(ds) { ds.borderColor = palette.doughnutBorder; });
            }
            chart.update('none');
        });
    }

    var systemDark = window.matchMedia('(prefers-color-scheme: dark)');
    var userOverride = null;
    function currentIsDark() {
        if (userOverride !== null) return userOverride;
        var stored = localStorage.getItem('nexdata-theme');
        if (stored) return stored === 'dark';
        return systemDark.matches;
    }

    document.addEventListener('DOMContentLoaded', function() {
        applyTheme(currentIsDark());
    });

    var toggleEl = document.getElementById('themeToggle');
    if (toggleEl) {
        toggleEl.addEventListener('click', function() { userOverride = !currentIsDark(); applyTheme(userOverride); });
        toggleEl.addEventListener('keydown', function(e) {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleEl.click(); }
        });
    }
    systemDark.addEventListener('change', function() { if (userOverride === null) applyTheme(systemDark.matches); });
})();
"""


# ---------------------------------------------------------------------------
# Helper Functions — HTML Components
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Minimal HTML escaping."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def html_document(
    title: str,
    body_content: str,
    charts_js: str = "",
    extra_css: str = "",
) -> str:
    """Full <!DOCTYPE html> wrapper with design system CSS, dark mode, and Chart.js."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{_esc(title)}</title>
    <style>{DESIGN_SYSTEM_CSS}{extra_css}</style>
    <script>{DARK_MODE_JS}</script>
</head>
<body>
{body_content}
    <script>{CHART_RUNTIME_JS}</script>
    <script>
    function _nexdataInitCharts() {{
        _nexdataSetChartDefaults();
        {charts_js}
    }}
    </script>
    <script src="{CHART_JS_CDN}" onload="window.CHARTJS_AVAILABLE=true;_nexdataInitCharts();" onerror="document.querySelectorAll('.chart-fallback').forEach(function(e){{e.style.display='block'}});document.querySelectorAll('.chart-container canvas').forEach(function(e){{e.style.display='none'}})"></script>
    <script>
    if (window.CHARTJS_AVAILABLE && typeof Chart !== 'undefined') {{
        _nexdataInitCharts();
    }}
    </script>
</body>
</html>"""


def page_header(
    title: str,
    subtitle: Optional[str] = None,
    badge: Optional[str] = None,
) -> str:
    """Page header with gradient banner, dark mode toggle, and optional badge."""
    subtitle_html = f'\n      <div class="subtitle">{_esc(subtitle)}</div>' if subtitle else ""
    badge_html = f'\n      <div class="badge">{_esc(badge)}</div>' if badge else ""

    return f"""<div class="page-header">
  <div class="container">
    <div>
      <h1>{_esc(title)}</h1>{subtitle_html}
    </div>
    <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">
      <div class="theme-toggle" id="themeToggle" role="switch" aria-label="Toggle dark mode" aria-checked="false" tabindex="0">
        <div class="toggle-track"><div class="toggle-thumb"></div></div>
      </div>{badge_html}
    </div>
  </div>
</div>"""


def kpi_strip(cards_html: str) -> str:
    """Wrap kpi_card outputs in a KPI strip grid."""
    return f'<div class="kpi-strip">{cards_html}</div>'


def kpi_card(
    label: str,
    value: str,
    delta: Optional[str] = None,
    delta_dir: str = "neutral",
) -> str:
    """Single KPI card. delta_dir: 'up', 'down', or 'neutral'."""
    delta_html = ""
    if delta:
        delta_html = f'\n    <div class="delta {_esc(delta_dir)}">{_esc(delta)}</div>'
    return f"""<div class="kpi-card">
    <div class="label">{_esc(label)}</div>
    <div class="value">{_esc(str(value))}</div>{delta_html}
</div>"""


def toc(items: List[Dict[str, Any]]) -> str:
    """Table of contents. items = [{"number": 1, "id": "section-id", "title": "Title"}]."""
    links = ""
    for item in items:
        num = item.get("number", "")
        sid = item.get("id", "")
        title = item.get("title", "")
        links += f'<a href="#{_esc(sid)}"><span class="toc-num">{num}</span> {_esc(title)}</a>\n'
    return f"""<div class="toc">
    <h2>Contents</h2>
    <div class="toc-grid">
        {links}
    </div>
</div>"""


def section_start(number: int, title: str, section_id: str) -> str:
    """Open a numbered section card. Must be closed with section_end()."""
    return f"""<div class="section" id="{_esc(section_id)}">
    <div class="section-header">
        <div class="section-number">{number}</div>
        <h2>{_esc(title)}</h2>
    </div>
    <div class="section-body">"""


def section_end() -> str:
    """Close a numbered section card."""
    return """    </div>
</div>"""


def data_table(
    headers: List[str],
    rows: List[List[str]],
    numeric_columns: Optional[set] = None,
    footer_row: Optional[List[str]] = None,
) -> str:
    """HTML data table with optional numeric alignment and footer row."""
    if numeric_columns is None:
        numeric_columns = set()

    th_cells = "".join(
        f'<th class="right">{_esc(h)}</th>' if i in numeric_columns else f"<th>{_esc(h)}</th>"
        for i, h in enumerate(headers)
    )

    if not rows:
        tbody = f'<tr><td colspan="{len(headers)}" style="text-align:center;color:var(--gray-500);padding:32px;font-style:italic">No data available</td></tr>'
    else:
        row_htmls = []
        for row in rows:
            cells = "".join(
                f'<td class="right">{cell}</td>' if i in numeric_columns else f"<td>{cell}</td>"
                for i, cell in enumerate(row)
            )
            row_htmls.append(f"<tr>{cells}</tr>")
        tbody = "\n".join(row_htmls)

    tfoot = ""
    if footer_row:
        foot_cells = "".join(
            f'<td class="right">{_esc(cell)}</td>' if i in numeric_columns else f"<td>{_esc(cell)}</td>"
            for i, cell in enumerate(footer_row)
        )
        tfoot = f"\n    <tfoot><tr>{foot_cells}</tr></tfoot>"

    return f"""<table class="data-table">
    <thead><tr>{th_cells}</tr></thead>
    <tbody>{tbody}</tbody>{tfoot}
</table>"""


def callout(content: str, variant: str = "info") -> str:
    """Callout box. Variants: 'info' (blue), 'warn' (orange), 'good' (green).
    Content should include <strong>Label:</strong> prefix."""
    cls = ""
    if variant == "warn":
        cls = " warn"
    elif variant == "good":
        cls = " good"
    return f'<div class="callout{cls}">{content}</div>'


def pill_badge(text: str, variant: str = "default") -> str:
    """Inline pill badge. Variants: public, pe, sub, private, default."""
    return f'<span class="pill pill-{_esc(variant)}">{_esc(text)}</span>'


def profile_card(
    name: str,
    title: str,
    initials: str,
    badges: Optional[List[str]] = None,
    bio: Optional[str] = None,
    experience: Optional[List[Dict]] = None,
    education: Optional[List[Dict]] = None,
    linkedin: Optional[str] = None,
) -> str:
    """Team member profile card."""
    name_html = _esc(name)
    if linkedin:
        name_html = f'<a href="{_esc(linkedin)}" target="_blank">{_esc(name)}</a>'

    badges_html = ""
    if badges:
        badges_html = '<div class="card-badges">' + "".join(badges) + "</div>"

    bio_html = ""
    if bio:
        bio_html = f'<p class="card-bio">{_esc(bio)}</p>'

    exp_html = ""
    if experience:
        exp_items = ""
        for exp in experience[:3]:
            years = ""
            if exp.get("start_year") and exp.get("end_year"):
                years = f' <span class="exp-years">({exp["start_year"]}\u2013{exp["end_year"]})</span>'
            elif exp.get("start_year"):
                years = f' <span class="exp-years">({exp["start_year"]}\u2013)</span>'
            exp_items += f'<li>{_esc(exp.get("title", ""))} at {_esc(exp.get("company", ""))}{years}</li>'
        exp_html = f'<div class="card-section"><span class="section-label">Prior Experience</span><ul class="exp-list">{exp_items}</ul></div>'

    edu_html = ""
    if education:
        edu_items = ""
        for edu in education:
            degree = edu.get("degree") or ""
            field = edu.get("field")
            inst = edu.get("institution", "")
            year = f" ({edu['year']})" if edu.get("year") else ""
            if field:
                edu_items += f"<li>{_esc(degree)} {_esc(field)} \u2014 {_esc(inst)}{year}</li>"
            else:
                edu_items += f"<li>{_esc(degree)} \u2014 {_esc(inst)}{year}</li>"
        edu_html = f'<div class="card-section"><span class="section-label">Education</span><ul class="edu-list">{edu_items}</ul></div>'

    return f"""<div class="profile-card">
    <div class="card-header">
        <div class="avatar">{_esc(initials)}</div>
        <div>
            <div class="card-name">{name_html}</div>
            <div class="card-title">{_esc(title)}</div>
        </div>
    </div>
    {badges_html}
    {bio_html}
    {exp_html}
    {edu_html}
</div>"""


def chart_container(
    chart_id: str,
    chart_config_json: str,
    fallback_html: str = "",
    size: str = "tall",
    title: Optional[str] = None,
    height: Optional[str] = None,
) -> str:
    """Chart.js canvas with CDN fallback. size: 'tall', 'medium', 'short'."""
    title_html = f'<div class="chart-title">{_esc(title)}</div>' if title else ""
    size_class = f" {size}" if size and not height else ""
    style = f' style="height:{height}"' if height else ""
    return f"""{title_html}<div class="chart-container{size_class}"{style}>
    <canvas id="{_esc(chart_id)}"></canvas>
    <div id="{_esc(chart_id)}_fallback" class="chart-fallback">{fallback_html}</div>
</div>"""


def chart_init_js(chart_id: str, chart_config_json: str) -> str:
    """Return JS snippet to initialize a chart via renderChartOrFallback."""
    return f"renderChartOrFallback('{chart_id}', {chart_config_json});"


def page_footer(
    notes: Optional[List[str]] = None,
    generated_line: str = "",
) -> str:
    """Page footer with data quality notes and generation stamp."""
    notes_html = ""
    if notes:
        items = "".join(f"<li>{_esc(n)}</li>" for n in notes)
        notes_html = f"""<div class="notes">
            <h3>Data Quality Notes &amp; Methodology</h3>
            <ul>{items}</ul>
        </div>"""
    return f"""<div class="container">
    <div class="page-footer">
        {notes_html}
        <div>{_esc(generated_line)}</div>
    </div>
</div>"""


# Legacy aliases for backward compatibility
def hero_header(title, subtitle=None, website=None, pills=None):
    """Legacy alias — use page_header() instead."""
    sub_parts = []
    if subtitle:
        sub_parts.append(subtitle)
    if website:
        sub_parts.append(website)
    badge = None
    if pills:
        badge = " · ".join(f'{p["label"]}: {p["value"]}' for p in pills)
    return page_header(title, " · ".join(sub_parts) if sub_parts else None, badge)


def kpi_grid(cards_html):
    """Legacy alias — use kpi_strip() instead."""
    return kpi_strip(cards_html)


def section_heading(title, count=None):
    """Legacy alias for simple section headings (non-numbered)."""
    badge = ""
    if count is not None:
        badge = f' <span class="count-badge">{count}</span>'
    return f'<h2 style="font-size:18px;font-weight:700;color:var(--primary);margin:24px 0 12px">{_esc(title)}{badge}</h2>'


def footer(generated_at, brand="Nexdata Investment Intelligence"):
    """Legacy alias — use page_footer() instead."""
    return page_footer(generated_line=f"Generated: {generated_at} | {brand}")


# ---------------------------------------------------------------------------
# Chart.js Config Builders
# ---------------------------------------------------------------------------

def build_doughnut_config(
    labels: List[str],
    values: List[float],
    colors: Optional[List[str]] = None,
) -> dict:
    """Build Chart.js doughnut config following chartjs_conventions.md."""
    if colors is None:
        colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(labels))]

    return {
        "type": "doughnut",
        "data": {
            "labels": labels,
            "datasets": [{
                "data": values,
                "backgroundColor": colors[:len(values)],
                "borderWidth": 2,
                "borderColor": "#ffffff",
                "hoverOffset": 4,
            }],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "cutout": "50%",
            "plugins": {
                "legend": {"display": False},
            },
        },
    }


def build_horizontal_bar_config(
    labels: List[str],
    values: List[float],
    colors: Optional[List[str]] = None,
    dataset_label: str = "Value",
) -> dict:
    """Build Chart.js horizontal bar config following chartjs_conventions.md."""
    if colors is None:
        colors = [BLUE] * len(labels)

    return {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": dataset_label,
                "data": values,
                "backgroundColor": colors[:len(values)],
                "borderWidth": 0,
                "borderRadius": 4,
                "barThickness": 28,
            }],
        },
        "options": {
            "indexAxis": "y",
            "responsive": True,
            "maintainAspectRatio": False,
            "plugins": {
                "legend": {"display": False},
            },
            "scales": {
                "x": {
                    "grid": {"color": "#edf2f7"},
                    "ticks": {"color": "#4a5568"},
                    "beginAtZero": True,
                },
                "y": {
                    "grid": {"display": False},
                    "ticks": {"color": "#4a5568", "font": {"size": 13}},
                },
            },
        },
    }


def build_bar_fallback(labels: List[str], values: List[float], color: str = "#2b6cb0") -> str:
    """Build simple CSS bar fallback HTML for when Chart.js CDN fails."""
    max_val = max(values) if values else 1
    rows = []
    for label, val in zip(labels, values):
        pct = (val / max_val * 100) if max_val > 0 else 0
        rows.append(
            f'<div class="fb-row">'
            f'<span class="fb-label">{_esc(label)}</span>'
            f'<div class="fb-bar-track"><div class="fb-bar-fill" style="width:{pct:.0f}%;background:{color}"></div></div>'
            f'<span class="fb-value">{val:,.0f}</span>'
            f'</div>'
        )
    return "\n".join(rows)


def build_chart_legend(
    labels: List[str],
    values: List[float],
    colors: Optional[List[str]] = None,
    value_suffix: str = "",
    show_pct: bool = True,
) -> str:
    """Build a custom HTML legend for charts (color dot + label + value)."""
    if colors is None:
        colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(labels))]
    total = sum(values) if values else 0
    rows = []
    for i, (label, val) in enumerate(zip(labels, values)):
        c = colors[i % len(colors)]
        pct_html = ""
        if show_pct and total > 0:
            pct = val / total * 100
            pct_html = f'<span class="legend-pct">{pct:.0f}%</span>'
        val_display = f"{val:,.0f}{value_suffix}" if val == int(val) else f"{val:,.1f}{value_suffix}"
        rows.append(
            f'<div class="legend-item">'
            f'<span class="legend-dot" style="background:{c}"></span>'
            f'<span class="legend-label">{_esc(label)}</span>'
            f'<span class="legend-value">{val_display}</span>'
            f'{pct_html}'
            f'</div>'
        )
    return '<div class="chart-legend">' + "\n".join(rows) + "</div>"

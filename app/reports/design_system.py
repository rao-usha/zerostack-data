"""
Nexdata Report Design System.

Shared CSS, JS, Chart.js helpers, and HTML component functions
for all report templates. Provides:
- Dark/light mode toggle with localStorage persistence
- Chart.js integration with CDN fallback
- Flat design: rounded 12px, shadow-only elevation, no card borders
- Responsive layout with print optimization
"""

import json as _json
from typing import Optional, List, Dict, Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHART_JS_CDN = "https://cdn.jsdelivr.net/npm/chart.js@4"

CHART_COLORS = [
    "#2563eb", "#059669", "#d97706", "#dc2626", "#7c3aed",
    "#0891b2", "#ea580c", "#4f46e5", "#0d9488", "#b91c1c",
]

# ---------------------------------------------------------------------------
# CSS — Design System (light + dark via CSS custom properties)
# ---------------------------------------------------------------------------

DESIGN_SYSTEM_CSS = """
/* ── Reset & Custom Properties ───────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }

:root {
    --bg-body: #f8fafc;
    --bg-card: #fff;
    --bg-card-hover: #fff;
    --bg-table-header: #f8fafc;
    --bg-table-stripe: #fafbfc;
    --bg-table-hover: #f1f5f9;
    --bg-hero: #0f172a;
    --bg-hero-pill: rgba(255,255,255,0.1);
    --border-hero-pill: rgba(255,255,255,0.15);
    --text-primary: #0f172a;
    --text-secondary: #334155;
    --text-muted: #64748b;
    --text-faint: #94a3b8;
    --text-hero: #fff;
    --text-hero-sub: #e2e8f0;
    --text-hero-label: #94a3b8;
    --text-hero-link: #93c5fd;
    --border-light: #e2e8f0;
    --border-table: #f1f5f9;
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-md: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
    --radius: 12px;
    --radius-pill: 20px;
    --color-blue: #2563eb;
    --color-emerald: #059669;
    --color-amber: #d97706;
    --color-slate: #64748b;
    --avatar-bg: #0f172a;
    --avatar-text: #fff;
    --badge-seniority-bg: #0f172a;
    --badge-seniority-text: #fff;
    --badge-dept-bg: #eff6ff;
    --badge-dept-text: #1e40af;
    --badge-dept-border: #bfdbfe;
    --badge-tenure-bg: #ecfdf5;
    --badge-tenure-text: #065f46;
    --badge-tenure-border: #a7f3d0;
    --pill-public-bg: #dbeafe;
    --pill-public-text: #1d4ed8;
    --pill-pe-bg: #fef3c7;
    --pill-pe-text: #92400e;
    --pill-sub-bg: #f1f5f9;
    --pill-sub-text: #475569;
    --pill-private-bg: #f0fdf4;
    --pill-private-text: #166534;
    --pill-default-bg: #f1f5f9;
    --pill-default-text: #475569;
    --bar-track: #e2e8f0;
    --bar-fill: #2563eb;
    --chart-grid: rgba(0,0,0,0.06);
    --chart-tick: #64748b;
}

[data-theme="dark"] {
    --bg-body: #0f172a;
    --bg-card: #1e293b;
    --bg-card-hover: #1e293b;
    --bg-table-header: #1e293b;
    --bg-table-stripe: #1a2435;
    --bg-table-hover: #263348;
    --bg-hero: #020617;
    --bg-hero-pill: rgba(255,255,255,0.08);
    --border-hero-pill: rgba(255,255,255,0.12);
    --text-primary: #f1f5f9;
    --text-secondary: #cbd5e1;
    --text-muted: #94a3b8;
    --text-faint: #64748b;
    --text-hero: #f1f5f9;
    --text-hero-sub: #cbd5e1;
    --text-hero-label: #64748b;
    --text-hero-link: #93c5fd;
    --border-light: #334155;
    --border-table: #293548;
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.3), 0 1px 2px rgba(0,0,0,0.2);
    --shadow-md: 0 4px 12px rgba(0,0,0,0.4), 0 2px 4px rgba(0,0,0,0.25);
    --avatar-bg: #334155;
    --avatar-text: #e2e8f0;
    --badge-seniority-bg: #334155;
    --badge-seniority-text: #e2e8f0;
    --badge-dept-bg: #1e3a5f;
    --badge-dept-text: #93c5fd;
    --badge-dept-border: #1e3a5f;
    --badge-tenure-bg: #14332a;
    --badge-tenure-text: #6ee7b7;
    --badge-tenure-border: #14332a;
    --pill-public-bg: #1e3a5f;
    --pill-public-text: #93c5fd;
    --pill-pe-bg: #422006;
    --pill-pe-text: #fbbf24;
    --pill-sub-bg: #334155;
    --pill-sub-text: #cbd5e1;
    --pill-private-bg: #14332a;
    --pill-private-text: #6ee7b7;
    --pill-default-bg: #334155;
    --pill-default-text: #cbd5e1;
    --bar-track: #334155;
    --bar-fill: #3b82f6;
    --chart-grid: rgba(255,255,255,0.08);
    --chart-tick: #94a3b8;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    margin: 0; padding: 0;
    background: var(--bg-body);
    color: var(--text-secondary);
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
}

a { color: var(--color-blue); text-decoration: none; }
a:hover { text-decoration: underline; }

/* ── Hero Header ─────────────────────────────────────────────── */
.hero {
    background: var(--bg-hero);
    color: var(--text-hero);
    padding: 48px 0 40px;
}
.hero-inner {
    max-width: 1100px;
    margin: 0 auto;
    padding: 0 40px;
}
.hero h1 {
    font-size: 2.5rem;
    font-weight: 700;
    margin: 0 0 4px;
    letter-spacing: -0.02em;
    color: var(--text-hero);
}
.hero-subtitle {
    color: var(--text-hero-sub);
    font-size: 1.05rem;
    margin: 2px 0 0;
}
.hero-website {
    color: var(--text-hero-link);
    text-decoration: none;
    font-size: 0.95rem;
}
.hero-website:hover { text-decoration: underline; }
.hero-pills {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 20px;
}
.hero-pill {
    background: var(--bg-hero-pill);
    border: 1px solid var(--border-hero-pill);
    border-radius: var(--radius-pill);
    padding: 6px 16px;
    font-size: 0.85rem;
    color: var(--text-hero-sub);
}
.hero-pill-label {
    color: var(--text-hero-label);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.7rem;
    letter-spacing: 0.05em;
    margin-right: 6px;
}

/* ── Container ───────────────────────────────────────────────── */
.container {
    max-width: 1100px;
    margin: 0 auto;
    padding: 0 40px 60px;
}

/* ── Section Headings ────────────────────────────────────────── */
.section-title {
    font-size: 1.35rem;
    font-weight: 700;
    color: var(--text-primary);
    margin: 48px 0 20px;
    padding-bottom: 0;
}
.count-badge {
    background: var(--border-light);
    color: var(--text-muted);
    font-size: 0.8rem;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 12px;
    margin-left: 8px;
    vertical-align: middle;
}

/* ── KPI Cards ───────────────────────────────────────────────── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px;
    margin-top: 32px;
}
.kpi-card {
    background: var(--bg-card);
    border-radius: var(--radius);
    padding: 24px 20px;
    text-align: center;
    box-shadow: var(--shadow-sm);
    border-left: 4px solid var(--border-light);
}
.kpi-blue  { border-left-color: var(--color-blue); }
.kpi-emerald { border-left-color: var(--color-emerald); }
.kpi-slate { border-left-color: var(--color-slate); }
.kpi-amber { border-left-color: var(--color-amber); }
.kpi-value {
    font-size: 2rem;
    font-weight: 700;
    color: var(--text-primary);
    line-height: 1.2;
}
.kpi-label {
    font-size: 0.82rem;
    color: var(--text-muted);
    margin-top: 6px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-weight: 500;
}

/* ── Tables ──────────────────────────────────────────────────── */
.table-container {
    overflow-x: auto;
    border-radius: var(--radius);
    box-shadow: var(--shadow-sm);
    background: var(--bg-card);
}
table {
    width: 100%;
    border-collapse: collapse;
}
th {
    background: var(--bg-table-header);
    color: var(--text-muted);
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 12px 16px;
    text-align: left;
    border-bottom: 2px solid var(--border-light);
    position: sticky;
    top: 0;
    z-index: 1;
}
td {
    padding: 11px 16px;
    border-bottom: 1px solid var(--border-table);
    font-size: 0.9rem;
    color: var(--text-secondary);
}
tbody tr:nth-child(even) { background: var(--bg-table-stripe); }
tbody tr:hover { background: var(--bg-table-hover); }
.num { text-align: right; font-variant-numeric: tabular-nums; }
th.num { text-align: right; }
.company-name { font-weight: 600; color: var(--text-primary); }
.ticker {
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 0.85rem;
    color: var(--color-blue);
    font-weight: 500;
}
.empty-state {
    text-align: center;
    color: var(--text-faint);
    padding: 32px 16px !important;
    font-style: italic;
}

/* ── Status Pills ────────────────────────────────────────────── */
.pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
    white-space: nowrap;
}
.pill-public  { background: var(--pill-public-bg);  color: var(--pill-public-text); }
.pill-pe      { background: var(--pill-pe-bg);      color: var(--pill-pe-text); }
.pill-sub     { background: var(--pill-sub-bg);      color: var(--pill-sub-text); }
.pill-private { background: var(--pill-private-bg);  color: var(--pill-private-text); }
.pill-default { background: var(--pill-default-bg);  color: var(--pill-default-text); }

/* ── Segments ────────────────────────────────────────────────── */
.segments-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 16px;
}
.segment-card {
    background: var(--bg-card);
    border-radius: var(--radius);
    padding: 20px;
    box-shadow: var(--shadow-sm);
}
.segment-header {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 4px;
}
.segment-name { font-weight: 600; color: var(--text-primary); font-size: 0.95rem; }
.segment-aum  { font-weight: 700; color: var(--text-primary); font-size: 1.1rem; }
.segment-strategy { color: var(--text-muted); font-size: 0.82rem; margin-bottom: 12px; }
.segment-bar-track {
    height: 6px;
    background: var(--bar-track);
    border-radius: 3px;
    overflow: hidden;
}
.segment-bar-fill {
    height: 100%;
    background: var(--bar-fill);
    border-radius: 3px;
}
.segment-pct { font-size: 0.78rem; color: var(--text-faint); margin-top: 6px; }
.segment-total {
    text-align: right;
    font-weight: 700;
    color: var(--text-primary);
    margin-top: 16px;
    font-size: 1rem;
}

/* ── Chart Containers ────────────────────────────────────────── */
.chart-wrapper {
    background: var(--bg-card);
    border-radius: var(--radius);
    padding: 24px;
    box-shadow: var(--shadow-sm);
    position: relative;
}
.chart-wrapper canvas {
    max-height: 350px;
}
.chart-fallback {
    display: none;
    padding: 12px 0;
}
.chart-fallback .fb-row {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
    font-size: 0.88rem;
    color: var(--text-secondary);
}
.chart-fallback .fb-label { min-width: 160px; }
.chart-fallback .fb-bar-track {
    flex: 1;
    height: 8px;
    background: var(--bar-track);
    border-radius: 4px;
    overflow: hidden;
}
.chart-fallback .fb-bar-fill {
    height: 100%;
    border-radius: 4px;
}
.chart-fallback .fb-value {
    min-width: 60px;
    text-align: right;
    font-weight: 600;
    color: var(--text-primary);
}
.charts-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
}

/* ── Team / Profile Cards ────────────────────────────────────── */
.team-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 20px;
}
.profile-card {
    background: var(--bg-card);
    border-radius: var(--radius);
    padding: 20px;
    box-shadow: var(--shadow-sm);
    transition: box-shadow 0.15s ease;
}
.profile-card:hover { box-shadow: var(--shadow-md); }
.card-header {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 10px;
}
.avatar {
    width: 44px; height: 44px;
    border-radius: 50%;
    background: var(--avatar-bg);
    color: var(--avatar-text);
    font-size: 0.85rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    letter-spacing: 0.02em;
}
.card-name { font-size: 1.05rem; font-weight: 600; color: var(--text-primary); }
.card-name a { color: var(--color-blue); text-decoration: none; }
.card-name a:hover { text-decoration: underline; }
.card-title { color: var(--text-muted); font-size: 0.88rem; margin: 1px 0 0; }
.card-badges { display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 8px; }
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.72rem;
    font-weight: 600;
}
.badge-seniority { background: var(--badge-seniority-bg); color: var(--badge-seniority-text); }
.badge-dept { background: var(--badge-dept-bg); color: var(--badge-dept-text); border: 1px solid var(--badge-dept-border); }
.badge-tenure { background: var(--badge-tenure-bg); color: var(--badge-tenure-text); border: 1px solid var(--badge-tenure-border); }
.card-bio { color: var(--text-muted); font-size: 0.85rem; line-height: 1.6; margin: 8px 0 4px; }
.card-section { margin-top: 10px; }
.section-label {
    font-size: 0.72rem; font-weight: 700;
    color: var(--text-faint);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.exp-list, .edu-list {
    margin: 4px 0 0 16px; padding: 0;
    font-size: 0.82rem; color: var(--text-muted);
}
.exp-list li, .edu-list li { margin-bottom: 3px; }
.exp-years { color: var(--text-faint); }

/* ── Footnote & Footer ───────────────────────────────────────── */
.footnote {
    color: var(--text-faint);
    font-size: 0.82rem;
    margin-top: 10px;
    font-style: italic;
}
footer {
    max-width: 1100px;
    margin: 0 auto;
    padding: 24px 40px;
    border-top: 1px solid var(--border-light);
    color: var(--text-faint);
    font-size: 0.82rem;
    display: flex;
    justify-content: space-between;
}
footer .brand { font-weight: 600; color: var(--text-muted); }

/* ── Dark Mode Toggle ────────────────────────────────────────── */
.theme-toggle {
    position: fixed;
    top: 16px;
    right: 16px;
    z-index: 9999;
    width: 40px; height: 40px;
    border-radius: 50%;
    border: none;
    background: var(--bg-card);
    box-shadow: var(--shadow-sm);
    cursor: pointer;
    font-size: 1.15rem;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background 0.2s, box-shadow 0.2s;
}
.theme-toggle:hover { box-shadow: var(--shadow-md); }

/* ── Print ───────────────────────────────────────────────────── */
@media print {
    body { background: #fff !important; color: #334155 !important; }
    .hero { background: #0f172a !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
    .kpi-card, .profile-card, .segment-card, .chart-wrapper, .table-container {
        box-shadow: none !important;
        border: 1px solid #e2e8f0;
    }
    .profile-card:hover { box-shadow: none !important; }
    .theme-toggle { display: none !important; }
    .chart-wrapper canvas { max-height: 280px; }
    footer { border-top-color: #e2e8f0; }
}

/* ── Responsive ──────────────────────────────────────────────── */
@media (max-width: 768px) {
    .hero-inner, .container, footer { padding-left: 20px; padding-right: 20px; }
    .hero h1 { font-size: 1.8rem; }
    .kpi-grid { grid-template-columns: repeat(2, 1fr); }
    .team-grid { grid-template-columns: 1fr; }
    .charts-row { grid-template-columns: 1fr; }
}
"""

# ---------------------------------------------------------------------------
# JavaScript — Dark mode + Chart.js theme + CDN fallback
# ---------------------------------------------------------------------------

DARK_MODE_JS = """
(function() {
    var stored = localStorage.getItem('nexdata-theme');
    if (stored === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
    } else if (!stored && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        document.documentElement.setAttribute('data-theme', 'dark');
    }
})();

function toggleTheme() {
    var html = document.documentElement;
    var current = html.getAttribute('data-theme');
    var next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem('nexdata-theme', next);
    var btn = document.getElementById('themeToggle');
    if (btn) btn.textContent = next === 'dark' ? '\\u2600' : '\\u263E';
    if (typeof updateChartsForTheme === 'function') {
        updateChartsForTheme(next === 'dark');
    }
}

document.addEventListener('DOMContentLoaded', function() {
    var btn = document.getElementById('themeToggle');
    if (btn) {
        var isDark = document.documentElement.getAttribute('data-theme') === 'dark';
        btn.textContent = isDark ? '\\u2600' : '\\u263E';
    }
});
"""

CHART_THEME_JS = """
window._nexdataCharts = [];

function updateChartsForTheme(isDark) {
    var gridColor = isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.06)';
    var tickColor = isDark ? '#94a3b8' : '#64748b';
    var legendColor = isDark ? '#cbd5e1' : '#334155';
    window._nexdataCharts.forEach(function(chart) {
        if (chart.options.scales) {
            Object.keys(chart.options.scales).forEach(function(key) {
                var scale = chart.options.scales[key];
                if (scale.ticks) scale.ticks.color = tickColor;
                if (scale.grid) scale.grid.color = gridColor;
            });
        }
        if (chart.options.plugins && chart.options.plugins.legend && chart.options.plugins.legend.labels) {
            chart.options.plugins.legend.labels.color = legendColor;
        }
        chart.update();
    });
}
"""

CHARTJS_FALLBACK_JS = """
window.CHARTJS_AVAILABLE = false;

function renderChartOrFallback(canvasId, config) {
    if (window.CHARTJS_AVAILABLE && typeof Chart !== 'undefined') {
        var ctx = document.getElementById(canvasId);
        if (ctx) {
            var c = new Chart(ctx, config);
            window._nexdataCharts.push(c);
        }
    } else {
        var wrapper = document.getElementById(canvasId + '_fallback');
        if (wrapper) wrapper.style.display = 'block';
        var canvas = document.getElementById(canvasId);
        if (canvas) canvas.style.display = 'none';
    }
}
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
    theme_icon_init = ""
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
    <button id="themeToggle" class="theme-toggle" onclick="toggleTheme()" title="Toggle dark/light mode"></button>
{body_content}
    <script>{CHART_THEME_JS}</script>
    <script>{CHARTJS_FALLBACK_JS}</script>
    <script src="{CHART_JS_CDN}" onload="window.CHARTJS_AVAILABLE=true;{charts_js}" onerror="document.querySelectorAll('.chart-fallback').forEach(function(e){{e.style.display='block'}});document.querySelectorAll('.chart-wrapper canvas').forEach(function(e){{e.style.display='none'}})"></script>
    <script>
    // If CDN loaded synchronously before our onload, run charts now
    if (window.CHARTJS_AVAILABLE && typeof Chart !== 'undefined') {{
        {charts_js}
    }}
    </script>
</body>
</html>"""


def hero_header(
    title: str,
    subtitle: Optional[str] = None,
    website: Optional[str] = None,
    pills: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Dark navy hero banner with optional subtitle, website, and metadata pills."""
    subtitle_html = ""
    if subtitle:
        subtitle_html = f'<div class="hero-subtitle">{_esc(subtitle)}</div>'

    website_html = ""
    if website:
        url = website if website.startswith("http") else f"https://{website}"
        website_html = f'<a href="{_esc(url)}" target="_blank" class="hero-website">{_esc(website)}</a>'

    pills_html = ""
    if pills:
        pill_items = "".join(
            f'<span class="hero-pill"><span class="hero-pill-label">{_esc(p["label"])}</span> {_esc(p["value"])}</span>'
            for p in pills
        )
        pills_html = f'<div class="hero-pills">{pill_items}</div>'

    return f"""
    <header class="hero">
        <div class="hero-inner">
            <h1>{_esc(title)}</h1>
            {subtitle_html}
            {website_html}
            {pills_html}
        </div>
    </header>"""


def kpi_card(value: str, label: str, color: str = "blue") -> str:
    """Single KPI stat card. Colors: blue, emerald, slate, amber."""
    return f"""<div class="kpi-card kpi-{_esc(color)}">
    <div class="kpi-value">{_esc(str(value))}</div>
    <div class="kpi-label">{_esc(label)}</div>
</div>"""


def kpi_grid(cards_html: str) -> str:
    """Wrap kpi_card outputs in a responsive grid."""
    return f'<div class="kpi-grid">{cards_html}</div>'


def section_heading(title: str, count: Optional[int] = None) -> str:
    """Section heading with optional count badge."""
    badge = ""
    if count is not None:
        badge = f' <span class="count-badge">{count}</span>'
    return f'<h2 class="section-title">{_esc(title)}{badge}</h2>'


def data_table(
    headers: List[str],
    rows: List[List[str]],
    numeric_columns: Optional[set] = None,
) -> str:
    """Full HTML table with thead/tbody, zebra striping."""
    if numeric_columns is None:
        numeric_columns = set()

    th_cells = "".join(
        f'<th class="num">{_esc(h)}</th>' if i in numeric_columns else f"<th>{_esc(h)}</th>"
        for i, h in enumerate(headers)
    )

    if not rows:
        empty_row = f'<tr><td colspan="{len(headers)}" class="empty-state">No data available</td></tr>'
        tbody = empty_row
    else:
        row_htmls = []
        for row in rows:
            cells = "".join(
                f'<td class="num">{cell}</td>' if i in numeric_columns else f"<td>{cell}</td>"
                for i, cell in enumerate(row)
            )
            row_htmls.append(f"<tr>{cells}</tr>")
        tbody = "\n".join(row_htmls)

    return f"""<div class="table-container">
<table>
    <thead><tr>{th_cells}</tr></thead>
    <tbody>{tbody}</tbody>
</table>
</div>"""


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
    """Team member profile card with avatar, badges, bio, experience, education."""
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
                years = f' <span class="exp-years">({exp["start_year"]}–{exp["end_year"]})</span>'
            elif exp.get("start_year"):
                years = f' <span class="exp-years">({exp["start_year"]}–)</span>'
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
                edu_items += f"<li>{_esc(degree)} {_esc(field)} — {_esc(inst)}{year}</li>"
            else:
                edu_items += f"<li>{_esc(degree)} — {_esc(inst)}{year}</li>"
        edu_html = f'<div class="card-section"><span class="section-label">Education</span><ul class="edu-list">{edu_items}</ul></div>'

    return f"""<div class="profile-card">
    <div class="card-header">
        <div class="avatar">{_esc(initials)}</div>
        <div class="card-header-text">
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
) -> str:
    """Chart.js canvas with CDN fallback."""
    return f"""<div class="chart-wrapper">
    <canvas id="{_esc(chart_id)}"></canvas>
    <div id="{_esc(chart_id)}_fallback" class="chart-fallback">{fallback_html}</div>
</div>"""


def chart_init_js(chart_id: str, chart_config_json: str) -> str:
    """Return JS snippet to initialize a chart via renderChartOrFallback."""
    return f"renderChartOrFallback('{chart_id}', {chart_config_json});"


def footer(generated_at: str, brand: str = "Nexdata Investment Intelligence") -> str:
    """Page footer with timestamp and brand."""
    return f"""<footer>
    <span>Generated: {_esc(generated_at)}</span>
    <span class="brand">{_esc(brand)}</span>
</footer>"""


# ---------------------------------------------------------------------------
# Chart.js Config Builders
# ---------------------------------------------------------------------------

def build_doughnut_config(
    labels: List[str],
    values: List[float],
    colors: Optional[List[str]] = None,
) -> dict:
    """Build Chart.js config dict for a doughnut chart (cutout 60%, legend bottom)."""
    if colors is None:
        colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(labels))]

    is_dark = False  # Charts adapt via updateChartsForTheme()

    return {
        "type": "doughnut",
        "data": {
            "labels": labels,
            "datasets": [{
                "data": values,
                "backgroundColor": colors[:len(values)],
                "borderWidth": 0,
            }],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": True,
            "cutout": "60%",
            "plugins": {
                "legend": {
                    "position": "bottom",
                    "labels": {
                        "padding": 16,
                        "usePointStyle": True,
                        "pointStyleWidth": 10,
                    },
                },
            },
        },
    }


def build_horizontal_bar_config(
    labels: List[str],
    values: List[float],
    colors: Optional[List[str]] = None,
    dataset_label: str = "Value",
) -> dict:
    """Build Chart.js config dict for a horizontal bar chart."""
    if colors is None:
        colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(labels))]

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
            }],
        },
        "options": {
            "indexAxis": "y",
            "responsive": True,
            "maintainAspectRatio": True,
            "plugins": {
                "legend": {"display": False},
            },
            "scales": {
                "x": {
                    "grid": {"color": "rgba(0,0,0,0.06)"},
                    "ticks": {"color": "#64748b"},
                },
                "y": {
                    "grid": {"display": False},
                    "ticks": {"color": "#64748b"},
                },
            },
        },
    }


def build_bar_fallback(labels: List[str], values: List[float], colors: Optional[List[str]] = None) -> str:
    """Build simple CSS bar fallback HTML for when Chart.js CDN fails."""
    if colors is None:
        colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(labels))]
    max_val = max(values) if values else 1
    rows = []
    for i, (label, val) in enumerate(zip(labels, values)):
        pct = (val / max_val * 100) if max_val > 0 else 0
        c = colors[i % len(colors)]
        rows.append(
            f'<div class="fb-row">'
            f'<span class="fb-label">{_esc(label)}</span>'
            f'<div class="fb-bar-track"><div class="fb-bar-fill" style="width:{pct:.0f}%;background:{c}"></div></div>'
            f'<span class="fb-value">{val:,.0f}</span>'
            f'</div>'
        )
    return "\n".join(rows)

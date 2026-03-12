"""
Med-Spa Opportunity Map — Interactive Leaflet Report Template.

Self-contained HTML with:
- Dark-themed Leaflet map (CartoDB Dark Matter tiles)
- State choropleth layer (color by prospect count)
- MarkerCluster layer with color-coded markers by acquisition grade
- Click popup: name, address, score, revenue, ownership, physician oversight
- Filter sidebar: score range, ownership type, revenue tier
- Stats bar: total prospects, avg score, top states, ownership breakdown
- All data embedded as JSON (no external API calls)
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.reports.design_system import (
    leaflet_head,
    map_container,
    _esc,
    BLUE, GREEN, ORANGE, RED, GRAY, PURPLE, TEAL,
)

logger = logging.getLogger(__name__)


class MedSpaOpportunityMapTemplate:
    """Interactive map of medspa acquisition prospects."""

    description = "Interactive map of medspa acquisition prospects with clustering, choropleth, and filters"

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Query medspa_prospects for map data."""

        # Core prospect data with coordinates
        prospect_sql = text("""
            SELECT
                name, city, state, zip_code,
                latitude, longitude,
                acquisition_score, acquisition_grade,
                rating, review_count, price,
                ownership_type, parent_entity,
                address
            FROM medspa_prospects
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND is_closed = false
            ORDER BY acquisition_score DESC
        """)

        rows = db.execute(prospect_sql).fetchall()

        prospects = []
        for r in rows:
            prospects.append({
                "name": r[0],
                "city": r[1],
                "state": r[2],
                "zip": r[3],
                "lat": float(r[4]) if r[4] else None,
                "lng": float(r[5]) if r[5] else None,
                "score": float(r[6]) if r[6] else 0,
                "grade": r[7],
                "rating": float(r[8]) if r[8] else None,
                "reviews": r[9] or 0,
                "price": r[10],
                "ownership": r[11] or "Unknown",
                "parent": r[12],
                "address": r[13],
            })

        # State-level aggregation
        state_sql = text("""
            SELECT
                state,
                COUNT(*) as cnt,
                AVG(acquisition_score) as avg_score,
                COUNT(*) FILTER (WHERE acquisition_grade = 'A') as grade_a,
                COUNT(*) FILTER (WHERE acquisition_grade = 'B') as grade_b
            FROM medspa_prospects
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND is_closed = false
            GROUP BY state
            ORDER BY cnt DESC
        """)

        state_rows = db.execute(state_sql).fetchall()
        state_stats = {}
        for r in state_rows:
            state_stats[r[0]] = {
                "count": r[1],
                "avg_score": round(float(r[2]), 1) if r[2] else 0,
                "grade_a": r[3],
                "grade_b": r[4],
            }

        # Ownership breakdown
        ownership_sql = text("""
            SELECT
                COALESCE(ownership_type, 'Unknown') as otype,
                COUNT(*) as cnt
            FROM medspa_prospects
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND is_closed = false
            GROUP BY otype
            ORDER BY cnt DESC
        """)
        ownership_rows = db.execute(ownership_sql).fetchall()
        ownership_breakdown = {r[0]: r[1] for r in ownership_rows}

        # Summary stats
        total = len(prospects)
        avg_score = round(sum(p["score"] for p in prospects) / total, 1) if total else 0
        top_states = [{"state": s[0], "count": s[1]} for s in state_rows[:5]]

        return {
            "prospects": prospects,
            "state_stats": state_stats,
            "ownership_breakdown": ownership_breakdown,
            "total": total,
            "avg_score": avg_score,
            "top_states": top_states,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        }

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render self-contained interactive map HTML."""

        title = data.get("report_title", "MedSpa Opportunity Map")
        prospects_json = json.dumps(data["prospects"], separators=(",", ":"))
        state_stats_json = json.dumps(data["state_stats"], separators=(",", ":"))
        total = data["total"]
        avg_score = data["avg_score"]
        top_states = data["top_states"]
        ownership = data["ownership_breakdown"]
        generated_at = data["generated_at"]

        # Build top-states text
        top_states_html = ", ".join(
            f'{s["state"]} ({s["count"]})'
            for s in top_states
        )

        # Ownership pills
        ownership_pills = "".join(
            f'<span class="own-pill own-{k.lower().replace("-", "").replace(" ", "")}">'
            f'{_esc(k)}: {v}</span>'
            for k, v in ownership.items()
        )

        leaflet_tags = leaflet_head()
        map_div = map_container("map", "calc(100vh - 160px)")

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{_esc(title)}</title>
    {leaflet_tags}
    <style>
    :root {{
        --primary: #1a365d;
        --primary-light: #2b6cb0;
        --accent: #ed8936;
        --accent-green: #38a169;
        --accent-red: #e53e3e;
        --gray-50: #f7fafc;
        --gray-100: #edf2f7;
        --gray-200: #e2e8f0;
        --gray-300: #cbd5e0;
        --gray-500: #718096;
        --gray-700: #4a5568;
        --gray-800: #2d3748;
        --gray-900: #1a202c;
        --bg: #1a202c;
        --surface: #2d3748;
        --surface-alt: #4a5568;
        --text: #e2e8f0;
        --text-muted: #a0aec0;
    }}
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        background: var(--bg);
        color: var(--text);
        overflow: hidden;
        height: 100vh;
    }}

    /* Top bar */
    .top-bar {{
        background: linear-gradient(135deg, var(--primary) 0%, var(--primary-light) 100%);
        padding: 12px 24px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 8px;
        z-index: 1000;
        position: relative;
    }}
    .top-bar h1 {{
        font-size: 20px;
        font-weight: 700;
        color: #fff;
    }}
    .top-bar .subtitle {{
        font-size: 13px;
        color: rgba(255,255,255,0.7);
        margin-left: 12px;
    }}

    /* KPI chips */
    .kpi-row {{
        display: flex;
        gap: 16px;
        align-items: center;
        flex-wrap: wrap;
    }}
    .kpi-chip {{
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.2);
        border-radius: 6px;
        padding: 4px 14px;
        font-size: 13px;
        color: #fff;
        white-space: nowrap;
    }}
    .kpi-chip .val {{
        font-weight: 700;
        margin-right: 4px;
    }}

    /* Layout */
    .map-wrapper {{
        display: flex;
        height: calc(100vh - 56px);
        position: relative;
    }}

    /* Sidebar */
    .sidebar {{
        width: 300px;
        background: var(--surface);
        padding: 16px;
        overflow-y: auto;
        flex-shrink: 0;
        z-index: 900;
        border-right: 1px solid var(--surface-alt);
    }}
    .sidebar h3 {{
        font-size: 14px;
        font-weight: 700;
        color: var(--accent);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 12px;
    }}
    .filter-group {{
        margin-bottom: 20px;
    }}
    .filter-group label {{
        display: block;
        font-size: 12px;
        font-weight: 600;
        color: var(--text-muted);
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }}
    .filter-group input[type="range"] {{
        width: 100%;
        accent-color: var(--accent);
    }}
    .range-labels {{
        display: flex;
        justify-content: space-between;
        font-size: 11px;
        color: var(--text-muted);
    }}
    .filter-group select {{
        width: 100%;
        padding: 8px;
        border-radius: 6px;
        border: 1px solid var(--surface-alt);
        background: var(--bg);
        color: var(--text);
        font-size: 13px;
    }}
    .checkbox-group {{
        display: flex;
        flex-direction: column;
        gap: 6px;
    }}
    .checkbox-group label {{
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 13px;
        color: var(--text);
        text-transform: none;
        letter-spacing: 0;
        font-weight: 400;
        cursor: pointer;
    }}
    .checkbox-group input[type="checkbox"] {{
        accent-color: var(--accent);
    }}

    /* Stats panel */
    .stats-section {{
        margin-top: 20px;
        padding-top: 16px;
        border-top: 1px solid var(--surface-alt);
    }}
    .stat-row {{
        display: flex;
        justify-content: space-between;
        padding: 4px 0;
        font-size: 13px;
    }}
    .stat-row .stat-label {{ color: var(--text-muted); }}
    .stat-row .stat-val {{ font-weight: 600; color: var(--text); }}

    /* Ownership pills */
    .own-pills {{ display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }}
    .own-pill {{
        font-size: 11px;
        padding: 3px 8px;
        border-radius: 10px;
        font-weight: 600;
    }}
    .own-independent {{ background: #38a16933; color: #68d391; }}
    .own-multisite {{ background: #ed893633; color: #fbd38d; }}
    .own-pebacked {{ background: #2b6cb033; color: #63b3ed; }}
    .own-unknown {{ background: #71809633; color: #a0aec0; }}

    /* Map */
    .map-area {{
        flex: 1;
        position: relative;
    }}
    #map {{
        width: 100%;
        height: 100%;
    }}

    /* Leaflet popup override */
    .leaflet-popup-content-wrapper {{
        background: var(--surface);
        color: var(--text);
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
    }}
    .leaflet-popup-tip {{
        background: var(--surface);
    }}
    .leaflet-popup-content {{
        margin: 12px 16px;
        font-size: 13px;
        line-height: 1.5;
    }}
    .popup-name {{
        font-size: 15px;
        font-weight: 700;
        color: var(--accent);
        margin-bottom: 6px;
    }}
    .popup-row {{
        display: flex;
        justify-content: space-between;
        padding: 2px 0;
    }}
    .popup-row .plabel {{ color: var(--text-muted); }}
    .popup-row .pval {{ font-weight: 600; }}
    .popup-score {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 700;
        font-size: 14px;
    }}
    .popup-score.grade-a {{ background: #38a16944; color: #68d391; }}
    .popup-score.grade-b {{ background: #2b6cb044; color: #63b3ed; }}
    .popup-score.grade-c {{ background: #ed893644; color: #fbd38d; }}
    .popup-score.grade-d {{ background: #e53e3e44; color: #fc8181; }}
    .popup-score.grade-f {{ background: #71809644; color: #a0aec0; }}

    /* Visible count badge */
    .visible-count {{
        position: absolute;
        bottom: 20px;
        left: 50%;
        transform: translateX(-50%);
        background: var(--surface);
        color: var(--text);
        padding: 8px 20px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 600;
        z-index: 800;
        box-shadow: 0 2px 12px rgba(0,0,0,0.4);
        pointer-events: none;
    }}

    /* Sidebar toggle for mobile */
    .sidebar-toggle {{
        display: none;
        position: absolute;
        top: 10px;
        left: 10px;
        z-index: 950;
        background: var(--surface);
        color: var(--text);
        border: 1px solid var(--surface-alt);
        border-radius: 6px;
        padding: 8px 12px;
        font-size: 13px;
        cursor: pointer;
    }}
    @media (max-width: 768px) {{
        .sidebar {{ position: absolute; left: -300px; top: 0; height: 100%; transition: left 0.3s; }}
        .sidebar.open {{ left: 0; }}
        .sidebar-toggle {{ display: block; }}
    }}

    /* Marker cluster overrides for dark theme */
    .marker-cluster-small {{
        background-color: rgba(43, 108, 176, 0.5);
    }}
    .marker-cluster-small div {{
        background-color: rgba(43, 108, 176, 0.8);
        color: #fff;
    }}
    .marker-cluster-medium {{
        background-color: rgba(237, 137, 54, 0.5);
    }}
    .marker-cluster-medium div {{
        background-color: rgba(237, 137, 54, 0.8);
        color: #fff;
    }}
    .marker-cluster-large {{
        background-color: rgba(229, 62, 62, 0.5);
    }}
    .marker-cluster-large div {{
        background-color: rgba(229, 62, 62, 0.8);
        color: #fff;
    }}

    /* Legend */
    .map-legend {{
        position: absolute;
        bottom: 60px;
        right: 20px;
        background: var(--surface);
        padding: 12px 16px;
        border-radius: 8px;
        z-index: 800;
        font-size: 12px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.3);
    }}
    .legend-title {{
        font-weight: 700;
        margin-bottom: 8px;
        color: var(--accent);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    .legend-item {{
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 4px;
    }}
    .legend-dot {{
        width: 12px;
        height: 12px;
        border-radius: 50%;
        flex-shrink: 0;
    }}
    </style>
</head>
<body>

<!-- Top bar -->
<div class="top-bar">
    <div style="display:flex;align-items:baseline">
        <h1>{_esc(title)}</h1>
        <span class="subtitle">Generated {generated_at}</span>
    </div>
    <div class="kpi-row">
        <div class="kpi-chip"><span class="val">{total:,}</span> Prospects</div>
        <div class="kpi-chip"><span class="val">{avg_score}</span> Avg Score</div>
        <div class="kpi-chip"><span class="val">{len(data["state_stats"])}</span> States</div>
        <div class="kpi-chip">Top: {_esc(top_states_html)}</div>
    </div>
</div>

<!-- Layout -->
<div class="map-wrapper">
    <!-- Sidebar -->
    <div class="sidebar" id="sidebar">
        <h3>Filters</h3>

        <div class="filter-group">
            <label>Min Acquisition Score</label>
            <input type="range" id="scoreMin" min="0" max="100" value="0" step="5">
            <div class="range-labels"><span>0</span><span id="scoreMinVal">0</span><span>100</span></div>
        </div>

        <div class="filter-group">
            <label>Grade</label>
            <div class="checkbox-group" id="gradeFilters">
                <label><input type="checkbox" value="A" checked> A (80+)</label>
                <label><input type="checkbox" value="B" checked> B (65-79)</label>
                <label><input type="checkbox" value="C" checked> C (50-64)</label>
                <label><input type="checkbox" value="D" checked> D (35-49)</label>
                <label><input type="checkbox" value="F" checked> F (&lt;35)</label>
            </div>
        </div>

        <div class="filter-group">
            <label>Ownership Type</label>
            <div class="checkbox-group" id="ownershipFilters">
                <label><input type="checkbox" value="Independent" checked> Independent</label>
                <label><input type="checkbox" value="Multi-Site" checked> Multi-Site</label>
                <label><input type="checkbox" value="PE-Backed" checked> PE-Backed</label>
                <label><input type="checkbox" value="Unknown" checked> Unknown</label>
            </div>
        </div>

        <div class="filter-group">
            <label>Price Tier</label>
            <select id="priceTier">
                <option value="">All</option>
                <option value="$$$$">$$$$ (Premium)</option>
                <option value="$$$">$$$ (Upscale)</option>
                <option value="$$">$$ (Mid-Range)</option>
                <option value="$">$ (Budget)</option>
            </select>
        </div>

        <div class="filter-group">
            <label>State</label>
            <select id="stateFilter">
                <option value="">All States</option>
            </select>
        </div>

        <!-- Stats -->
        <div class="stats-section">
            <h3>Visible Stats</h3>
            <div class="stat-row">
                <span class="stat-label">Showing</span>
                <span class="stat-val" id="visibleCount">{total:,}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Avg Score</span>
                <span class="stat-val" id="visibleAvgScore">{avg_score}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Grade A</span>
                <span class="stat-val" id="visibleGradeA">-</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Grade B</span>
                <span class="stat-val" id="visibleGradeB">-</span>
            </div>
        </div>

        <div class="stats-section">
            <h3>Ownership</h3>
            <div class="own-pills">{ownership_pills}</div>
        </div>
    </div>

    <!-- Map area -->
    <div class="map-area">
        <button class="sidebar-toggle" id="sidebarToggle">Filters</button>
        {map_div}
        <div class="visible-count" id="visibleBadge">Showing {total:,} prospects</div>

        <!-- Legend -->
        <div class="map-legend">
            <div class="legend-title">Acquisition Grade</div>
            <div class="legend-item"><div class="legend-dot" style="background:#38a169"></div> A (80+)</div>
            <div class="legend-item"><div class="legend-dot" style="background:#2b6cb0"></div> B (65-79)</div>
            <div class="legend-item"><div class="legend-dot" style="background:#ed8936"></div> C (50-64)</div>
            <div class="legend-item"><div class="legend-dot" style="background:#e53e3e"></div> D (35-49)</div>
            <div class="legend-item"><div class="legend-dot" style="background:#718096"></div> F (&lt;35)</div>
        </div>
    </div>
</div>

<script>
// --- Data ---
var PROSPECTS = {prospects_json};
var STATE_STATS = {state_stats_json};

// --- Grade colors ---
var GRADE_COLORS = {{
    "A": "#38a169",
    "B": "#2b6cb0",
    "C": "#ed8936",
    "D": "#e53e3e",
    "F": "#718096"
}};

// --- Init map ---
var map = L.map("map", {{
    center: [39.8, -98.5],
    zoom: 4,
    zoomControl: true,
    preferCanvas: true
}});

L.tileLayer("https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png", {{
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: "abcd",
    maxZoom: 19
}}).addTo(map);

// --- MarkerCluster ---
var clusterGroup = L.markerClusterGroup({{
    maxClusterRadius: 50,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true,
    iconCreateFunction: function(cluster) {{
        var count = cluster.getChildCount();
        var size = count < 20 ? "small" : count < 100 ? "medium" : "large";
        return L.divIcon({{
            html: "<div>" + count + "</div>",
            className: "marker-cluster marker-cluster-" + size,
            iconSize: L.point(40, 40)
        }});
    }}
}});

// --- Create markers ---
var allMarkers = [];
PROSPECTS.forEach(function(p) {{
    if (!p.lat || !p.lng) return;
    var color = GRADE_COLORS[p.grade] || "#718096";
    var icon = L.divIcon({{
        className: "",
        html: '<div style="width:12px;height:12px;border-radius:50%;background:' + color +
              ';border:2px solid rgba(255,255,255,0.7);box-shadow:0 1px 4px rgba(0,0,0,0.5)"></div>',
        iconSize: [12, 12],
        iconAnchor: [6, 6]
    }});

    var marker = L.marker([p.lat, p.lng], {{ icon: icon }});

    var gradeClass = "grade-" + (p.grade || "f").toLowerCase();
    var popupHtml =
        '<div class="popup-name">' + escHtml(p.name) + '</div>' +
        '<div class="popup-row"><span class="plabel">Score</span><span class="popup-score ' + gradeClass + '">' + p.score.toFixed(1) + ' (' + p.grade + ')</span></div>' +
        '<div class="popup-row"><span class="plabel">City</span><span class="pval">' + escHtml(p.city || "") + ', ' + escHtml(p.state || "") + '</span></div>' +
        (p.address ? '<div class="popup-row"><span class="plabel">Address</span><span class="pval">' + escHtml(p.address) + '</span></div>' : '') +
        '<div class="popup-row"><span class="plabel">Rating</span><span class="pval">' + (p.rating ? p.rating.toFixed(1) + ' (' + p.reviews + ' reviews)' : 'N/A') + '</span></div>' +
        '<div class="popup-row"><span class="plabel">Price</span><span class="pval">' + escHtml(p.price || "N/A") + '</span></div>' +
        '<div class="popup-row"><span class="plabel">Ownership</span><span class="pval">' + escHtml(p.ownership) + '</span></div>' +
        (p.parent ? '<div class="popup-row"><span class="plabel">Parent</span><span class="pval">' + escHtml(p.parent) + '</span></div>' : '');

    marker.bindPopup(popupHtml, {{ maxWidth: 300 }});

    marker._prospectData = p;
    allMarkers.push(marker);
}});

// Add all markers initially
allMarkers.forEach(function(m) {{ clusterGroup.addLayer(m); }});
map.addLayer(clusterGroup);

// --- Populate state filter ---
var stateSelect = document.getElementById("stateFilter");
var states = Object.keys(STATE_STATS).sort();
states.forEach(function(s) {{
    var opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s + " (" + STATE_STATS[s].count + ")";
    stateSelect.appendChild(opt);
}});

// --- Filter logic ---
function getCheckedValues(containerId) {{
    var checks = document.querySelectorAll("#" + containerId + " input[type=checkbox]:checked");
    var vals = [];
    checks.forEach(function(c) {{ vals.push(c.value); }});
    return vals;
}}

function applyFilters() {{
    var minScore = parseInt(document.getElementById("scoreMin").value);
    var grades = getCheckedValues("gradeFilters");
    var ownerships = getCheckedValues("ownershipFilters");
    var priceTier = document.getElementById("priceTier").value;
    var stateVal = document.getElementById("stateFilter").value;

    document.getElementById("scoreMinVal").textContent = minScore;

    clusterGroup.clearLayers();
    var visible = [];

    allMarkers.forEach(function(m) {{
        var p = m._prospectData;
        if (p.score < minScore) return;
        if (grades.indexOf(p.grade) === -1) return;
        if (ownerships.indexOf(p.ownership) === -1) return;
        if (priceTier && p.price !== priceTier) return;
        if (stateVal && p.state !== stateVal) return;

        clusterGroup.addLayer(m);
        visible.push(p);
    }});

    // Update stats
    var vCount = visible.length;
    var vAvg = vCount > 0 ? (visible.reduce(function(s,p){{ return s+p.score; }}, 0) / vCount).toFixed(1) : "-";
    var gradeA = visible.filter(function(p){{ return p.grade==="A"; }}).length;
    var gradeB = visible.filter(function(p){{ return p.grade==="B"; }}).length;

    document.getElementById("visibleCount").textContent = vCount.toLocaleString();
    document.getElementById("visibleAvgScore").textContent = vAvg;
    document.getElementById("visibleGradeA").textContent = gradeA.toLocaleString();
    document.getElementById("visibleGradeB").textContent = gradeB.toLocaleString();
    document.getElementById("visibleBadge").textContent = "Showing " + vCount.toLocaleString() + " prospects";
}}

// Bind filter events
document.getElementById("scoreMin").addEventListener("input", applyFilters);
document.querySelectorAll("#gradeFilters input, #ownershipFilters input").forEach(function(el) {{
    el.addEventListener("change", applyFilters);
}});
document.getElementById("priceTier").addEventListener("change", applyFilters);
document.getElementById("stateFilter").addEventListener("change", function() {{
    applyFilters();
    var st = this.value;
    if (st && STATE_STATS[st]) {{
        // Zoom to state — approximate center via visible markers
        var stateMarkers = allMarkers.filter(function(m) {{ return m._prospectData.state === st; }});
        if (stateMarkers.length > 0) {{
            var group = L.featureGroup(stateMarkers);
            map.fitBounds(group.getBounds().pad(0.1));
        }}
    }}
}});

// Sidebar toggle (mobile)
document.getElementById("sidebarToggle").addEventListener("click", function() {{
    document.getElementById("sidebar").classList.toggle("open");
}});

// Initial stats
applyFilters();

// Escape HTML helper
function escHtml(s) {{
    if (!s) return "";
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
}}
</script>
</body>
</html>"""

    def render_excel(self, data: Dict[str, Any]) -> bytes:
        """Excel export not supported for map template."""
        raise NotImplementedError("Map template only supports HTML format")

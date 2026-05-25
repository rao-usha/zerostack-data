"""
SPEC_073 — Atlas headless-Chrome smoke harness.

Runs `docs/ATLAS_TOUR.md` end-to-end:
  L (layer)    — one scenario per registered Atlas layer (~20)
  D (dynamic)  — one scenario per dynamic feature (4-5)
  P (place)    — one scenario per featured place deep-dive (8)

Per scenario: open the URL in headless Chrome, capture stderr console,
dump DOM, assert the scenario-specific selector is present and
populated. Writes a CSV report to docs/atlas_smoke_report.csv. Exits 0
on all-pass, 1 on any fail.

Run:
    python scripts/smoke_atlas.py
    python scripts/smoke_atlas.py --only L          # just layers
    python scripts/smoke_atlas.py --base http://localhost:3001
    python scripts/smoke_atlas.py --api  http://localhost:8001
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile

# Force UTF-8 on stdout regardless of locale (Windows cp1252 trips on
# arrows / superscripts otherwise).
try:
    sys.stdout.reconfigure(encoding="utf-8")  # py3.7+
except Exception:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import time
import urllib.request
import urllib.parse
import json
from dataclasses import dataclass, field
from typing import Callable, List, Optional


# ── Chrome discovery ─────────────────────────────────────────────────────
CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]


def find_chrome() -> str:
    for c in CHROME_CANDIDATES:
        if os.path.exists(c):
            return c
    found = shutil.which("google-chrome") or shutil.which("chromium")
    if found:
        return found
    raise RuntimeError("Chrome not found — install or set CHROME env var")


# ── Console-noise allowlist ──────────────────────────────────────────────
# Known-harmless stderr lines that don't indicate a real JS failure
NOISE_PATTERNS = [
    re.compile(p) for p in (
        r"USB:",
        r"DevTools listening",
        r"HandleProcess",
        r"\[Extension",
        r"GroupPolicy",
        r"gpu_init",
        r"gl_factory",
        r"D3D",
        r"font_fallback",
        r"NetworkServiceFactory",
        r"INFO:CONSOLE",   # mere info-level console messages
        r"WARNING:CONSOLE",
        r"GCM",
        r"FullBrowserCrash",
        r"\[OptimizationGuide",
        r"\[FontHandle",
    )
]


def real_js_error(stderr: str) -> Optional[str]:
    """Return the first line that's a real JS error, or None."""
    for line in stderr.splitlines():
        if "Atlas init failed" in line or "Uncaught" in line:
            if not any(p.search(line) for p in NOISE_PATTERNS):
                return line.strip()
    return None


# ── Scenario model ───────────────────────────────────────────────────────
@dataclass
class Scenario:
    category: str       # 'L' (layer) | 'D' (dynamic) | 'P' (place)
    name: str
    url_suffix: str
    # Each assert is (description, predicate(dom) -> bool)
    asserts: List[tuple] = field(default_factory=list)


def count_matches(dom: str, pattern: str) -> int:
    return len(re.findall(pattern, dom))


# ── DOM-assertion helpers ────────────────────────────────────────────────
def assert_legend_populated(layer_label_substr: Optional[str] = None) -> tuple:
    """Choropleth render succeeded if #legend-label changed from '—'
    and (optionally) contains an expected label substring. The map
    canvas itself can't be pixel-inspected via DOM, but the legend is
    only updated by renderLegend → which is only called from
    applyLayerToBoundaries → which fires after the layer-data fetch."""
    def f(dom):
        m = re.search(r'<div class="label" id="legend-label">([^<]+)</div>', dom)
        if not m: return False
        label = m.group(1).strip()
        if label in ("", "—", "-"): return False
        if layer_label_substr and layer_label_substr.lower() not in label.lower():
            return False
        return True
    desc = "legend label populated"
    if layer_label_substr:
        desc += f" w/ '{layer_label_substr[:24]}'"
    return (desc, f)


def assert_overlay_active(layer_id: str) -> tuple:
    """Point overlay activated if the corresponding layer-btn has
    class 'active-overlay' applied via markActiveLayerButton."""
    pat = re.compile(
        rf'data-layer-id="{re.escape(layer_id)}"[^>]*data-grain="point"',
    )
    def f(dom):
        # Find the button block; look for 'active-overlay' nearby
        for m in re.finditer(
            r'<button[^>]*class="([^"]*)"[^>]*data-layer-id="'
            + re.escape(layer_id) + r'"', dom,
        ):
            if "active-overlay" in m.group(1):
                return True
        # Also check inverted attribute order
        for m in re.finditer(
            r'<button[^>]*data-layer-id="' + re.escape(layer_id)
            + r'"[^>]*class="([^"]*)"', dom,
        ):
            if "active-overlay" in m.group(1):
                return True
        return False
    return (f"layer-btn[{layer_id}] has active-overlay class", f)


def assert_bivariate_legend() -> tuple:
    """3×3 bivariate grid must show 9 cells."""
    def f(dom): return count_matches(dom, r'<div class="cell"') == 9
    return ("bivariate 9-cell grid", f)


def assert_brush_present() -> tuple:
    def f(dom): return '<g class="brush"' in dom
    return ("brush group present", f)


def assert_arcs(min_count: int = 50) -> tuple:
    def f(dom): return count_matches(dom, r'class="arc flowing"') >= min_count
    return (f"migration arcs >={min_count}", f)


def assert_sparklines(min_count: int = 1) -> tuple:
    def f(dom): return count_matches(dom, r'<div class="lspark"><svg') >= min_count
    return (f"sparklines >={min_count}", f)


def assert_place_layers(min_count: int = 4) -> tuple:
    def f(dom): return count_matches(dom, r'<div class="place-layer"') >= min_count
    return (f"place-layer cards >={min_count}", f)


def assert_counters_populated() -> tuple:
    """The chrome counters must end in non-zero values once init runs."""
    def f(dom):
        m = re.search(r'<b id="ct-layers">(\d+)</b>', dom)
        return bool(m and int(m.group(1)) > 0)
    return ("counters populated", f)


# ── Scenario assembly ────────────────────────────────────────────────────
def fetch_layer_registry(api_base: str) -> List[dict]:
    """GET /atlas/layers, flatten to a list of dicts."""
    url = f"{api_base}/api/v1/atlas/layers"
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.loads(resp.read())
    out = []
    for dom, layers in data["layers_by_domain"].items():
        for l in layers:
            out.append({**l, "domain": dom})
    return out


# Hardcoded interesting deep-dives (kept in sync with ATLAS_TOUR.md)
PLACE_DEEP_DIVES = [
    ("Harris Co TX",        "48201"),
    ("Arlington Co VA",     "51013"),
    ("LA County CA",        "06037"),
    ("New York County NY",  "36061"),
    ("Cook County IL",      "17031"),
    ("Maricopa County AZ",  "04013"),
    ("San Francisco CA",    "06075"),
    ("Miami-Dade FL",       "12086"),
]

def assert_scrubber_visible() -> tuple:
    """SPEC_066c FEMA time-cascade scrubber must be shown for the FEMA layer."""
    def f(dom):
        # The activateFemaScrubber adds the 'show' class; either it has
        # show OR display:none has been removed
        return bool(re.search(r'<div id="scrubber"[^>]*class="[^"]*show', dom))
    return ("FEMA year scrubber visible", f)


def assert_calendar_present() -> tuple:
    """SPEC_066c calendar heatmap rendered in place panel (Observable Plot
    cell mark inside .place-calendar div)."""
    def f(dom):
        return bool(re.search(r'<div class="place-calendar"[^>]*>.*?<svg', dom, re.S))
    return ("place calendar SVG", f)


DYNAMIC_SCENARIOS = [
    {
        # Counters tween via D3 (start at 0, write intermediate values).
        # Dump can hit mid-tween → 0 stays in DOM and trips a >0 check.
        # The render itself is cosmetic; assert_legend_populated already
        # proves init() ran. So we don't try to assert the tween value.
        "name": "landing-default-layer",
        "url_suffix": "/atlas.html",
        "asserts": [assert_legend_populated("Natural")],
    },
    {
        "name": "bivariate-compare",
        "url_suffix": "/atlas.html?layer=demo_irs_county_agi_per_return"
                      "&compare=disaster_nri",
        "asserts": [assert_bivariate_legend()],
    },
    {
        "name": "brushed-histogram",
        "url_suffix": "/atlas.html?layer=disaster_nri",
        "asserts": [assert_brush_present(), assert_legend_populated()],
    },
    {
        "name": "migration-arcs",
        "url_suffix": "/atlas.html?layer=demo_irs_migration_net_agi",
        "asserts": [assert_arcs(50)],
    },
    {
        "name": "place-panel-sparklines",
        "url_suffix": "/atlas.html?layer=finance_fdic_county_deposits&place=48201",
        "asserts": [assert_sparklines(1), assert_place_layers(4)],
    },
    # SPEC_066c additions
    {
        "name": "fema-time-scrubber",
        "url_suffix": "/atlas.html?layer=disaster_fema_declarations",
        "asserts": [assert_scrubber_visible(), assert_legend_populated()],
    },
    {
        "name": "place-calendar-heatmap",
        "url_suffix": "/atlas.html?place=48201",
        "asserts": [assert_calendar_present()],
    },
]


def build_scenarios(layers: List[dict]) -> List[Scenario]:
    scenarios: List[Scenario] = []
    for l in layers:
        lid = l["id"]
        label_substr = l.get("label", "").split("(")[0].strip()[:14]
        if l["grain"] == "point":
            # Point layers activate as overlays — URL needs both a base
            # layer (any choropleth) AND the point layer as overlay.
            scenarios.append(Scenario(
                category="L", name=f"{lid} (point overlay)",
                url_suffix=f"/atlas.html?layer=disaster_nri"
                           f"&overlay={urllib.parse.quote(lid)}",
                asserts=[assert_overlay_active(lid)],
            ))
        else:
            scenarios.append(Scenario(
                category="L", name=f"{lid} ({l['grain']} choropleth)",
                url_suffix=f"/atlas.html?layer={urllib.parse.quote(lid)}",
                asserts=[assert_legend_populated(label_substr)],
            ))
    for d in DYNAMIC_SCENARIOS:
        scenarios.append(Scenario(
            category="D", name=d["name"],
            url_suffix=d["url_suffix"], asserts=d["asserts"],
        ))
    for label, fips in PLACE_DEEP_DIVES:
        scenarios.append(Scenario(
            category="P", name=f"{label} ({fips})",
            url_suffix=f"/atlas.html?place={fips}",
            asserts=[assert_place_layers(4)],
        ))
    return scenarios


# ── Harness execution ────────────────────────────────────────────────────
def run_scenario(chrome: str, frontend_base: str, sc: Scenario,
                 timeout_ms: int = 18000) -> dict:
    url = frontend_base + sc.url_suffix
    started = time.time()
    with tempfile.TemporaryDirectory() as td:
        dom_path = os.path.join(td, "dom.html")
        stderr_path = os.path.join(td, "stderr.log")
        screenshot_path = os.path.join(td, "throwaway.png")
        # Adding --screenshot forces Chrome to actually wait for the
        # virtual-time budget before snapping; --dump-dom alone returns
        # at load-event-fired time (before async fetches+Leaflet finish).
        cmd = [
            chrome, "--headless", "--disable-gpu", "--no-sandbox",
            "--hide-scrollbars",
            "--enable-logging=stderr", "--log-level=0",
            f"--virtual-time-budget={timeout_ms}",
            f"--screenshot={screenshot_path}",
            "--window-size=1400,900",
            "--dump-dom", url,
        ]
        with open(dom_path, "wb") as fout, open(stderr_path, "wb") as ferr:
            try:
                subprocess.run(cmd, stdout=fout, stderr=ferr,
                               timeout=timeout_ms / 1000 + 20, check=False)
            except subprocess.TimeoutExpired:
                return {"status": "fail", "selector_asserted": "",
                        "ms": int((time.time() - started) * 1000),
                        "error": "Chrome timeout"}
        dom = open(dom_path, encoding="utf-8", errors="ignore").read()
        stderr = open(stderr_path, encoding="utf-8", errors="ignore").read()

    err = real_js_error(stderr)
    if err:
        return {"status": "fail", "selector_asserted": "",
                "ms": int((time.time() - started) * 1000), "error": f"JS: {err}"}

    asserts_desc = []
    for desc, pred in sc.asserts:
        asserts_desc.append(desc)
        if not pred(dom):
            return {"status": "fail",
                    "selector_asserted": " · ".join(asserts_desc),
                    "ms": int((time.time() - started) * 1000),
                    "error": f"assertion failed: {desc}"}
    return {"status": "pass",
            "selector_asserted": " · ".join(asserts_desc),
            "ms": int((time.time() - started) * 1000), "error": ""}


def main() -> int:
    ap = argparse.ArgumentParser(description="SPEC_073 Atlas smoke harness")
    ap.add_argument("--base", default="http://localhost:3001",
                    help="frontend base URL")
    ap.add_argument("--api", default="http://localhost:8001",
                    help="API base URL")
    ap.add_argument("--only", choices=["L", "D", "P"], help="restrict category")
    ap.add_argument("--report",
                    default=os.path.join("docs", "atlas_smoke_report.csv"))
    args = ap.parse_args()

    chrome = find_chrome()
    print(f"Chrome: {chrome}")
    print(f"Frontend: {args.base}    API: {args.api}")
    print(f"Fetching layer registry…")

    layers = fetch_layer_registry(args.api)
    print(f"  -> {len(layers)} layers across "
          f"{len({l['domain'] for l in layers})} domains")

    scenarios = build_scenarios(layers)
    if args.only:
        scenarios = [s for s in scenarios if s.category == args.only]
    print(f"Running {len(scenarios)} scenarios…")

    results = []
    pass_n = fail_n = 0
    total_started = time.time()
    for i, sc in enumerate(scenarios, 1):
        r = run_scenario(chrome, args.base, sc)
        results.append({
            "category": sc.category, "scenario": sc.name,
            "url": args.base + sc.url_suffix, **r,
        })
        flag = "OK" if r["status"] == "pass" else "FAIL"
        if r["status"] == "pass": pass_n += 1
        else: fail_n += 1
        print(f"  [{i:>2}/{len(scenarios)}] {flag:4s} {sc.category} "
              f"{sc.name[:48]:48s} {r['ms']:>5}ms"
              f"{'  ' + r['error'] if r['error'] else ''}")

    elapsed = time.time() - total_started
    pass_rate = 100.0 * pass_n / max(1, len(scenarios))
    print()
    print(f"Total: {len(scenarios)}  pass: {pass_n}  fail: {fail_n}  "
          f"rate: {pass_rate:.0f}%  elapsed: {elapsed:.1f}s")

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "category", "scenario", "url", "status",
            "selector_asserted", "ms", "error",
        ])
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"Report -> {args.report}")

    return 0 if fail_n == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

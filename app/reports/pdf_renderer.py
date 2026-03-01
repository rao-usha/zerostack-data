"""
PDF Renderer for Nexdata Reports.

Converts self-contained HTML reports to PDF using Playwright (Chromium).
Injects a comprehensive @media print stylesheet that transforms the web
layout into a professional Investment Committee memo format:
  - Full cover page, serif body typography, sans-serif headings
  - Section-per-page with clean "1. Title" headers (no circular badges)
  - KPI strips as compact inline dividers
  - Charts constrained to 280px, side-by-side → stacked
  - Grid/flex → block+inline-block (Chromium break-inside bug workaround)
  - No shadows, no border-radius, no gradients
"""

import logging
import tempfile
import os
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Print CSS — IC Memo professional styling
# ---------------------------------------------------------------------------

PRINT_CSS = r"""
/* ================================================================
   PDF / Print Overrides — IC Memo Professional Styling
   ================================================================ */

/* Preserve all colors/backgrounds in print */
* {
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
}

/* Force light theme */
:root, html, html[data-theme="dark"] {
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
    color-scheme: light !important;
}
body {
    background: #fff !important;
    color: #2d3748 !important;
}

/* Hide interactive elements */
.theme-toggle, .theme-switch { display: none !important; }

@media print {

    /* ================================================================
       TYPOGRAPHY — Dual-font IC memo style
       ================================================================ */
    body {
        font-family: Georgia, 'Times New Roman', serif;
        font-size: 10.5pt;
        line-height: 1.5;
        color: #2d3748;
    }

    /* Headings stay sans-serif */
    h1, h2, h3, h4, h5, h6,
    .section-header h2,
    .page-header h1,
    .kpi-card .label,
    .chart-title,
    .toc h2,
    .metric-label {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, 'Helvetica Neue', Arial, sans-serif;
    }

    /* ================================================================
       GLOBAL CLEANUP — Remove web artifacts
       ================================================================ */

    * {
        box-shadow: none !important;
        transition: none !important;
        animation: none !important;
        float: none !important;
    }

    /* Sharp corners — document, not widget */
    .section, .kpi-card, .toc, .callout, .thesis-box,
    .metric-card, .scenario-card, .segment-card,
    .profile-card, .pill {
        border-radius: 0 !important;
    }

    body, .container {
        background: #fff !important;
    }

    .container {
        max-width: 100% !important;
        padding: 0 !important;
        margin: 0 !important;
    }

    /* Kill hover effects */
    .data-table tbody tr:hover { background: none !important; }
    .toc a:hover { background: none !important; }

    /* ================================================================
       COVER PAGE — Full first page, centered, authoritative
       ================================================================ */
    .page-header {
        min-height: 8.5in;
        display: block !important;
        padding: 0 !important;
        text-align: center;
        background: #fff !important;
        color: #1a365d !important;
        border-bottom: 3pt solid #1a365d;
        position: relative;
        break-after: page;
    }

    .page-header .container {
        display: block !important;
        position: absolute;
        top: 35%;
        left: 0;
        right: 0;
        text-align: center;
    }

    .page-header h1 {
        font-size: 26pt;
        font-weight: 700;
        color: #1a365d;
        margin-bottom: 6pt;
        letter-spacing: -0.5px;
    }

    .page-header .subtitle {
        font-size: 12pt;
        color: #4a5568;
        opacity: 1;
        font-family: Georgia, 'Times New Roman', serif;
        font-style: italic;
    }

    .page-header .badge {
        display: inline-block;
        background: none !important;
        border: 2pt solid #1a365d;
        color: #1a365d;
        font-size: 9pt;
        padding: 5pt 14pt;
        margin-top: 20pt;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }

    /* ================================================================
       KPI STRIP — Compact printed dashboard strip
       ================================================================ */
    .kpi-strip {
        display: block !important;
        border: 1pt solid #cbd5e0;
        margin: 0 0 14pt 0 !important;
        padding: 0 !important;
        overflow: hidden;
        break-inside: avoid;
    }

    .kpi-card {
        display: inline-block !important;
        width: 24% !important;
        vertical-align: top;
        border-right: 1pt solid #e2e8f0;
        padding: 7pt 8pt !important;
        text-align: center;
        background: #fff !important;
        margin: 0 !important;
    }
    .kpi-card:last-child { border-right: none; }

    .kpi-card .value {
        font-size: 17pt;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, 'Helvetica Neue', Arial, sans-serif;
        font-weight: 700;
        color: #1a365d;
    }
    .kpi-card .label {
        font-size: 7pt;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #718096;
    }
    .kpi-card .delta { font-size: 8pt; }

    /* ================================================================
       TABLE OF CONTENTS — Clean print TOC
       ================================================================ */
    .toc {
        break-after: page;
        border: none !important;
        padding: 0 !important;
        background: #fff !important;
    }
    .toc h2 {
        font-size: 13pt;
        color: #1a365d;
        text-transform: uppercase;
        letter-spacing: 1px;
        border-bottom: 2pt solid #1a365d;
        padding-bottom: 5pt;
        margin-bottom: 10pt;
    }
    .toc-grid {
        display: block !important;
        column-count: 2;
        column-gap: 20pt;
    }
    .toc a {
        display: block;
        padding: 3pt 0;
        border-bottom: 1pt dotted #cbd5e0;
        color: #2d3748 !important;
        font-size: 9.5pt;
        font-family: Georgia, 'Times New Roman', serif;
        text-decoration: none !important;
        break-inside: avoid;
    }
    .toc a:hover { background: none !important; }
    .toc-num {
        display: inline-block !important;
        width: 18pt;
        height: auto !important;
        min-height: 0 !important;
        background: none !important;
        color: #1a365d;
        font-weight: 700;
        font-size: 9.5pt;
        border-radius: 0 !important;
        text-align: left;
        line-height: inherit !important;
        padding: 0 !important;
    }

    /* ================================================================
       SECTIONS — Page-per-section, clean headers
       ================================================================ */
    .section {
        break-before: page;
        border: none !important;
        background: #fff !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: visible !important;
    }

    .section-header {
        display: block !important;
        padding: 0 0 6pt 0 !important;
        margin-bottom: 10pt !important;
        border-bottom: 1.5pt solid #cbd5e0;
        break-after: avoid;
        background: none !important;
    }

    /* Inline "1." numbering instead of circular badge */
    .section-number {
        display: inline !important;
        width: auto !important;
        height: auto !important;
        min-width: 0 !important;
        min-height: 0 !important;
        background: none !important;
        color: #1a365d;
        font-size: 13pt;
        font-weight: 700;
        border-radius: 0 !important;
        margin-right: 4pt;
        padding: 0 !important;
        vertical-align: baseline;
        line-height: inherit !important;
    }
    .section-number::after {
        content: ".";
    }

    .section-header h2 {
        display: inline !important;
        font-size: 13pt;
        font-weight: 700;
        color: #1a365d;
        vertical-align: baseline;
    }

    .section-body {
        padding: 0 !important;
    }
    .section-body p {
        font-size: 10.5pt;
        line-height: 1.5;
        color: #2d3748;
        margin-bottom: 7pt;
    }

    /* ================================================================
       HEADINGS — Keep with following content
       ================================================================ */
    h2, h3, h4 {
        break-after: avoid;
        orphans: 3;
        widows: 3;
    }
    h3 {
        font-size: 11pt;
        font-weight: 700;
        color: #2d3748;
        margin: 10pt 0 5pt 0;
    }
    h4 {
        font-size: 10pt;
        font-weight: 600;
        color: #4a5568;
        margin: 7pt 0 4pt 0;
    }

    /* ================================================================
       TABLES — Professional print tables
       ================================================================ */
    table { break-inside: auto; }
    thead { display: table-header-group; }
    tfoot { display: table-footer-group; }
    tr { break-inside: avoid; break-after: auto; }

    .data-table {
        font-size: 8.5pt;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, 'Helvetica Neue', Arial, sans-serif;
        border-collapse: collapse;
        width: 100%;
        margin: 6pt 0;
    }

    .data-table thead th {
        background: #f1f5f9 !important;
        color: #475569;
        font-size: 7.5pt;
        font-weight: 700;
        padding: 4pt 5pt;
        border-bottom: 1.5pt solid #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }

    .data-table tbody td {
        padding: 3.5pt 5pt;
        border-bottom: 0.5pt solid #e2e8f0;
        font-size: 8.5pt;
        line-height: 1.3;
    }

    .data-table tbody tr:nth-child(even) {
        background: #f8fafc !important;
    }

    .data-table tfoot td {
        border-top: 1.5pt solid #94a3b8;
        font-weight: 700;
        padding: 4pt 5pt;
        background: #f1f5f9 !important;
    }

    /* ================================================================
       CHARTS — Constrained, stacked, no split
       ================================================================ */
    .chart-container {
        break-inside: avoid;
        max-height: 280px !important;
    }
    .chart-container canvas {
        max-height: 260px !important;
    }
    .chart-container.tall   { height: 280px !important; }
    .chart-container.medium { height: 240px !important; }
    .chart-container.short  { height: 200px !important; }

    .chart-title {
        font-size: 8.5pt;
        font-weight: 700;
        color: #475569;
        margin-bottom: 3pt;
        break-after: avoid;
    }

    /* Side-by-side → stacked (grid breaks page-break in Chromium) */
    .chart-row {
        display: block !important;
    }
    .chart-row > * {
        margin-bottom: 10pt;
        break-inside: avoid;
    }

    .chart-legend { break-inside: avoid; }

    /* ================================================================
       CALLOUT / THESIS BOXES — Print-friendly
       ================================================================ */
    .callout {
        border-left: 3pt solid #2b6cb0;
        background: #f7fafc !important;
        padding: 7pt 10pt;
        margin: 7pt 0;
        font-size: 9.5pt;
        break-inside: avoid;
    }
    .callout.warn  { border-left-color: #ed8936; background: #fffaf0 !important; }
    .callout.good  { border-left-color: #38a169; background: #f0fff4 !important; }

    .thesis-box {
        background: #f7fafc !important;
        border: 1.5pt solid #2b6cb0;
        padding: 10pt 14pt;
        margin: 8pt 0;
        break-inside: avoid;
    }
    .thesis-box h3 { font-size: 10.5pt; margin-bottom: 5pt; }
    .thesis-box li { font-size: 9.5pt; margin-bottom: 3pt; }

    /* ================================================================
       GRID → BLOCK (Chromium page-break + grid/flex bug workaround)
       ================================================================ */
    .metric-grid, .segments-grid, .team-grid,
    .grid-2, .provider-split-grid, .wealth-composition-grid,
    .deal-scenario-grid {
        display: block !important;
    }

    .metric-card, .segment-card {
        display: inline-block !important;
        width: 48% !important;
        vertical-align: top;
        margin-bottom: 8pt;
        margin-right: 2%;
        border: 1pt solid #e2e8f0;
        padding: 7pt 9pt;
        break-inside: avoid;
        background: #fff !important;
    }

    .scenario-card {
        display: inline-block !important;
        width: 31% !important;
        vertical-align: top;
        margin-bottom: 8pt;
        margin-right: 1.5%;
        padding: 7pt 9pt;
        break-inside: avoid;
        background: #fff !important;
    }

    .profile-card {
        break-inside: avoid;
        border: 1pt solid #e2e8f0;
        padding: 8pt;
        margin-bottom: 8pt;
        background: #fff !important;
    }

    /* ================================================================
       PILLS / BADGES — Print-safe
       ================================================================ */
    .pill {
        border: 1pt solid #94a3b8;
        font-size: 7.5pt;
        padding: 1pt 5pt;
    }

    /* Score bars — ensure visible */
    .score-bar {
        height: 6px !important;
    }

    /* ================================================================
       CAPITAL STACK / WATERFALL — Keep together
       ================================================================ */
    .capital-stack, .pnl-waterfall {
        break-inside: avoid;
    }

    /* ================================================================
       FOOTER — Last page
       ================================================================ */
    .page-footer {
        break-before: page;
        background: #fff !important;
    }
    .page-footer .notes li {
        font-size: 8.5pt;
        line-height: 1.4;
    }

    /* ================================================================
       LINKS — No URL printing
       ================================================================ */
    a[href]::after { content: none; }
    a {
        color: #2b6cb0 !important;
        text-decoration: none !important;
    }

    /* ================================================================
       HIGHLIGHT TABLES — Keep structural styling
       ================================================================ */
    .highlight-table {
        break-inside: avoid;
    }
    .highlight-table tr.emerging-highlight {
        background: #f0fff4 !important;
    }
}
"""


def prepare_html_for_pdf(html: str) -> str:
    """
    Prepare HTML report for Playwright PDF rendering.

    - Forces light theme attribute
    - Injects IC memo print stylesheet
    - Keeps all <script> tags (Chromium executes JS for Chart.js)
    """
    # Force light theme on <html> tag
    html = re.sub(
        r'<html([^>]*)data-theme="[^"]*"',
        r'<html\1data-theme="light"',
        html,
    )
    if 'data-theme=' not in html:
        html = html.replace('<html', '<html data-theme="light"', 1)

    # Inject print CSS before closing </style>
    if '</style>' in html:
        html = html.replace('</style>', PRINT_CSS + '\n</style>', 1)
    elif '</head>' in html:
        html = html.replace(
            '</head>', f'<style>{PRINT_CSS}</style>\n</head>', 1
        )
    else:
        html = f'<style>{PRINT_CSS}</style>' + html

    return html


def render_pdf(html: str) -> bytes:
    """
    Convert HTML report to PDF bytes using Playwright's Chromium.

    Launches headless Chromium, loads the HTML, waits for Chart.js to
    render, then exports as PDF. The @media print stylesheet transforms
    the web layout into a professional IC memo format.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise ImportError(
            "playwright is required for PDF export. "
            "Install with: pip install playwright && playwright install chromium"
        )

    pdf_html = prepare_html_for_pdf(html)

    # Write to temp file — Playwright loads file:// URLs cleanly
    tmp = tempfile.NamedTemporaryFile(
        suffix=".html", delete=False, mode="w", encoding="utf-8"
    )
    try:
        tmp.write(pdf_html)
        tmp.close()
        file_url = f"file://{tmp.name}"

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1100, "height": 900})

            # Load page — JS executes, Chart.js renders on canvases
            page.goto(file_url, wait_until="networkidle")

            # Smart wait: check if Chart.js loaded, or fallback displayed
            try:
                page.wait_for_function(
                    "() => window.CHARTJS_AVAILABLE === true "
                    "|| document.querySelectorAll("
                    "'.chart-fallback[style*=\"display: block\"]'"
                    ").length > 0",
                    timeout=10000,
                )
            except Exception:
                pass  # proceed even if charts didn't load

            # Brief settle time for animations to finish
            page.wait_for_timeout(1500)

            # Generate PDF — Chromium switches to print media,
            # our @media print CSS transforms the layout
            pdf_bytes = page.pdf(
                format="Letter",
                print_background=True,
                margin={
                    "top": "0.75in",
                    "bottom": "0.85in",
                    "left": "0.75in",
                    "right": "0.75in",
                },
                display_header_footer=True,
                header_template='<span></span>',
                footer_template="""
                    <div style="width:100%; text-align:center; font-size:8px;
                                color:#94a3b8; font-family:Georgia, serif;
                                padding-top:4px;">
                        <span style="float:left; padding-left:0.75in;
                                     font-size:7px; text-transform:uppercase;
                                     letter-spacing:0.5px;">
                            Confidential
                        </span>
                        Page <span class="pageNumber"></span>
                        of <span class="totalPages"></span>
                        <span style="float:right; padding-right:0.75in;
                                     font-size:7px;">
                            Nexdata Investment Intelligence
                        </span>
                    </div>
                """,
            )

            browser.close()

        logger.info(f"PDF generated via Playwright: {len(pdf_bytes):,} bytes")
        return pdf_bytes

    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
        raise RuntimeError(f"PDF generation failed: {e}") from e
    finally:
        os.unlink(tmp.name)

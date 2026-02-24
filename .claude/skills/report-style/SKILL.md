---
name: report-style
description: Guidelines for generating business reports in self-contained HTML with consistent structure, visual design, and analytical rigor.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Glob
  - Grep
---

# Report Style Guide

Follow this guide so that every report has a consistent structure, visual design, and level of analytical rigor.

Use the exemplar report as ground-truth references when in doubt:

`tenants/group_iii/references/2025q2_reports/ceo_quarterly_2025q2.html`

For the full CSS, see `css_reference.md`. For Chart.js conventions, see `chartjs_conventions.md`.

## 1. Report Structure

Every report is a single self-contained `.html` file with inline CSS and inline JavaScript. The only external dependency is Chart.js loaded from CDN.

The page follows this top-to-bottom anatomy:

- **Page header** -- full-width gradient banner with title, subtitle, a metadata badge, and a dark mode toggle.
- **KPI strip** -- a row of 5 elevated cards that visually overlap the header, showing the most important metrics at a glance.
- **Table of contents** -- a compact card listing all sections with numbered links for quick navigation.
- **Numbered sections** -- the body of the report, typically 5-8 sections. Each section is a white card containing:
  - A numbered header (circled number + section title).
  - A narrative paragraph summarizing the data.
  - A chart row (typically two charts side by side).
  - One or more data tables.
  - One or more callout boxes (insights, warnings, positive signals).
- **Footer** -- data quality notes & methodology, followed by a one-line generation stamp.

### Skeleton

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{Company} - {Report Title} - {Period}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
  <style>
    /* Full CSS (see css_reference.md) */
  </style>
</head>
<body>

<!-- 1. Page Header -->
<div class="page-header">
  <div class="container">
    <div>
      <h1>{Company} - {Report Title}</h1>
      <div class="subtitle">{Date Range} ({Period Label})</div>
    </div>
    <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap">
      <div class="theme-toggle" id="themeToggle" role="switch" aria-label="Toggle dark mode" aria-checked="false" tabindex="0">
        <div class="toggle-track"><div class="toggle-thumb"></div></div>
      </div>
      <div class="badge">{Data source metadata}</div>
    </div>
  </div>
</div>

<!-- 2. KPI Strip -->
<div class="container">
  <div class="kpi-strip">
    <!-- 5 kpi-card elements -->
  </div>
</div>

<!-- 3. Table of Contents -->
<div class="container">
  <div class="toc">
    <h2>Contents</h2>
    <div class="toc-grid">
      <a href="#section-id"><span class="toc-num">1</span> Section Title</a>
      <!-- One link per section -->
    </div>
  </div>
</div>

<!-- 4. Numbered Sections -->
<div class="container">
  <div class="section" id="section-id">
    <div class="section-header">
      <div class="section-number">1</div>
      <h2>Section Title</h2>
    </div>
    <div class="section-body">
      <p>Narrative paragraph...</p>
      <div class="chart-row">
        <!-- Two chart containers side by side -->
      </div>
      <table class="data-table">...</table>
      <div class="callout warn">...</div>
    </div>
  </div>
  <!-- More sections... -->
</div>

<!-- 5. Footer -->
<div class="container">
  <div class="page-footer">
    <div class="notes">
      <h3>Data Quality Notes & Methodology</h3>
      <ul>
        <li>...</li>
      </ul>
    </div>
    <div>Report generated {date} | {data coverage} | {source} | {filters}</div>
  </div>
</div>

<script>
  /* Chart.js code (see chartjs_conventions.md) */
  /* Dark mode IIFE (see chartjs_conventions.md § Dark Mode Re-Theming) */
</script>
</body>
</html>
```

## 2. Visual Design System

All colors, typography, spacing, and responsive breakpoints are defined in `css_reference.md`. Copy that CSS verbatim into every report. Do not hard-code hex values; always use the CSS custom properties (e.g., `var(--primary)`).

## 3. Component Reference

### 3.1 KPI Cards

The KPI strip is a 5-column grid sitting between the header and the first section. Each card shows one top-level metric.

```html
<div class="kpi-card">
  <div class="label">Metric Name</div>
  <div class="value">$24.1M</div>
  <div class="delta down">-31.3% vs Q2 2024</div>
</div>
```

**Rules:**

- Exactly 5 cards per report. Choose the 5 most important KPIs for the report's audience.
- The **label** is a short uppercase name (e.g., "Q2 Revenue", "Shipments").
- The **value** is formatted concisely: use `$24.1M` not `$24,091,297`; use `16,692` for counts; use `51.7%` for percentages.
- The **delta** describes the change vs. a comparison period. Apply one of three classes:
  - `.up` (green) for favorable changes.
  - `.down` (red) for unfavorable changes.
  - `.neutral` (gray) when there is no meaningful change.
- Whether "up" is favorable depends on the metric. A revenue decrease is `.down`; a cost decrease might be `.up`.

### 3.2 Table of Contents

The TOC sits between the KPI strip and the first numbered section. It is a white card with a responsive grid of anchor links.

```html
<div class="container">
  <div class="toc">
    <h2>Contents</h2>
    <div class="toc-grid">
      <a href="#section-id"><span class="toc-num">1</span> Section Title</a>
      <a href="#another-section"><span class="toc-num">2</span> Another Section</a>
      <!-- One link per section -->
    </div>
  </div>
</div>
```

**Rules:**

- Include one `<a>` per numbered section.
- The `href` must match the `id` on the corresponding `.section` div.
- Use lowercase kebab-case IDs (e.g., `revenue-summary`, `cash-collections`).
- The `.toc-num` span mirrors the section number in a smaller (22px) circle.
- The `.toc-grid` auto-fills columns at 280px minimum, so links flow into 2-3 columns on wide screens and stack on narrow ones.
- Use `&amp;` for ampersands in link text since it is inside HTML attributes context.

### 3.3 Sections

Each section is wrapped in a `.section` card. Every section must have an `id` attribute for TOC anchor linking.

**Section header:**

```html
<div class="section" id="revenue-summary">
  <div class="section-header">
    <div class="section-number">1</div>
    <h2>Revenue Summary</h2>
  </div>
```

The number is a 28x28 circle with white text on a `var(--primary)` background. Number sections sequentially starting from 1.

**Section body** contains, in order:

1. A narrative `<p>` (see Section 5 for writing guidelines).
2. A `.chart-row` with two chart containers (optional -- some sections may have only a table or only charts).
3. A `.data-table` (optional).
4. One or more `.callout` boxes (optional but encouraged).

### 3.4 Data Tables

```html
<table class="data-table">
  <thead>
    <tr>
      <th>Label Column</th>
      <th class="right">Numeric Column</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td class="bold">Row Label</td>
      <td class="right">$14,419,036</td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td><strong>Total</strong></td>
      <td class="right">$24,091,297</td>
    </tr>
  </tfoot>
</table>
```

**Rules:**

- Left-align text columns; right-align numeric columns with `.right`.
- Use `.bold` on the first cell of each row when it serves as a row label.
- Use `font-variant-numeric: tabular-nums` (already set via `.right`) so numbers align vertically.
- Use `tfoot` for totals rows. Omit if not applicable.
- For change/delta columns, apply `.change-positive` (green) or `.change-negative` (red).
- Format monetary values with `$` and comma separators. Use abbreviated forms (`$14.4M`) for large values in chart tooltips and summaries, but full values (`$14,419,036`) in detailed tables when precision matters.
- Percentages: include `+` or `-` sign and the `%` suffix. Use `pp` for percentage-point changes.

### 3.5 Chart Containers

Charts sit inside a `.chart-row` (2-column grid) or a single full-width container.

```html
<div class="chart-row">
  <div>
    <div class="chart-title">Chart Title Here</div>
    <div class="chart-container tall">
      <canvas id="chartUniqueId"></canvas>
    </div>
  </div>
  <div>
    <div class="chart-title">Second Chart Title</div>
    <div class="chart-container tall">
      <canvas id="chartAnotherId"></canvas>
    </div>
  </div>
</div>
```

**Size classes:**

- `.tall` -- 360px. Use for primary charts with many data points.
- `.medium` -- 300px. Use for secondary charts or charts with fewer data points.
- `.short` -- 240px. Use sparingly for compact sparkline-style charts.

Every `<canvas>` must have a unique `id`. Give IDs descriptive camelCase names like `chartQuarterlyRevenue`, `chartWarehouseTxn`.

### 3.6 Callout Boxes

Three variants:

```html
<!-- Info (blue, default) -->
<div class="callout">
  <strong>Insight:</strong> Description of an analytical observation.
</div>

<!-- Warning (orange) -->
<div class="callout warn">
  <strong>Risk:</strong> Description of a risk or concern.
</div>

<!-- Positive (green) -->
<div class="callout good">
  <strong>Positive signal:</strong> Description of a favorable finding.
</div>
```

**Rules:**

- Always start with a `<strong>` label that categorizes the callout (e.g., "Risk:", "Note:", "Insight:", "Positive signal:", "Attention:", "Recommendation:").
- Keep callouts to 1-3 sentences.
- Use `.warn` for risks, deteriorating trends, data quality concerns.
- Use `.good` for positive developments, efficiency gains, favorable comparisons.
- Use the default (blue) for neutral observations, methodology notes, insights that are neither positive nor negative.

### 3.7 Footer

The footer has two parts:

1. A **notes card** with a bulleted list of data quality notes and methodology explanations.
2. A **one-line generation stamp** below the card.

```html
<div class="page-footer">
  <div class="notes">
    <h3>Data Quality Notes & Methodology</h3>
    <ul>
      <li>Data source and layer.</li>
      <li>Key filters applied (e.g., CompanyID, order types).</li>
      <li>How key metrics are calculated.</li>
      <li>Known data quality issues or limitations.</li>
      <li>Currency and unit conventions.</li>
    </ul>
  </div>
  <div>Report generated {month year} | Data through {end date} | Source: {system} | {key filters}</div>
</div>
```

**Rules:**

- Document every non-obvious filter or exclusion.
- Explain calculated metrics (e.g., "Margin % is the order-weighted average of margin_pct").
- Call out known data quality issues (e.g., stale snapshots, sentinel dates, low field population rates).
- This section builds reader trust. Be thorough.

### 3.8 Dark Mode Toggle

Every report includes a light/dark mode toggle in the page header. It respects the system `prefers-color-scheme` on load and allows manual override.

**HTML** (placed next to the badge in the header):

```html
<div class="theme-toggle" id="themeToggle" role="switch" aria-label="Toggle dark mode" aria-checked="false" tabindex="0">
  <div class="toggle-track"><div class="toggle-thumb"></div></div>
</div>
```

The toggle is a 48x26px pill with a 22px circular thumb. The thumb displays a sun icon (light mode) or moon icon (dark mode) via CSS `::after` pseudo-element. Icons use `\2600\FE0E` (sun) and `\263E\FE0E` (moon) with the text variation selector to prevent emoji rendering.

**Rules:**

- The CSS for the toggle and all `[data-theme="dark"]` overrides is part of `css_reference.md`. Copy it verbatim.
- The JavaScript IIFE that handles system detection, manual toggle, and Chart.js re-theming is documented in `chartjs_conventions.md` under "Dark Mode Re-Theming". Place it at the end of the `<script>` block, after all charts are created.
- The toggle is keyboard-accessible (Enter/Space) with `role="switch"` and `aria-checked`.
- When the user clicks the toggle, it overrides the system preference. If the user has not clicked, the report follows system changes (e.g., switching macOS appearance).

## 4. Content and Writing Guidelines

### 4.1 Narrative Paragraphs

Each section opens with a single paragraph (occasionally two) that summarizes the key findings from that section's data.

**Rules:**

- **Lead with the headline number.** Bold the most important figure: "Q2 2025 revenue was **$24.1M**, down 31.3% from Q2 2024."
- **State the direction and magnitude of change.** Always include both the absolute value and the percentage change.
- **Attribute the drivers.** Name the specific customers, products, or categories responsible for the trend.
- **Keep it concise.** 3-5 sentences maximum. The charts and tables provide the detail.
- **Use plain business language.** Avoid jargon. Write for a CEO or VP who needs to make decisions, not for a data engineer.

### 4.2 Callout Box Content

- **Risks** should be specific and quantified: "Amazon Import alone represents $7.0M (29% of Q2 revenue)."
- **Positive signals** should connect the data to a business implication: "Collections exceeded invoicing by $5.5M, actively reducing the open AR balance."
- **Insights** should surface non-obvious patterns: "The Shopify channel appears to be replacing Swissgear.com orders."
- **Recommendations** should be actionable: "Review April POs still open (190K units) and verify expected arrival timelines."

### 4.3 Data Quality Notes

The footer notes section is critical for building trust with the reader. Document:

- The data source and transformation layer (e.g., "curated layer in Databricks, built by SQLMesh from the staged layer").
- Every filter applied to the data (e.g., CompanyID, order types, date ranges).
- How each key metric is calculated (e.g., "Revenue is based on fct_sales_order.order_total").
- Known limitations (e.g., "SalesPersonID is only 1.9% populated", "Inventory balance snapshot is stale since April 2024").
- Currency and unit conventions.

### 4.4 Number Formatting Conventions

| Context | Format | Example |
|---------|--------|---------|
| KPI card value | Abbreviated with unit | `$24.1M`, `16,692`, `51.7%` |
| Chart tooltip | Abbreviated with unit | `$24.1M`, `318.2K` |
| Table cell (detail) | Full precision with commas | `$14,419,036`, `422,004` |
| Table cell (summary) | Abbreviated when space is tight | `$7.3M` |
| Percentage change | Signed with % | `+5.7pp`, `-31.3%` |
| Percentage point change | Signed with pp | `+5.7pp`, `-3.9pp` |
| Date range | Spelled out in header, coded in charts | `April - June 2025`, `25Q2` |

## 5. Data Querying Workflow

### 5.1 Data Sources

Reports are built from the curated layer in Databricks, which contains fact and dimension tables produced by SQLMesh from the staged layer. Use `hippo_execute_query(tenant, schema, query)` to run SQL against the curated schema.

To discover available tables and their definitions:

- `hippo_list_tables(tenant, 'raw')` for raw tables.
- For curated and other layers, read the SQLMesh model files in `tenants/{tenant}/sqlmesh/{layer}/`.

### 5.2 Standard Filters

Apply these filters to every query unless the report specifically requires otherwise:

- **CompanyID:** Filter to the relevant company (e.g., `companyid = 2` for US operations).
- **Order types:** For sales metrics, use `ordertype IN ('SA', 'SO', 'CO', 'SP', 'WO')`.
- **Date ranges:** Define the reporting period and comparison period explicitly (e.g., Q2 2025 = 2025-04-01 to 2025-06-30).

### 5.3 Companion SQL File

For every report `{report_name}.html`, create a companion `{report_name}_sql.md` that documents every SQL query used. Structure it as:

- A header explaining the data source and filters.
- One numbered section per query, matching the report section numbers.
- Each section contains:
  - A short description of what the query produces and which report element consumes it.
  - The full SQL query in a fenced code block.

This companion file serves as an audit trail and makes it easy to re-run or modify queries later.

### 5.4 Query Patterns

Common query patterns used in the exemplar reports:

- **Quarterly aggregation:** `CONCAT(YEAR(date_col), 'Q', QUARTER(date_col))` for period labels.
- **YoY comparison:** Use `UNION ALL` or conditional aggregation with `YEAR(date_col)` to get both periods in one result.
- **Percentile calculations:** `PERCENTILE_APPROX(metric, 0.5)` for median, `0.9` for P90, `0.95` for P95.
- **Percentage rates:** `ROUND(100.0 * SUM(CASE WHEN condition THEN 1 ELSE 0 END) / COUNT(*), 1)`.
- **Joins:** Always join on `companyid` in addition to business keys when joining fact tables.

## 6. Report Generation Checklist

Follow these steps when building a new report:

1. **Understand the audience and scope.** Who will read this report? What decisions does it need to support? What time period does it cover?

2. **Identify the data sources.** List the curated tables needed. Read their definitions from the SQLMesh model files. Understand grain, available dimensions, and known data quality issues.

3. **Choose the 5 KPIs.** These are the first things the reader sees. Pick metrics that summarize the report's story in one glance. Include the comparison period delta for each.

4. **Plan the sections.** Typically 5-8 sections. Each section should focus on one analytical theme (e.g., revenue, customers, cash flow, operations). Order sections from most important to least important for the audience.

5. **Write and run the SQL queries.** Query the curated layer for each section's data. Record every query in the companion `_sql.md` file. Verify the numbers make sense (e.g., totals match, YoY changes are directionally correct).

6. **Build the HTML file.** Start from the skeleton in Section 1. Copy the full CSS from `css_reference.md`, including the dark mode overrides and toggle styles. Include the dark mode toggle in the page header (see Section 3.8). Populate the KPI strip, sections, tables, and callouts with the queried data. Build the Chart.js charts in the `<script>` block following `chartjs_conventions.md`. Add the dark mode IIFE at the end of the script block (see `chartjs_conventions.md`).

7. **Write the narratives.** For each section, write the opening paragraph following the guidelines in Section 4. Add callout boxes for risks, positive signals, and insights.

8. **Write the footer notes.** Document every filter, calculation methodology, and known data quality issue.

9. **Review the report.** Open the HTML file in a browser. Verify all charts render correctly. Check that table totals match KPI card values. Confirm YoY changes are calculated correctly. Ensure the narrative is consistent with the data shown.

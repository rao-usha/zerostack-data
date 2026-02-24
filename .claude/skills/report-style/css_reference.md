# Full CSS Reference

The CSS below is shared across all exemplar reports. Copy it verbatim into new reports. Do not modify it unless adding a genuinely new component type.

```css
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

/* KPI Cards */
.kpi-strip {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
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

/* Charts */
.chart-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
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

.change-positive { color: var(--accent-green); font-weight: 600; }
.change-negative { color: var(--accent-red); font-weight: 600; }
.change-neutral { color: var(--gray-500); }

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

/* Utility */
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
@media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
.mt-16 { margin-top: 16px; }
.mb-8 { margin-bottom: 8px; }

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
[data-theme="dark"] .toc { box-shadow: 0 2px 8px rgba(0,0,0,0.3); }
[data-theme="dark"] .page-footer .notes code {
  background: #4a5568;
  color: #e2e8f0;
}

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
.theme-toggle .toggle-thumb::after { content: '\2600\FE0E'; }
[data-theme="dark"] .theme-toggle .toggle-track { background: rgba(144,205,244,0.3); }
[data-theme="dark"] .theme-toggle .toggle-thumb { transform: translateX(22px); color: #63b3ed; }
[data-theme="dark"] .theme-toggle .toggle-thumb::after { content: '\263E\FE0E'; }
```

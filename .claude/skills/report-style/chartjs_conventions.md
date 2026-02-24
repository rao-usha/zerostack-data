# Chart.js Conventions

## Color Palette

Define these constants at the top of the `<script>` block:

```js
const BLUE = '#2b6cb0';
const BLUE_LIGHT = '#63b3ed';
const BLUE_DARK = '#2c5282';
const ORANGE = '#ed8936';
const GREEN = '#38a169';
const RED = '#e53e3e';
const GRAY = '#a0aec0';
const PURPLE = '#805ad5';
const TEAL = '#319795';
const PINK = '#d53f8c';
```

### Usage guidelines

- **Primary series:** `BLUE` for the current/focus period.
- **Comparison series:** `BLUE_LIGHT` or `GRAY` for prior periods.
- **Positive values:** `GREEN` for gains, inflows, favorable changes.
- **Negative values:** `RED` for losses, outflows, unfavorable changes.
- **Secondary/accent series:** `ORANGE` for overlay lines, secondary metrics.
- **Additional series (when needed):** `PURPLE`, `TEAL`, `PINK`.
- In bar charts, use per-bar coloring to highlight the focus period (e.g., the current quarter in `BLUE`, all others in `GRAY`).

## Chart.js Defaults

Set these at the top of the script block:

```js
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif";
Chart.defaults.font.size = 12;
Chart.defaults.color = '#4a5568';
```

## Helper Functions

Define formatting helpers:

```js
function millions(v) { return '$' + (v / 1e6).toFixed(1) + 'M'; }
function thousands(v) { return (v / 1e3).toFixed(1) + 'K'; }
```

## Common Chart Types

Use these chart patterns (all examples use `responsive: true` and `maintainAspectRatio: false`):

### Vertical bar chart
For time series (quarterly revenue, monthly shipments):
- `borderRadius: 4` on bars.
- Y-axis `beginAtZero: true` unless the data range makes that impractical.
- Grid lines: `color: '#edf2f7'` on the value axis, `display: false` on the category axis.
- Hide legend when there is only one dataset: `plugins: { legend: { display: false } }`.

### Grouped bar chart
For period-over-period comparisons (Q2 2024 vs Q2 2025):
- Legend at top: `position: 'top'`, `labels: { boxWidth: 12, padding: 16 }`.

### Horizontal bar chart
For ranked lists (top categories, top vendors):
- Set `indexAxis: 'y'`.
- Good for long labels that would overlap on a vertical axis.

### Combo bar + line chart
For dual-axis views (spend bars + count line, inflows/outflows bars + net line):
- Bars on the left axis, line on the right axis.
- Use `order` property to control z-order (lower order renders on top; give the line `order: 1`, bars `order: 2`).
- Line style: `pointRadius: 4-5`, `borderWidth: 2`, `tension: 0.3`.

### Stacked bar chart
For showing composition (inflows vs outflows stacked):
- Set `stacked: true` on the x-axis scale and optionally on the y-axis.
- Use `stack` property on datasets to group them.

### Doughnut chart
For proportional breakdowns (new vs repeat customers, carrier mix):
- `cutout: '45%'` to `'55%'`.
- `borderWidth: 2`, `borderColor: '#ffffff'` between segments.
- Legend at bottom or right: `position: 'bottom'` or `position: 'right'`.

### Line chart
For trends (concentration %, AOV over time):
- `tension: 0.3` for slight curve.
- `pointRadius: 5-6`, `pointBackgroundColor: [same as borderColor]`.
- Use `fill: true` with a transparent background for area effect when appropriate.

## Tooltip Formatting

Always provide a custom `tooltip.callbacks.label` that formats values with proper units:

```js
tooltip: {
  callbacks: {
    label: ctx => '$' + ctx.raw.toFixed(1) + 'M'
  }
}
```

## Scale Formatting

Format axis tick labels to match the data:

```js
// Monetary
ticks: { callback: v => '$' + v + 'M' }

// Counts
ticks: { callback: v => (v / 1000).toFixed(0) + 'K' }

// Percentages
ticks: { callback: v => v + '%' }

// Days
ticks: { callback: v => v + 'd' }
```

## Data Embedding

Embed all chart data directly in the JavaScript as arrays and objects. Do not fetch data from external endpoints. For large datasets, use JavaScript arrays and `forEach` loops to populate table bodies dynamically (see the exemplar reports for examples of `document.getElementById('tableBody').innerHTML += ...` patterns).

## Dark Mode Re-Theming

After all charts are created, include the dark mode IIFE (see below). It must re-theme every Chart.js instance when the user toggles dark mode or the system preference changes.

Two palettes:

```js
const LIGHT = { text: '#4a5568', grid: '#edf2f7', doughnutBorder: '#ffffff' };
const DARK  = { text: '#a0aec0', grid: '#4a5568', doughnutBorder: '#2d3748' };
```

The `applyTheme(isDark)` function iterates over `Chart.instances` and updates:
- Grid color on every scale (skip scales with `grid.display === false`).
- Tick color on every scale.
- Legend label color.
- Doughnut border color (`borderColor` on datasets of `type === 'doughnut'`).
- Call `chart.update('none')` (no animation) after updating each chart.

```js
(function() {
  var LIGHT = { text: '#4a5568', grid: '#edf2f7', doughnutBorder: '#ffffff' };
  var DARK  = { text: '#a0aec0', grid: '#4a5568', doughnutBorder: '#2d3748' };

  function applyTheme(isDark) {
    document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
    var toggle = document.getElementById('themeToggle');
    if (toggle) toggle.setAttribute('aria-checked', isDark ? 'true' : 'false');

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
  function currentIsDark() { return userOverride !== null ? userOverride : systemDark.matches; }
  applyTheme(currentIsDark());

  var toggleEl = document.getElementById('themeToggle');
  if (toggleEl) {
    toggleEl.addEventListener('click', function() { userOverride = !currentIsDark(); applyTheme(userOverride); });
    toggleEl.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleEl.click(); }
    });
  }
  systemDark.addEventListener('change', function() { if (userOverride === null) applyTheme(systemDark.matches); });
})();
```

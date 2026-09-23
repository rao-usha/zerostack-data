/*
 * SPEC_125 — DOM smoke test for frontend/status.html under jsdom.
 *
 * Run by tests/test_spec_125_dataset_status_page.py (TestStatusPageDom) when
 * node and the `jsdom` package are both available (NODE_PATH may point at a
 * node_modules holding jsdom); skipped otherwise. Usage:
 *     node tests/js/status_page_smoke.js <repo root> <scenario>
 * Prints one JSON line: {"ok": [...], "fail": [...]}.
 *
 * Timers the page registers with a delay >= 20 s (the slow tick, fallback
 * poll, SSE retry, coverage catch-up) are captured instead of scheduled, so a
 * scenario can fire them after moving the page's clock (Date.now) forward.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const root = process.argv[2];
const scenario = process.argv[3] || 'admin';

let html = fs.readFileSync(path.join(root, 'frontend', 'status.html'), 'utf8');
const auth = fs.readFileSync(path.join(root, 'frontend', 'js', 'auth.js'), 'utf8')
    .split('</script>').join('<\\/script>');
html = html.replace('<script src="/js/auth.js"></script>', () => '<script>' + auth + '</script>');

const ok = [], fail = [];
function check(cond, msg) { (cond ? ok : fail).push(msg); }
const sleep = ms => new Promise(r => setTimeout(r, ms));

function b64url(obj) {
    return Buffer.from(JSON.stringify(obj)).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
}

const STATUSES = ['current', 'awaiting_upstream', 'behind', 'stalled', 'failing', 'partial', 'blocked', 'dormant', 'never_run', 'unknown'];
function clocks(o) {
    return Object.assign({ last_run_at: null, last_run_status: null, last_run_store: null, last_run_duration_s: null,
        last_success_at: null, last_publish_at: null, coverage_through: null, expected_through: null,
        expectation_basis: null, lag_days: null, coverage_error: null }, o || {});
}
function ds(key, kind, status, producer, extra) {
    return Object.assign({ key, display_name: key.toUpperCase() + ' <img src=x onerror=alert(1)>', source: 'x', kind,
        grain: 'g', cadence: 'monthly', status_public: 'internal', producer, status,
        status_reason: '<script>bad()</script> reason',
        tables: [{ name: key + '_t', exists: true, rows: 10, rows_exact: false, bytes: 2048 }],
        rows_total: 10, rows_exact: false, clocks: clocks(), releases: null, schedule: null, blockers: [],
        can_run: { allowed: false, requires: 'admin', policy: 'idempotent', producer, run_path: null,
            blockers: [{ code: 'no_run_path', message: 'needs <b>payload</b>' }], warnings: [] } }, extra || {});
}

function boot(opts) {
    const calls = [], esList = [], captured = [];
    let clockOffset = 0;
    let version = 0;
    const statusBody = () => {
        const list = [
            ds('adv', 'filings', version ? 'current' : 'behind', 'bulk:sec_adv', {
                clocks: clocks({ coverage_through: '2026-07-31', expected_through: '2026-08-31', lag_days: 31,
                    expectation_basis: 'coverage', last_run_at: new Date(Date.now() + clockOffset - 300000).toISOString(),
                    last_run_status: 'success', coverage_error: opts.coverageError || null }),
                releases: { loaded: 3, unloaded: 1, failed: 0, superseded: 2, latest_release_key: 'r1', last_bytes: 100 } }),
            ds('firms', 'derived_mart', 'blocked', 'job:pe_mart_build#firms'),
            ds('fred', 'timeseries', 'dormant', 'dispatch:fred:series'),
            ds('tdb', 'timeseries', 'stalled', 'dispatch:treasury'),
            ds('power', 'geo', 'never_run', 'collector:power_plants'),
        ];
        const summary = {};
        STATUSES.forEach(s => { summary[s] = 0; });
        list.forEach(d => { summary[d.status] += 1; });
        return { generated_at: new Date(Date.now() + clockOffset).toISOString(), summary, count: list.length, total: list.length,
            worker: { worker_mode: opts.workerMode !== false, live_workers: 1, last_heartbeat_at: null },
            degraded: [], datasets: list };
    };
    const dom = new JSDOM(html, {
        url: 'http://localhost:3001/status.html', runScripts: 'dangerously', pretendToBeVisual: true,
        beforeParse(window) {
            const realNow = window.Date.now.bind(window.Date);
            window.Date.now = () => realNow() + clockOffset;
            const realSetInterval = window.setInterval.bind(window);
            const realSetTimeout = window.setTimeout.bind(window);
            window.setInterval = (fn, ms) => {
                if (ms >= 20000) { captured.push({ kind: 'interval', fn, ms }); return 100000 + captured.length; }
                return realSetInterval(fn, ms);
            };
            window.setTimeout = (fn, ms) => {
                if (ms >= 20000) { captured.push({ kind: 'timeout', fn, ms }); return 100000 + captured.length; }
                return realSetTimeout(fn, ms);
            };
            if (opts.role) {
                const tok = b64url({ alg: 'HS256' }) + '.' + b64url({ aud: 'nexdata-app', role: opts.role, exp: 4102444800 }) + '.sig';
                window.localStorage.setItem('nexdata_token', tok);
            }
            window.CSS = window.CSS || {};
            window.CSS.escape = window.CSS.escape || (s => String(s).replace(/["\\]/g, '\\$&'));
            window.alert = () => { fail.push('XSS executed'); };
            window.bad = () => { fail.push('XSS executed'); };
            window.fetch = async (url, init) => {
                calls.push([String(url), (init && init.method) || 'GET']);
                const u = new URL(String(url), 'http://localhost:3001');
                let body = null, status = 200;
                const headers = {};
                if (u.pathname === '/api/v1/datasets/status') body = statusBody();
                else if (u.pathname === '/api/v1/catalog') body = { datasets: [] };
                else if (u.pathname === '/api/v1/bulk/releases') body = { releases: [] };
                else if (u.pathname === '/api/v1/jobs') {
                    const producer = u.searchParams.get('producer');
                    if (producer === 'dispatch:treasury') {
                        headers['x-producer-scan-truncated'] = 'true';
                        body = [];
                    } else {
                        body = [{ id: 7, status: 'failed', created_at: '2026-09-20T00:00:00', config: { dataset: 'series' }, error_message: '<b>boom</b>' }];
                    }
                } else if (u.pathname === '/api/v1/pe/marts/builds') {
                    const admin = !opts.role || opts.role === 'admin';
                    body = { builds: [{ status: 'failed', started_at: '2026-09-22T00:00:00', finished_at: '2026-09-22T00:05:00',
                        refusal_reason: null, error: admin ? 'SECRET-TRACE psycopg2 boom' : null, error_hidden: !admin,
                        gate_results: { rows: { passed: false, detail: 'too few' } }, inputs: [] }] };
                } else if (u.pathname === '/api/v1/auth/stream-token') body = { stream_token: 'st' };
                else { status = 404; body = {}; }
                return { ok: status < 400, status, headers: { get: k => headers[String(k).toLowerCase()] || null },
                    json: async () => body };
            };
            window.EventSource = class {
                constructor(url) {
                    this.url = url; this.listeners = {}; esList.push(this);
                    realSetTimeout(() => { if (this.onopen) this.onopen(); }, 0);
                }
                addEventListener(n, f) { this.listeners[n] = f; }
                close() { this.closed = true; }
                emit(n, d) { this.listeners[n]({ data: JSON.stringify(d) }); }
            };
        },
    });
    return {
        dom, calls, esList, captured,
        advance(ms) { clockOffset += ms; },
        setVersion(v) { version = v; },
        fire(kind, ms) { captured.filter(c => c.kind === kind && c.ms === ms).forEach(c => c.fn()); },
        statusCalls() { return calls.filter(c => c[0] === '/api/v1/datasets/status').length; },
    };
}

function click(w, el) { el.dispatchEvent(new w.MouseEvent('click', { bubbles: true })); }

const scenarios = {
    async admin() {
        const b = boot({});
        const w = b.dom.window, doc = w.document;
        await sleep(300);
        check(doc.querySelectorAll('tr.ds-row').length === 5, 'five rows');
        check(!doc.body.innerHTML.includes('<img src="x"'), 'display names escaped');
        check(b.esList.length === 1, 'SSE opened in worker mode');
        check(b.captured.some(c => c.kind === 'interval' && c.ms === 60000), 'slow tick registered');
        check(!b.captured.some(c => c.kind === 'interval' && c.ms === 90000), 'no fallback poll while SSE is up');

        // recent runs: server-side producer filter, bare and qualified keys
        click(w, doc.querySelector('tr.ds-row[data-key="fred"]'));
        await sleep(100);
        check(b.calls.some(c => c[0] === '/api/v1/jobs?producer=dispatch%3Afred%3Aseries&limit=10'), 'qualified key queried by producer');
        check(!b.calls.some(c => c[0].includes('/api/v1/jobs?source=')), 'no client-side source query');
        check(doc.querySelector('tr.detail-row[data-detail-for="fred"]').textContent.includes('#7'), 'fred runs shown');
        click(w, doc.querySelector('tr.ds-row[data-key="tdb"]'));
        await sleep(100);
        check(b.calls.some(c => c[0] === '/api/v1/jobs?producer=dispatch%3Atreasury&limit=10'), 'bare key queried by producer');
        const tdb = doc.querySelector('tr.detail-row[data-detail-for="tdb"]').textContent;
        check(tdb.includes('search limit reached') && !tdb.includes('No ingestion jobs recorded'), 'truncated scan is not reported as no runs');

        // mart builds: error text for admins in the operator view only
        click(w, doc.querySelector('tr.ds-row[data-key="firms"]'));
        await sleep(100);
        check(doc.querySelector('tr.detail-row[data-detail-for="firms"]').textContent.includes('SECRET-TRACE'), 'admin operator sees build error');

        // quiet SSE: nothing arrives, the clock moves
        const lastRunCell = () => doc.querySelector('tr.ds-row[data-key="adv"] [data-rel="ago"]').textContent;
        const before = lastRunCell();
        const n0 = b.statusCalls();
        b.advance(11 * 60 * 1000);
        b.fire('interval', 60000);
        check(lastRunCell() !== before && lastRunCell().includes('m ago'), 'relative times re-rendered by the tick: ' + before + ' -> ' + lastRunCell());
        await sleep(100);
        check(b.statusCalls() === n0 + 1, 'safety refresh while SSE is live and quiet');
        check(!doc.getElementById('generated-at').classList.contains('stale'), 'fresh snapshot not stale');
        b.fire('interval', 60000);
        await sleep(100);
        check(b.statusCalls() === n0 + 1, 'no second refresh within the safety window');

        // stale flag when the snapshot ages (e.g. the refresh keeps failing)
        b.advance(11 * 60 * 1000);
        const gen = doc.getElementById('generated-at');
        // retick through the tick; the refresh that follows will clear it again
        b.captured.filter(c => c.kind === 'interval' && c.ms === 60000)[0].fn();
        check(gen.classList.contains('stale'), 'stale flag set on an old snapshot');
        await sleep(100);
        check(!gen.classList.contains('stale'), 'stale flag cleared by the refresh');

        // job events: one debounced refresh, changed row re-rendered
        b.setVersion(1);
        const n1 = b.statusCalls();
        b.esList[0].emit('job_started', { job_id: 1, job_type: 'bulk_ingest' });
        b.esList[0].emit('job_completed', { job_id: 1, job_type: 'bulk_ingest' });
        await sleep(3000);
        check(b.statusCalls() === n1 + 1, 'one debounced refresh for two job events');
        check(doc.querySelector('tr.ds-row[data-key="adv"] .job-status-badge').textContent.trim() === 'current', 'adv row updated');

        // public view hides build error text
        click(w, doc.querySelector('.seg button[data-view="public"]'));
        check(b.calls.every(c => c[1] === 'GET' || c[0] === '/api/v1/auth/stream-token'), 'only GET calls');
    },

    async inprocess() {
        const b = boot({ workerMode: false });
        const doc = b.dom.window.document;
        await sleep(300);
        check(b.esList.length === 0, 'no SSE when WORKER_MODE=0');
        check(b.captured.some(c => c.kind === 'interval' && c.ms === 90000), 'fallback poll started');
        check(!b.captured.some(c => c.kind === 'interval' && c.ms === 300000), 'no SSE retry timer');
        check(doc.getElementById('notices').textContent.includes('WORKER_MODE=0'), 'notice explains in-process mode');
        const n0 = b.statusCalls();
        b.fire('interval', 90000);
        await sleep(100);
        check(b.statusCalls() === n0 + 1, 'poll refreshes');
    },

    async viewer() {
        const b = boot({ role: 'viewer' });
        const w = b.dom.window, doc = w.document;
        await sleep(300);
        check(b.esList.length === 0, 'no SSE for a non-admin');
        click(w, doc.querySelector('tr.ds-row[data-key="firms"]'));
        await sleep(100);
        const t = doc.querySelector('tr.detail-row[data-detail-for="firms"]').textContent;
        check(!t.includes('SECRET-TRACE'), 'no build error text for a viewer');
        check(t.includes('shown to admins'), 'viewer told error text is admin-only');
        check(!b.calls.some(c => c[0].startsWith('/api/v1/jobs')), 'viewer does not call admin /jobs');
    },

    async coverage() {
        const b = boot({ coverageError: 'deadline' });
        await sleep(300);
        const t = b.captured.filter(c => c.kind === 'timeout' && c.ms === 20000);
        check(t.length === 1, 'one coverage catch-up scheduled');
        const n0 = b.statusCalls();
        t[0].fn();
        await sleep(100);
        check(b.statusCalls() === n0 + 1, 'catch-up re-fetches');
        check(b.captured.filter(c => c.kind === 'timeout' && c.ms === 20000).length <= 3, 'catch-ups are bounded');
    },
};

(async () => {
    try {
        await scenarios[scenario]();
    } catch (e) {
        fail.push('exception: ' + (e && e.stack || e));
    }
    console.log(JSON.stringify({ ok, fail }));
    process.exit(0);
})();

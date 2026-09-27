/*
 * SPEC_145 — DOM smoke test for frontend/catalog.html under jsdom.
 *
 * Run by tests/test_spec_145_catalog_browser.py when node and `jsdom` resolve
 * (NODE_PATH may point at a node_modules holding jsdom); skipped otherwise.
 *     node tests/js/catalog_page_smoke.js <repo root> <scenario>
 * Prints one JSON line: {"ok": [...], "fail": [...]}.
 *
 * Every catalog / DB string the mocks return carries markup that would run
 * script or add elements if it were rendered unescaped.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const root = process.argv[2];
const scenario = process.argv[3] || 'detail';

let html = fs.readFileSync(path.join(root, 'frontend', 'catalog.html'), 'utf8');
const auth = fs.readFileSync(path.join(root, 'frontend', 'js', 'auth.js'), 'utf8')
    .split('</script>').join('<\\/script>');
html = html.replace('<script src="/js/auth.js"></script>', () => '<script>' + auth + '</script>');

const ok = [], fail = [];
function check(cond, msg) { (cond ? ok : fail).push(msg); }
const sleep = ms => new Promise(r => setTimeout(r, ms));

const EVIL = '<img src=x onerror=bad()><script>bad()</script>';
const EVIL_URL = 'javascript:bad()';

function entry(key, extra) {
    return Object.assign({
        key, source: 'sec', display_name: 'Evil ' + EVIL, subtitle: 'Sub ' + EVIL,
        description: 'Desc ' + EVIL + ' long enough to be a catalog description of the data',
        kind: 'filings', grain: 'one row ' + EVIL, tables: ['evil_t'], table_patterns: [], primary_key: ['id'],
        producer: 'bulk:evil', also_produced_by: [], inputs: [], cadence: 'monthly', coverage_from: '2001-01-01',
        has_coverage_sql: true, rerun: 'idempotent', owner: 'owner ' + EVIL, slo_lag_hours: 48,
        rights: { license: 'lic ' + EVIL, license_url: EVIL_URL, redistribution: 'open', effective_redistribution: 'internal_only',
            attribution: 'attr ' + EVIL, reviewed: false, storage: 'forbidden', storage_max_age_days: null,
            commercial_use: 'allowed', share_alike: false, citation_url: 'https://example.com/terms', citation_quote: 'q ' + EVIL,
            confidence: 'high', notes: null, gate: ['storage_forbidden'], rights_hash: 'h', proposed: null },
        pii_class: 'business_contact', origin: 'official', status_public: 'internal', upstream_url: EVIL_URL,
        notes: 'note ' + EVIL, subtitle_x: null, keywords: ['pe'], spatial_coverage: 'US:state', coverage_basis: 'period',
        data_state: 'fabricated', limitations: ['LIMIT-ONE ' + EVIL, 'BUG-X second'], missing_tables: [], row_filters: [],
        verified_at: '2026-09-25', quality_flags: ['fabricated'],
    }, extra || {});
}

function searchRow(e) {
    return { key: e.key, display_name: e.display_name, subtitle: e.subtitle, description: e.description, kind: e.kind,
        source: e.source, status_public: e.status_public, data_state: e.data_state, quality_flags: e.quality_flags,
        redistribution: e.rights.redistribution, effective_redistribution: e.rights.effective_redistribution,
        rights_gate: e.rights.gate, storage: e.rights.storage, commercial_use: e.rights.commercial_use,
        pii_class: e.pii_class, origin: e.origin, keywords: e.keywords, identifiers: ['cik'], coverage_from: e.coverage_from,
        score: 10, matched: ['name'], highlight: 'hl ' + EVIL, row_estimate: 12345, rows_exact: false };
}

function boot(opts) {
    const calls = [];
    const evil = entry('evil');
    const clean = entry('clean', { display_name: 'Clean data', subtitle: null, data_state: 'ok', quality_flags: [], limitations: [],
        rights: Object.assign({}, evil.rights, { storage: 'allowed', gate: [] }), upstream_url: 'https://example.com/' });
    const catalog = [evil, clean];
    const dom = new JSDOM(html, {
        url: 'http://localhost:3001/catalog.html' + (opts.hash || ''), runScripts: 'dangerously', pretendToBeVisual: true,
        beforeParse(window) {
            window.alert = () => { fail.push('XSS executed (alert)'); };
            window.bad = () => { fail.push('XSS executed'); };
            window.scrollTo = () => {};
            window.fetch = async (url, init) => {
                calls.push([String(url), (init && init.method) || 'GET']);
                const u = new URL(String(url), 'http://localhost:3001');
                const p = u.pathname;
                let body = null, status = 200;
                if (p === '/api/v1/catalog') body = { count: 2, total: 2, datasets: catalog };
                else if (p === '/api/v1/catalog/search') body = {
                    q: u.searchParams.get('q'), count: 2, total: 2, filters: {},
                    facets: { kind: { filings: 2 }, source: { ['sec' + EVIL]: 2 }, data_state: { fabricated: 1, ok: 1 },
                        effective_redistribution: { internal_only: 2 }, redistribution: { open: 2 }, pii_class: { business_contact: 2 },
                        status_public: { internal: 2 }, identifier: { cik: 2 }, origin: { official: 2 }, keyword: { pe: 2 } },
                    results: catalog.map(searchRow) };
                else if (p === '/api/v1/datasets/status') body = { datasets: [
                    { key: 'evil', status: 'behind', status_reason: 'why ' + EVIL, clocks: { coverage_through: '2026-06-30', last_success_at: '2026-09-01T00:00:00' } },
                    { key: 'clean', status: 'current', status_reason: null, clocks: { coverage_through: '2026-08-31' } }] };
                else if (p === '/api/v1/catalog/evil' || p === '/api/v1/catalog/clean') body = Object.assign({}, catalog[0], {
                    live: { rows_total: 99, rows_exact: true, coverage_through: '2026-06-30', tables: [] },
                    quality: { score: 72.4, components: { completeness: 80 }, live_state: 'populated', live_state_reasons: ['r ' + EVIL],
                        flags: ['fabricated'], verified_state: 'fabricated', rules: { passed: 1, failed: 0 }, open_anomalies: 0,
                        profile: { oldest_profiled_at: '2026-09-01' },
                        row_trend: [{ date: '2026-09-01', rows: 10 }, { date: '2026-09-02', rows: 12 }],
                        tables: [{ table: 'evil_t' + EVIL, key_null_pct: { ['id' + EVIL]: 0.5 } }] } });
                else if (/\/schema$/.test(p)) body = { dataset: 'evil', tables_total: 1, tables_truncated: false, tables: [
                    { table: 'evil_t', exists: true, row_estimate: 99, primary_key: ['id'], coverage: { pct: 50 },
                      columns: [{ name: 'cik' + EVIL, pg_type: 'text', description: 'col ' + EVIL, pii: 'business_contact', semantic_type: 'cik', null_pct: 1.5, unit: 'u' + EVIL }] }] };
                else if (/\/sample$/.test(p)) {
                    if (opts.sampleRefused) { status = 403; body = { detail: 'restricted dataset: samples are admin-only ' + EVIL }; }
                    else body = { table: 'evil_t', masked: true, sampled: false, hidden_columns: 1, attribution: 'a ' + EVIL,
                        columns: [{ name: 'name' + EVIL, masked: true }], rows: [{ ['name' + EVIL]: 'v ' + EVIL }] };
                }
                else if (/\/lineage$/.test(p)) body = { key: 'evil', upstream: [{ key: 'up' + EVIL, depth: 1, via: ['declared'] }],
                    downstream: [{ key: 'clean', depth: 1, via: ['sql' + EVIL] }], producers: ['bulk:evil'], tables: ['evil_t'], views: [],
                    drift: [{ type: 'read_not_declared', message: 'm ' + EVIL }] };
                else if (p === '/api/v1/catalog/rights/evil') body = { dataset: 'evil', rights: catalog[0].rights, pii_class: 'business_contact',
                    origin: 'official', review_state: 'none', reviews: [] };
                else if (/\/jsonld$/.test(p)) { status = 403; body = { detail: { error: 'jsonld_refused', reasons: ['storage_forbidden'] } }; }
                else { status = 404; body = { detail: 'nope' }; }
                return { ok: status < 400, status, headers: { get: () => null }, json: async () => body };
            };
        },
    });
    return { dom, calls };
}

function noInjected(doc, where) {
    check(doc.querySelectorAll('img').length === 0, where + ': no injected <img>');
    const scripts = Array.from(doc.querySelectorAll('script')).filter(s => !s.id && s.textContent.indexOf('NexdataAuth') === -1);
    check(scripts.length === 0, where + ': no injected <script>');
    check(Array.from(doc.querySelectorAll('a[href]')).every(a => !/^javascript:/i.test(a.getAttribute('href'))), where + ': no javascript: links');
}

async function go(b, hash) {
    b.dom.window.location.hash = hash;
    await b.dom.window.CatalogPage.route();
    await sleep(50);
}

const scenarios = {
    async detail() {
        const b = boot({ hash: '#/dataset/evil' });
        const w = b.dom.window, doc = w.document;
        await w.CatalogPage.ready;
        await sleep(100);
        const view = doc.getElementById('view-detail');
        check(!view.hidden, 'detail view shown');
        check(doc.querySelector('.flag-banner') !== null, 'warning banner shown');
        const flags = doc.querySelector('.flag-banner').getAttribute('data-flags');
        check(flags.includes('storage_forbidden') && flags.includes('fabricated'), 'banner lists the gate and data state: ' + flags);
        check(view.textContent.includes('Evil <img src=x onerror=bad()>'), 'display name shown as text');
        check(view.textContent.includes('Desc <img'), 'description shown as text');
        check(view.textContent.includes('LIMIT-ONE'), 'limitations shown on the overview');
        check(view.querySelector('.limits') !== null, 'limitations block prominent');
        noInjected(doc, 'overview');
        for (const tab of ['schema', 'sample', 'lineage', 'quality', 'rights', 'access']) {
            await go(b, '#/dataset/evil/' + tab);
            const body = doc.getElementById('tab-body');
            check(body && body.textContent.trim().length > 0, tab + ' tab renders');
            check(!body.textContent.includes('Loading'), tab + ' tab loaded');
            noInjected(doc, tab);
            check(doc.querySelector('.flag-banner') !== null, tab + ': banner stays');
        }
        await go(b, '#/dataset/evil/schema');
        check(doc.querySelector('#tab-body .tag.pii') !== null && doc.querySelector('#tab-body .tag.id') !== null, 'schema PII + identifier tags');
        await go(b, '#/dataset/evil/lineage');
        check(doc.querySelector('#tab-body svg.lineage-svg') !== null, 'lineage graph drawn');
        await go(b, '#/dataset/evil/quality');
        check(doc.querySelector('#tab-body svg.spark polyline') !== null, 'row trend sparkline drawn');
        await go(b, '#/dataset/evil/access');
        check(doc.getElementById('curl-text').textContent.includes('X-API-Key'), 'curl example uses X-API-Key');
        doc.querySelector('[data-jsonld]').dispatchEvent(new w.MouseEvent('click', { bubbles: true }));
        await sleep(50);
        check(doc.getElementById('jsonld-out').textContent.includes('storage_forbidden'), 'JSON-LD refusal shown');
        check(b.calls.every(c => c[1] === 'GET'), 'only GET calls');
    },

    async refused() {
        const b = boot({ hash: '#/dataset/evil/sample', sampleRefused: true });
        const w = b.dom.window, doc = w.document;
        await w.CatalogPage.ready;
        await sleep(100);
        const n = doc.querySelector('#tab-body [data-refusal]');
        check(n !== null && n.textContent.includes('Sample refused: restricted dataset'), 'sample refusal reason shown');
        noInjected(doc, 'refused');
    },

    async search() {
        const b = boot({ hash: '#/?q=evil&kind=filings' });
        const w = b.dom.window, doc = w.document;
        await w.CatalogPage.ready;
        await sleep(100);
        check(b.calls.some(c => c[0] === '/api/v1/catalog/search?q=evil&kind=filings'), 'search query from the hash');
        check(doc.querySelectorAll('.ds-card').length === 2, 'two cards');
        check(doc.querySelector('.ds-card[data-key="evil"]').classList.contains('flagged'), 'flagged card marked');
        check(!doc.querySelector('.ds-card[data-key="clean"]').classList.contains('flagged'), 'clean card not flagged');
        check(doc.getElementById('q').value === 'evil', 'query box filled');
        check(doc.querySelector('input[data-facet="kind"][value="filings"]').checked, 'selected facet checked');
        check(doc.getElementById('results').textContent.includes('~12k rows'), 'row estimate shown');
        check(doc.getElementById('results').textContent.includes('2001-01-01 → 2026-06-30'), 'coverage range with status clock');
        noInjected(doc, 'search');
        // facet click updates the hash
        const box = doc.querySelector('input[data-facet="data_state"][value="ok"]');
        box.checked = true;
        box.dispatchEvent(new w.Event('change', { bubbles: true }));
        check(w.location.hash.includes('data_state=ok'), 'facet state kept in the hash: ' + w.location.hash);
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

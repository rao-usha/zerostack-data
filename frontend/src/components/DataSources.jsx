import { useState, useEffect, useCallback } from 'react'

const API_BASE = '/api/v1'

// Static source registry — all known data sources grouped by category
const SOURCE_REGISTRY = {
  gov: {
    label: 'Government & Economic',
    sources: [
      { key: 'census', label: 'Census ACS' },
      { key: 'census_business', label: 'Census Business' },
      { key: 'census_trade', label: 'Census Trade' },
      { key: 'census_housing', label: 'Census Housing' },
      { key: 'fred', label: 'FRED (Federal Reserve)' },
      { key: 'bls', label: 'BLS (Labor Statistics)' },
      { key: 'bea', label: 'BEA (Economic Analysis)' },
      { key: 'eia', label: 'EIA (Energy)' },
      { key: 'sec', label: 'SEC EDGAR' },
      { key: 'treasury', label: 'Treasury' },
      { key: 'noaa', label: 'NOAA (Weather/Climate)' },
      { key: 'epa', label: 'EPA (Environment)' },
      { key: 'epa_sdwis', label: 'EPA SDWIS (Drinking Water)' },
      { key: 'epa_envirofacts', label: 'EPA Envirofacts' },
      { key: 'usda', label: 'USDA (Agriculture)' },
      { key: 'hud', label: 'HUD (Housing)' },
      { key: 'dot', label: 'DOT (Transportation)' },
      { key: 'bts', label: 'BTS (Transport Stats)' },
      { key: 'osha', label: 'OSHA (Safety)' },
      { key: 'fema', label: 'FEMA (Emergency)' },
      { key: 'usgs', label: 'USGS (Geological Survey)' },
      { key: 'fcc', label: 'FCC (Communications)' },
      { key: 'fra', label: 'FRA (Rail Admin)' },
      { key: 'fmcsa', label: 'FMCSA (Motor Carrier)' },
      { key: 'fdic', label: 'FDIC (Banking)' },
      { key: 'sba', label: 'SBA (Small Business)' },
      { key: 'patent', label: 'USPTO Patents' },
      { key: 'trade', label: 'Trade.gov' },
      { key: 'sam', label: 'SAM.gov (Contracts)' },
      { key: 'data_gov', label: 'Data.gov' },
      { key: 'cbp', label: 'CBP (Customs)' },
      { key: 'irs', label: 'IRS Statistics' },
    ],
  },
  site_intel: {
    label: 'Site Intelligence',
    sources: [
      // Power
      { key: 'eia_power_plants', label: 'EIA Power Plants', domain: 'power' },
      { key: 'epa_emissions', label: 'EPA Emissions', domain: 'power' },
      { key: 'nrc_nuclear', label: 'NRC Nuclear', domain: 'power' },
      { key: 'ferc_hydro', label: 'FERC Hydroelectric', domain: 'power' },
      // Risk
      { key: 'fema_disasters', label: 'FEMA Disasters', domain: 'risk' },
      { key: 'usgs_earthquakes', label: 'USGS Earthquakes', domain: 'risk' },
      { key: 'noaa_storms', label: 'NOAA Storm Events', domain: 'risk' },
      { key: 'epa_superfund', label: 'EPA Superfund', domain: 'risk' },
      // Environment
      { key: 'epa_air_quality', label: 'EPA Air Quality', domain: 'environment' },
      { key: 'epa_water', label: 'EPA Water Quality', domain: 'environment' },
      { key: 'epa_tri', label: 'EPA Toxic Release', domain: 'environment' },
      { key: 'usgs_water', label: 'USGS Water Resources', domain: 'environment' },
      // Transport
      { key: 'bts_freight', label: 'BTS Freight', domain: 'transport' },
      { key: 'fra_rail', label: 'FRA Rail Crossings', domain: 'transport' },
      { key: 'fmcsa_carriers', label: 'FMCSA Carriers', domain: 'transport' },
      { key: 'dot_bridges', label: 'DOT Bridges', domain: 'transport' },
      { key: 'faa_airports', label: 'FAA Airports', domain: 'transport' },
      // Telecom
      { key: 'fcc_broadband', label: 'FCC Broadband', domain: 'telecom' },
      { key: 'fcc_towers', label: 'FCC Cell Towers', domain: 'telecom' },
      // Labor
      { key: 'bls_employment', label: 'BLS Employment', domain: 'labor' },
      { key: 'osha_inspections', label: 'OSHA Inspections', domain: 'labor' },
      // Regulatory
      { key: 'sec_filings', label: 'SEC Filings', domain: 'regulatory' },
      { key: 'osha_violations', label: 'OSHA Violations', domain: 'regulatory' },
      { key: 'epa_enforcement', label: 'EPA Enforcement', domain: 'regulatory' },
      // Demographics
      { key: 'census_population', label: 'Census Population', domain: 'demographics' },
      { key: 'census_income', label: 'Census Income', domain: 'demographics' },
      { key: 'hud_fair_market', label: 'HUD Fair Market Rents', domain: 'demographics' },
      { key: 'bea_gdp_metro', label: 'BEA Metro GDP', domain: 'demographics' },
      { key: 'usda_food_access', label: 'USDA Food Access', domain: 'demographics' },
      { key: 'fdic_banks', label: 'FDIC Bank Branches', domain: 'demographics' },
      { key: 'sba_loans', label: 'SBA Loan Data', domain: 'demographics' },
    ],
  },
  people: {
    label: 'People & Org Chart',
    sources: [
      { key: 'people_website', label: 'Website Scraping' },
      { key: 'people_sec_10k', label: 'SEC 10-K Officers' },
      { key: 'people_news', label: 'News Deep Scan' },
      { key: 'people_deep_crawl', label: 'Deep Crawl' },
      { key: 'people_org_chart', label: 'Org Chart Builder' },
    ],
  },
  pe: {
    label: 'PE Intelligence',
    sources: [
      { key: 'pe_13f', label: '13F Filings' },
      { key: 'pe_fund_performance', label: 'Fund Performance' },
      { key: 'pe_portfolio', label: 'Portfolio Tracking' },
      { key: 'pe_deal_flow', label: 'Deal Flow' },
      { key: 'pe_fundraising', label: 'Fundraising' },
      { key: 'pe_exits', label: 'Exits & Returns' },
      { key: 'pe_co_invest', label: 'Co-Investments' },
      { key: 'pe_benchmarks', label: 'Benchmarks' },
      { key: 'pe_news', label: 'PE News' },
      { key: 'pe_compliance', label: 'Compliance' },
    ],
  },
  fo: {
    label: 'Family Office & LP',
    sources: [
      { key: 'fo_profiles', label: 'Family Office Profiles' },
      { key: 'lp_commitments', label: 'LP Commitments' },
    ],
  },
  integrations: {
    label: 'Supporting Integrations',
    sources: [
      { key: 'openai', label: 'OpenAI GPT' },
      { key: 'anthropic', label: 'Anthropic Claude' },
      { key: 'duckduckgo', label: 'DuckDuckGo Search' },
      { key: 'yelp', label: 'Yelp Business' },
      { key: 'kaggle', label: 'Kaggle Datasets' },
      { key: 'graphql', label: 'GraphQL Layer' },
      { key: 'webhooks', label: 'Webhooks' },
      { key: 'search_index', label: 'Search Index' },
      { key: 'llm_cost_tracker', label: 'LLM Cost Tracker' },
      { key: 'data_quality', label: 'Data Quality' },
      { key: 'audit_trail', label: 'Audit Trail' },
      { key: 'scheduler', label: 'Job Scheduler' },
    ],
  },
}

const TOTAL_SOURCES = Object.values(SOURCE_REGISTRY).reduce(
  (sum, cat) => sum + cat.sources.length, 0
)

function timeAgo(dateStr) {
  if (!dateStr) return 'Never'
  const now = new Date()
  const date = new Date(dateStr)
  const diffMs = now - date
  if (diffMs < 0) return 'Just now'
  const mins = Math.floor(diffMs / 60000)
  if (mins < 1) return 'Just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  if (days < 30) return `${days}d ago`
  return `${Math.floor(days / 30)}mo ago`
}

function formatNumber(n) {
  if (n == null) return '-'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

export default function DataSources() {
  // Phase 1 data (loaded on mount)
  const [loading, setLoading] = useState(true)
  const [dashboard, setDashboard] = useState(null)
  const [siteIntel, setSiteIntel] = useState(null)
  const [schedules, setSchedules] = useState(null)

  // Phase 2 data (lazy loaded)
  const [detailLoaded, setDetailLoaded] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [watermarks, setWatermarks] = useState(null)
  const [auditSummary, setAuditSummary] = useState(null)
  const [sourceConfigs, setSourceConfigs] = useState(null)
  const [activity, setActivity] = useState(null)

  // UI state
  const [expanded, setExpanded] = useState({})
  const [filter, setFilter] = useState('')

  // Phase 1: load core data on mount
  const loadPhase1 = useCallback(async () => {
    setLoading(true)
    try {
      const [dashRes, siteRes, schedRes] = await Promise.all([
        fetch(`${API_BASE}/jobs/monitoring/dashboard`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/site-intel/sites/collect/status`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/schedules`).then(r => r.ok ? r.json() : null).catch(() => null),
      ])
      setDashboard(dashRes)
      setSiteIntel(siteRes)
      setSchedules(schedRes)
    } catch {
      // Errors already handled per-fetch
    }
    setLoading(false)
  }, [])

  // Phase 2: load detail data (lazy)
  const loadPhase2 = useCallback(async () => {
    if (detailLoaded || detailLoading) return
    setDetailLoading(true)
    try {
      const [auditRes, wmRes, cfgRes, actRes] = await Promise.all([
        fetch(`${API_BASE}/audit-trail/summary`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/site-intel/sites/watermarks`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/source-configs`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/audit-trail?limit=15`).then(r => r.ok ? r.json() : null).catch(() => null),
      ])
      setAuditSummary(auditRes)
      setWatermarks(wmRes)
      setSourceConfigs(cfgRes)
      setActivity(actRes)
      setDetailLoaded(true)
    } catch {
      // handled per-fetch
    }
    setDetailLoading(false)
  }, [detailLoaded, detailLoading])

  useEffect(() => { loadPhase1() }, [loadPhase1])

  const handleRefresh = () => {
    setDetailLoaded(false)
    loadPhase1()
  }

  const toggleCategory = (catKey) => {
    const willExpand = !expanded[catKey]
    setExpanded(prev => ({ ...prev, [catKey]: willExpand }))
    if (willExpand && !detailLoaded) {
      loadPhase2()
    }
  }

  // Build lookup maps from API data
  const sourceHealthMap = dashboard?.source_health?.sources || {}
  const scheduleMap = {}
  if (Array.isArray(schedules)) {
    for (const s of schedules) {
      scheduleMap[s.source] = s
    }
  }
  const latestJobsMap = siteIntel?.latest_jobs || {}
  const watermarkMap = {}
  if (watermarks?.watermarks) {
    for (const w of watermarks.watermarks) {
      watermarkMap[w.source] = w
    }
  }
  const configMap = {}
  if (sourceConfigs?.configs) {
    for (const c of sourceConfigs.configs) {
      configMap[c.source] = c
    }
  }
  const auditBySource = auditSummary?.last_24h?.by_source || {}

  // Compute hero stats
  const totalRecords = dashboard?.metrics_24h?.total_rows_inserted || 0
  const activeJobs = (dashboard?.metrics_1h?.status_breakdown?.running || 0)
    + (dashboard?.metrics_1h?.status_breakdown?.pending || 0)
  const healthSources = Object.values(sourceHealthMap)
  const avgHealth = healthSources.length > 0
    ? Math.round(healthSources.reduce((s, h) => s + (h.health_score || 0), 0) / healthSources.length)
    : null
  const alerts = dashboard?.alerts || []

  // Get status for a source
  const getSourceStatus = (sourceKey) => {
    // Check source_health first
    const health = sourceHealthMap[sourceKey]
    if (health) {
      if (health.status === 'healthy') return 'ok'
      if (health.status === 'degraded' || health.status === 'warning') return 'stale'
      if (health.status === 'critical') return 'error'
      return 'idle'
    }
    // Check site intel latest jobs
    const job = latestJobsMap[sourceKey]
    if (job) {
      if (job.status === 'completed' && job.rows_collected > 0) return 'ok'
      if (job.status === 'failed') return 'error'
      if (job.status === 'running') return 'ok'
      return 'idle'
    }
    return 'idle'
  }

  // Get last run time for a source
  const getLastRun = (sourceKey) => {
    const health = sourceHealthMap[sourceKey]
    if (health?.last_success_at) return timeAgo(health.last_success_at)
    const job = latestJobsMap[sourceKey]
    if (job?.completed_at) return timeAgo(job.completed_at)
    const wm = watermarkMap[sourceKey]
    if (wm?.last_collected_at) return timeAgo(wm.last_collected_at)
    const sched = scheduleMap[sourceKey]
    if (sched?.last_run_at) return timeAgo(sched.last_run_at)
    return 'Never'
  }

  // Get record count for a source
  const getRecords = (sourceKey) => {
    const job = latestJobsMap[sourceKey]
    if (job?.rows_collected != null) return formatNumber(job.rows_collected)
    const health = sourceHealthMap[sourceKey]
    if (health?.success_24h > 0) return `${health.success_24h} jobs`
    return '-'
  }

  // Get frequency from schedule
  const getFrequency = (sourceKey) => {
    const sched = scheduleMap[sourceKey]
    if (!sched) return 'Manual'
    if (!sched.is_active) return 'Paused'
    return sched.frequency ? sched.frequency.charAt(0).toUpperCase() + sched.frequency.slice(1) : 'Manual'
  }

  // Filter sources by search text
  const filterSources = (sources) => {
    if (!filter.trim()) return sources
    const q = filter.toLowerCase()
    return sources.filter(s =>
      s.label.toLowerCase().includes(q) || s.key.toLowerCase().includes(q)
      || (s.domain && s.domain.toLowerCase().includes(q))
    )
  }

  // Count active sources in a category
  const countActive = (sources) => {
    return sources.filter(s => {
      const status = getSourceStatus(s.key)
      return status === 'ok' || status === 'stale'
    }).length
  }

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        Loading data sources...
      </div>
    )
  }

  return (
    <div className="ds-container">
      {/* Hero Stats */}
      <div className="ds-hero">
        <div className="ds-hero-card">
          <div className="ds-hero-value">{TOTAL_SOURCES}</div>
          <div className="ds-hero-label">Data Sources</div>
        </div>
        <div className="ds-hero-card">
          <div className="ds-hero-value">{formatNumber(totalRecords)}</div>
          <div className="ds-hero-label">Records (24h)</div>
        </div>
        <div className="ds-hero-card">
          <div className="ds-hero-value">{activeJobs}</div>
          <div className="ds-hero-label">Active Jobs</div>
        </div>
        <div className="ds-hero-card">
          <div className="ds-hero-value" style={avgHealth != null ? {
            color: avgHealth >= 80 ? 'var(--success)' : avgHealth >= 50 ? 'var(--warning)' : 'var(--error)'
          } : undefined}>
            {avgHealth != null ? `${avgHealth}/100` : '--'}
          </div>
          <div className="ds-hero-label">Health Score</div>
        </div>
      </div>

      {/* Alerts */}
      {alerts.length > 0 && (
        <div className="ds-alerts">
          {alerts.map((alert, i) => (
            <div key={i} className="ds-alert-item">
              <span className={`ds-alert-severity ${alert.severity}`}>
                {alert.severity === 'critical' ? '!!' : alert.severity === 'warning' ? '!' : 'i'}
              </span>
              <span>{alert.message}</span>
            </div>
          ))}
        </div>
      )}

      {/* Toolbar */}
      <div className="ds-toolbar">
        <input
          className="search-input"
          type="text"
          placeholder="Filter sources..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
        />
        <button className="btn btn-secondary" onClick={handleRefresh}>
          Refresh
        </button>
      </div>

      {/* Category Accordions */}
      {Object.entries(SOURCE_REGISTRY).map(([catKey, category]) => {
        const filtered = filterSources(category.sources)
        if (filter && filtered.length === 0) return null
        const isExpanded = expanded[catKey]
        const active = countActive(category.sources)

        return (
          <div key={catKey} className="ds-category">
            <div className="ds-category-header" onClick={() => toggleCategory(catKey)}>
              <div className="ds-category-left">
                <span className="ds-chevron">{isExpanded ? '\u25BC' : '\u25B6'}</span>
                <span className="ds-category-title">{category.label}</span>
                <span className="ds-category-count">({filtered.length} sources)</span>
              </div>
              <div className="ds-category-right">
                {active > 0 && <span className="ds-badge-active">{active} active</span>}
              </div>
            </div>
            {isExpanded && (
              <div className="ds-category-body">
                {catKey === 'site_intel' ? (
                  // Group site intel by domain
                  <SiteIntelTable
                    sources={filtered}
                    getSourceStatus={getSourceStatus}
                    getLastRun={getLastRun}
                    getRecords={getRecords}
                    getFrequency={getFrequency}
                  />
                ) : (
                  <SourceTable
                    sources={filtered}
                    getSourceStatus={getSourceStatus}
                    getLastRun={getLastRun}
                    getRecords={getRecords}
                    getFrequency={getFrequency}
                  />
                )}
                {detailLoading && !detailLoaded && (
                  <div className="loading" style={{ padding: '0.5rem' }}>
                    <div className="spinner" style={{ width: 16, height: 16 }} />
                    Loading details...
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}

      {/* Recent Activity */}
      <ActivityFeed activity={activity} onLoad={loadPhase2} detailLoaded={detailLoaded} />
    </div>
  )
}

function SourceTable({ sources, getSourceStatus, getLastRun, getRecords, getFrequency }) {
  if (sources.length === 0) {
    return <div className="ds-empty">No matching sources</div>
  }
  return (
    <table className="ds-table">
      <thead>
        <tr>
          <th>Source</th>
          <th>Status</th>
          <th>Frequency</th>
          <th>Records</th>
          <th>Last Run</th>
        </tr>
      </thead>
      <tbody>
        {sources.map(source => {
          const status = getSourceStatus(source.key)
          return (
            <tr key={source.key}>
              <td>
                <span className={`ds-status ${status}`} />
                {source.label}
              </td>
              <td><span className={`ds-status-text ${status}`}>{status}</span></td>
              <td>{getFrequency(source.key)}</td>
              <td>{getRecords(source.key)}</td>
              <td>{getLastRun(source.key)}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function SiteIntelTable({ sources, getSourceStatus, getLastRun, getRecords, getFrequency }) {
  // Group by domain
  const grouped = {}
  for (const s of sources) {
    const d = s.domain || 'other'
    if (!grouped[d]) grouped[d] = []
    grouped[d].push(s)
  }

  return (
    <div className="ds-site-intel-groups">
      {Object.entries(grouped).map(([domain, domainSources]) => (
        <div key={domain} className="ds-domain-group">
          <div className="ds-domain-header">
            {domain.charAt(0).toUpperCase() + domain.slice(1)}
            <span className="ds-domain-count">{domainSources.length}</span>
          </div>
          <table className="ds-table">
            <thead>
              <tr>
                <th>Collector</th>
                <th>Status</th>
                <th>Frequency</th>
                <th>Records</th>
                <th>Last Run</th>
              </tr>
            </thead>
            <tbody>
              {domainSources.map(source => {
                const status = getSourceStatus(source.key)
                return (
                  <tr key={source.key}>
                    <td>
                      <span className={`ds-status ${status}`} />
                      {source.label}
                    </td>
                    <td><span className={`ds-status-text ${status}`}>{status}</span></td>
                    <td>{getFrequency(source.key)}</td>
                    <td>{getRecords(source.key)}</td>
                    <td>{getLastRun(source.key)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}

function ActivityFeed({ activity, onLoad, detailLoaded }) {
  useEffect(() => {
    if (!detailLoaded) onLoad()
  }, [detailLoaded, onLoad])

  const entries = activity?.entries || []

  return (
    <div className="ds-activity">
      <div className="ds-activity-header">Recent Activity</div>
      {entries.length === 0 ? (
        <div className="ds-empty">No recent activity</div>
      ) : (
        <div className="ds-activity-list">
          {entries.slice(0, 10).map((entry, i) => (
            <div key={entry.id || i} className="ds-activity-item">
              <span className="ds-activity-time">
                {timeAgo(entry.created_at)}
              </span>
              <span className="ds-activity-desc">
                <span className="ds-activity-trigger">{entry.trigger_type}</span>
                {' triggered '}
                <strong>{entry.source}</strong>
                {' collection'}
                {entry.job_id && <span className="ds-activity-job"> (job #{entry.job_id})</span>}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

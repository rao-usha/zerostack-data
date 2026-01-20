import { useState, useEffect } from 'react'

const API_BASE = '/api/v1'

export default function MarketIntel() {
  const [brief, setBrief] = useState(null)
  const [signals, setSignals] = useState([])
  const [trends, setTrends] = useState([])
  const [opportunities, setOpportunities] = useState([])
  const [loading, setLoading] = useState(true)
  const [scanning, setScanning] = useState(false)

  useEffect(() => {
    fetchData()
  }, [])

  const fetchData = async () => {
    setLoading(true)
    try {
      const [briefResp, signalsResp, trendsResp, oppsResp] = await Promise.all([
        fetch(`${API_BASE}/market/brief?period_type=weekly`),
        fetch(`${API_BASE}/market/scan`),
        fetch(`${API_BASE}/market/trends?period=30`),
        fetch(`${API_BASE}/market/opportunities`)
      ])

      if (briefResp.ok) setBrief(await briefResp.json())
      if (signalsResp.ok) {
        const data = await signalsResp.json()
        setSignals(data.signals || [])
      }
      if (trendsResp.ok) setTrends((await trendsResp.json()).trends || [])
      if (oppsResp.ok) setOpportunities((await oppsResp.json()).opportunities || [])
    } catch (err) {
      console.error('Failed to fetch market data:', err)
    } finally {
      setLoading(false)
    }
  }

  const triggerScan = async () => {
    setScanning(true)
    try {
      const resp = await fetch(`${API_BASE}/market/scan/trigger`, { method: 'POST' })
      if (resp.ok) {
        const data = await resp.json()
        setSignals(data.signals || [])
        fetchData() // Refresh all data
      }
    } catch (err) {
      console.error('Scan failed:', err)
    } finally {
      setScanning(false)
    }
  }

  const getStrengthClass = (strength) => {
    if (strength >= 0.7) return 'high'
    if (strength >= 0.4) return 'medium'
    return 'low'
  }

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        Loading market intelligence...
      </div>
    )
  }

  return (
    <div>
      {/* Weekly Brief */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">📊 Weekly Market Brief</h2>
          <button className="btn btn-secondary" onClick={triggerScan} disabled={scanning}>
            {scanning ? 'Scanning...' : '🔄 Refresh Scan'}
          </button>
        </div>

        {brief && (
          <>
            <p style={{ marginBottom: '1rem' }}>{brief.summary}</p>

            {brief.stats && (
              <div className="network-stats">
                <div className="stat-card">
                  <div className="stat-value">{brief.stats.total_signals}</div>
                  <div className="stat-label">Total Signals</div>
                </div>
                <div className="stat-card">
                  <div className="stat-value" style={{ color: 'var(--success)' }}>
                    {brief.stats.accelerating}
                  </div>
                  <div className="stat-label">Accelerating</div>
                </div>
                <div className="stat-card">
                  <div className="stat-value" style={{ color: 'var(--error)' }}>
                    {brief.stats.decelerating}
                  </div>
                  <div className="stat-label">Decelerating</div>
                </div>
              </div>
            )}

            {brief.sections?.sector_spotlight?.sector !== 'none' && (
              <div style={{ marginTop: '1rem', padding: '1rem', background: 'var(--bg-input)', borderRadius: '8px' }}>
                <h4>🎯 Sector Spotlight: {brief.sections.sector_spotlight.sector}</h4>
                <p style={{ color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                  {brief.sections.sector_spotlight.signal_count} signals •
                  Avg strength: {(brief.sections.sector_spotlight.avg_strength * 100).toFixed(0)}%
                </p>
              </div>
            )}
          </>
        )}
      </div>

      <div className="grid-2">
        {/* Signals */}
        <div className="card">
          <h3 className="card-title">📡 Active Signals</h3>
          {signals.length === 0 ? (
            <div className="empty">
              <div className="empty-icon">📡</div>
              <p>No signals detected</p>
              <p style={{ fontSize: '0.875rem' }}>Run a market scan to detect signals</p>
            </div>
          ) : (
            <div className="signal-list">
              {signals.slice(0, 5).map(signal => (
                <div key={signal.signal_id} className="signal-item">
                  <div className={`signal-strength ${getStrengthClass(signal.strength)}`}>
                    {(signal.strength * 100).toFixed(0)}%
                  </div>
                  <div className="signal-content">
                    <div className="signal-type">{signal.signal_type?.replace('_', ' ')}</div>
                    <div className="signal-description">{signal.description}</div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Trends */}
        <div className="card">
          <h3 className="card-title">📈 Emerging Trends</h3>
          {trends.length === 0 ? (
            <div className="empty">
              <div className="empty-icon">📈</div>
              <p>No trends detected</p>
            </div>
          ) : (
            <div className="results">
              {trends.slice(0, 5).map(trend => (
                <div key={trend.trend_id} className="result-item">
                  <div>
                    <div className="result-name">{trend.name}</div>
                    <div className="result-meta">
                      Stage: {trend.stage} • Signals: {trend.signal_count}
                    </div>
                  </div>
                  <span className={`score-badge ${trend.momentum > 0.5 ? 'tier-a' : 'tier-c'}`}>
                    {(trend.momentum * 100).toFixed(0)}%
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Opportunities */}
      <div className="card">
        <h3 className="card-title">💡 Investment Opportunities</h3>
        {opportunities.length === 0 ? (
          <div className="empty">
            <div className="empty-icon">💡</div>
            <p>No opportunities identified yet</p>
            <p style={{ fontSize: '0.875rem' }}>Opportunities are generated from market signals</p>
          </div>
        ) : (
          <div className="results">
            {opportunities.map(opp => (
              <div key={opp.opportunity_id} className="result-item" style={{ flexDirection: 'column', alignItems: 'stretch' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                  <div className="result-name">{opp.title}</div>
                  <span className={`score-badge ${opp.confidence > 0.6 ? 'tier-b' : 'tier-c'}`}>
                    {(opp.confidence * 100).toFixed(0)}% confidence
                  </span>
                </div>
                <p style={{ color: 'var(--text-muted)', marginBottom: '0.5rem' }}>{opp.thesis}</p>
                <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                  {opp.recommended_actions?.slice(0, 2).map((action, i) => (
                    <span key={i} className="stat">{action}</span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

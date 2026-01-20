import { useState } from 'react'

const API_BASE = '/api/v1'

export default function CompanyResearch() {
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [score, setScore] = useState(null)
  const [jobId, setJobId] = useState(null)

  const handleSearch = async () => {
    if (!query.trim()) return

    setLoading(true)
    setResult(null)
    setScore(null)
    setJobId(null)

    try {
      // Start research job
      const researchResp = await fetch(`${API_BASE}/agents/research/company`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_name: query })
      })
      const researchData = await researchResp.json()

      if (researchData.job_id) {
        setJobId(researchData.job_id)

        // Poll for results
        let attempts = 0
        while (attempts < 10) {
          await new Promise(r => setTimeout(r, 2000))
          const statusResp = await fetch(`${API_BASE}/agents/research/${researchData.job_id}`)
          const statusData = await statusResp.json()

          if (statusData.status === 'completed' || statusData.status === 'partial') {
            setResult(statusData)
            break
          } else if (statusData.status === 'failed') {
            setResult({ error: 'Research failed' })
            break
          }
          attempts++
        }
      }

      // Also get company score
      const scoreResp = await fetch(`${API_BASE}/scores/company/${encodeURIComponent(query)}`)
      if (scoreResp.ok) {
        const scoreData = await scoreResp.json()
        setScore(scoreData)
      }
    } catch (err) {
      console.error('Search error:', err)
      setResult({ error: err.message })
    } finally {
      setLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') handleSearch()
  }

  return (
    <div>
      <div className="card">
        <h2 className="card-title">🔬 Company Research</h2>
        <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
          AI-powered research across 9+ data sources
        </p>

        <div className="search-box">
          <input
            type="text"
            className="search-input"
            placeholder="Enter company name (e.g., Stripe, OpenAI, Anthropic)"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button className="btn" onClick={handleSearch} disabled={loading}>
            {loading ? 'Researching...' : 'Research'}
          </button>
        </div>

        {loading && (
          <div className="loading">
            <div className="spinner" />
            Querying data sources...
          </div>
        )}
      </div>

      {score && (
        <div className="card">
          <div className="card-header">
            <h3 className="card-title">📈 Company Score</h3>
            <span className={`score-badge tier-${score.tier?.toLowerCase() || 'c'}`}>
              {score.composite_score?.toFixed(0) || 'N/A'} ({score.tier || 'N/A'})
            </span>
          </div>

          <div className="grid-2">
            {score.category_scores && Object.entries(score.category_scores).map(([key, value]) => (
              <div key={key} className="stat-card">
                <div className="stat-value">{value}</div>
                <div className="stat-label">{key}</div>
              </div>
            ))}
          </div>

          {score.confidence && (
            <p style={{ color: 'var(--text-muted)', marginTop: '1rem', fontSize: '0.875rem' }}>
              Confidence: {(score.confidence * 100).toFixed(0)}%
            </p>
          )}
        </div>
      )}

      {result && !result.error && (
        <div className="card">
          <h3 className="card-title">📋 Research Results</h3>

          {result.profile && (
            <div style={{ marginTop: '1rem' }}>
              <h4 style={{ marginBottom: '0.5rem' }}>{result.profile.name || query}</h4>
              {result.profile.description && (
                <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
                  {result.profile.description}
                </p>
              )}

              <div className="grid-2">
                {result.profile.domain && (
                  <div className="result-item">
                    <span className="result-name">Domain</span>
                    <span className="result-meta">{result.profile.domain}</span>
                  </div>
                )}
                {result.profile.industry && (
                  <div className="result-item">
                    <span className="result-name">Industry</span>
                    <span className="result-meta">{result.profile.industry}</span>
                  </div>
                )}
                {result.profile.employee_count && (
                  <div className="result-item">
                    <span className="result-name">Employees</span>
                    <span className="result-meta">{result.profile.employee_count.toLocaleString()}</span>
                  </div>
                )}
                {result.profile.founded && (
                  <div className="result-item">
                    <span className="result-name">Founded</span>
                    <span className="result-meta">{result.profile.founded}</span>
                  </div>
                )}
              </div>
            </div>
          )}

          {result.sources_completed && (
            <div style={{ marginTop: '1.5rem' }}>
              <h4 style={{ marginBottom: '0.75rem' }}>Data Sources</h4>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
                {result.sources_completed.map(source => (
                  <span key={source} className="stat" style={{ background: 'var(--success)', color: 'white' }}>
                    ✓ {source}
                  </span>
                ))}
                {result.sources_failed?.map(source => (
                  <span key={source} className="stat" style={{ background: 'var(--error)', color: 'white' }}>
                    ✗ {source}
                  </span>
                ))}
              </div>
            </div>
          )}

          {result.confidence && (
            <p style={{ color: 'var(--text-muted)', marginTop: '1rem', fontSize: '0.875rem' }}>
              Overall Confidence: {(result.confidence * 100).toFixed(0)}%
            </p>
          )}
        </div>
      )}

      {result?.error && (
        <div className="card" style={{ borderColor: 'var(--error)' }}>
          <p style={{ color: 'var(--error)' }}>Error: {result.error}</p>
        </div>
      )}

      {!loading && !result && (
        <div className="empty">
          <div className="empty-icon">🔍</div>
          <p>Enter a company name to start research</p>
          <p style={{ fontSize: '0.875rem', marginTop: '0.5rem' }}>
            Try: Stripe, OpenAI, Anthropic, Databricks, Figma
          </p>
        </div>
      )}
    </div>
  )
}

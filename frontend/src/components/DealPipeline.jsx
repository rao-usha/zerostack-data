import { useState, useEffect } from 'react'

const API_BASE = '/api/v1'

const STAGES = ['sourced', 'reviewing', 'due_diligence', 'negotiation', 'closed_won', 'closed_lost']

export default function DealPipeline() {
  const [deals, setDeals] = useState([])
  const [pipeline, setPipeline] = useState(null)
  const [predictions, setPredictions] = useState(null)
  const [loading, setLoading] = useState(true)
  const [showAddForm, setShowAddForm] = useState(false)
  const [newDeal, setNewDeal] = useState({
    company_name: '',
    stage: 'sourced',
    sector: 'fintech',
    priority: 2
  })

  useEffect(() => {
    fetchData()
  }, [])

  const fetchData = async () => {
    setLoading(true)
    try {
      const [dealsResp, pipelineResp, predictionsResp] = await Promise.all([
        fetch(`${API_BASE}/deals?limit=50`),
        fetch(`${API_BASE}/deals/pipeline`),
        fetch(`${API_BASE}/predictions/pipeline`)
      ])

      if (dealsResp.ok) setDeals((await dealsResp.json()).deals || [])
      if (pipelineResp.ok) setPipeline(await pipelineResp.json())
      if (predictionsResp.ok) setPredictions(await predictionsResp.json())
    } catch (err) {
      console.error('Failed to fetch deals:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleAddDeal = async () => {
    try {
      const resp = await fetch(`${API_BASE}/deals`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newDeal)
      })
      if (resp.ok) {
        setShowAddForm(false)
        setNewDeal({ company_name: '', stage: 'sourced', sector: 'fintech', priority: 2 })
        fetchData()
      }
    } catch (err) {
      console.error('Failed to add deal:', err)
    }
  }

  const getDealPrediction = (dealId) => {
    if (!predictions?.deals) return null
    return predictions.deals.find(p => p.deal_id === dealId)
  }

  const groupedDeals = STAGES.reduce((acc, stage) => {
    acc[stage] = deals.filter(d => d.stage === stage)
    return acc
  }, {})

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        Loading pipeline...
      </div>
    )
  }

  return (
    <div>
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">💼 Deal Pipeline</h2>
          <button className="btn" onClick={() => setShowAddForm(!showAddForm)}>
            + Add Deal
          </button>
        </div>

        {pipeline && (
          <div className="network-stats">
            <div className="stat-card">
              <div className="stat-value">{pipeline.total_deals || 0}</div>
              <div className="stat-label">Total Deals</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">{pipeline.active_deals || 0}</div>
              <div className="stat-label">Active</div>
            </div>
            {predictions?.summary && (
              <>
                <div className="stat-card">
                  <div className="stat-value">{(predictions.summary.avg_probability * 100).toFixed(0)}%</div>
                  <div className="stat-label">Avg Win Prob</div>
                </div>
                <div className="stat-card">
                  <div className="stat-value">{predictions.summary.expected_wins?.toFixed(1) || 0}</div>
                  <div className="stat-label">Expected Wins</div>
                </div>
              </>
            )}
          </div>
        )}

        {showAddForm && (
          <div style={{ background: 'var(--bg-input)', padding: '1rem', borderRadius: '8px', marginBottom: '1rem' }}>
            <h4 style={{ marginBottom: '1rem' }}>Add New Deal</h4>
            <div style={{ display: 'grid', gap: '0.75rem' }}>
              <input
                type="text"
                className="search-input"
                placeholder="Company name"
                value={newDeal.company_name}
                onChange={e => setNewDeal({ ...newDeal, company_name: e.target.value })}
              />
              <div style={{ display: 'flex', gap: '0.75rem' }}>
                <select
                  className="search-input"
                  value={newDeal.stage}
                  onChange={e => setNewDeal({ ...newDeal, stage: e.target.value })}
                >
                  {STAGES.map(s => (
                    <option key={s} value={s}>{s.replace('_', ' ')}</option>
                  ))}
                </select>
                <select
                  className="search-input"
                  value={newDeal.sector}
                  onChange={e => setNewDeal({ ...newDeal, sector: e.target.value })}
                >
                  {['fintech', 'healthcare', 'ai_ml', 'enterprise', 'consumer', 'climate'].map(s => (
                    <option key={s} value={s}>{s.replace('_', '/')}</option>
                  ))}
                </select>
                <select
                  className="search-input"
                  value={newDeal.priority}
                  onChange={e => setNewDeal({ ...newDeal, priority: parseInt(e.target.value) })}
                >
                  <option value={1}>High Priority</option>
                  <option value={2}>Medium Priority</option>
                  <option value={3}>Low Priority</option>
                </select>
              </div>
              <button className="btn" onClick={handleAddDeal}>Add Deal</button>
            </div>
          </div>
        )}
      </div>

      <div className="pipeline">
        {STAGES.slice(0, 4).map(stage => (
          <div key={stage} className="pipeline-stage">
            <div className="pipeline-stage-header">
              <span className="pipeline-stage-title">{stage.replace('_', ' ')}</span>
              <span className="pipeline-count">{groupedDeals[stage].length}</span>
            </div>
            {groupedDeals[stage].length === 0 ? (
              <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', textAlign: 'center', padding: '1rem 0' }}>
                No deals
              </p>
            ) : (
              groupedDeals[stage].map(deal => {
                const prediction = getDealPrediction(deal.id)
                return (
                  <div key={deal.id} className="deal-card">
                    <div className="deal-name">{deal.company_name}</div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <span className="deal-sector">{deal.sector}</span>
                      {prediction && (
                        <span className={`score-badge tier-${prediction.tier?.toLowerCase() || 'c'}`} style={{ fontSize: '0.7rem' }}>
                          {(prediction.win_probability * 100).toFixed(0)}%
                        </span>
                      )}
                    </div>
                  </div>
                )
              })
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

import { useState, useEffect } from 'react'

const API_BASE = '/api/v1'

export default function NetworkGraph() {
  const [centralInvestors, setCentralInvestors] = useState([])
  const [selectedInvestor, setSelectedInvestor] = useState(null)
  const [investorNetwork, setInvestorNetwork] = useState(null)
  const [loading, setLoading] = useState(true)
  const [networkLoading, setNetworkLoading] = useState(false)

  useEffect(() => {
    fetchCentralInvestors()
  }, [])

  const fetchCentralInvestors = async () => {
    setLoading(true)
    try {
      const resp = await fetch(`${API_BASE}/network/central?limit=20`)
      if (resp.ok) {
        const data = await resp.json()
        setCentralInvestors(data || [])
      }
    } catch (err) {
      console.error('Failed to fetch central investors:', err)
    } finally {
      setLoading(false)
    }
  }

  const fetchInvestorNetwork = async (investor) => {
    setNetworkLoading(true)
    setSelectedInvestor(investor)
    try {
      const resp = await fetch(`${API_BASE}/network/investor/${investor.investor_id}?investor_type=${investor.type}`)
      if (resp.ok) {
        const data = await resp.json()
        setInvestorNetwork(data)
      }
    } catch (err) {
      console.error('Failed to fetch investor network:', err)
    } finally {
      setNetworkLoading(false)
    }
  }

  const getTypeColor = (type) => {
    switch (type) {
      case 'lp': return 'var(--primary)'
      case 'family_office': return 'var(--accent)'
      case 'sovereign_wealth': return 'var(--warning)'
      default: return 'var(--text-muted)'
    }
  }

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        Loading network data...
      </div>
    )
  }

  return (
    <div>
      <div className="grid-2">
        {/* Central Investors */}
        <div className="card">
          <h2 className="card-title">🕸️ Most Connected Investors</h2>
          <p style={{ color: 'var(--text-muted)', marginBottom: '1rem' }}>
            Ranked by co-investment network centrality
          </p>

          {centralInvestors.length === 0 ? (
            <div className="empty">
              <div className="empty-icon">🕸️</div>
              <p>No network data available</p>
            </div>
          ) : (
            <div className="investor-list">
              {centralInvestors.map((investor, idx) => (
                <div
                  key={investor.id}
                  className="investor-item"
                  style={{ cursor: 'pointer' }}
                  onClick={() => fetchInvestorNetwork(investor)}
                >
                  <div>
                    <span style={{ color: 'var(--text-muted)', marginRight: '0.5rem' }}>#{idx + 1}</span>
                    <span className="investor-name">{investor.name}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                      {investor.degree} connections
                    </span>
                    <span
                      className="investor-type"
                      style={{ background: getTypeColor(investor.type), color: 'white' }}
                    >
                      {investor.type?.replace('_', ' ')}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Investor Network Detail */}
        <div className="card">
          <h2 className="card-title">
            {selectedInvestor ? `🔗 ${selectedInvestor.name}'s Network` : '🔗 Investor Network'}
          </h2>

          {networkLoading ? (
            <div className="loading">
              <div className="spinner" />
              Loading network...
            </div>
          ) : !selectedInvestor ? (
            <div className="empty">
              <div className="empty-icon">👈</div>
              <p>Select an investor to view their network</p>
            </div>
          ) : investorNetwork ? (
            <>
              {/* Stats */}
              <div className="network-stats" style={{ marginBottom: '1rem' }}>
                <div className="stat-card">
                  <div className="stat-value">{investorNetwork.stats?.total_nodes || 0}</div>
                  <div className="stat-label">Nodes</div>
                </div>
                <div className="stat-card">
                  <div className="stat-value">{investorNetwork.stats?.total_edges || 0}</div>
                  <div className="stat-label">Connections</div>
                </div>
                <div className="stat-card">
                  <div className="stat-value">{investorNetwork.stats?.direct_connections || 0}</div>
                  <div className="stat-label">Direct</div>
                </div>
              </div>

              {/* Connected Investors */}
              <h4 style={{ marginBottom: '0.75rem' }}>Connected Investors</h4>
              <div className="investor-list" style={{ maxHeight: '300px', overflowY: 'auto' }}>
                {investorNetwork.nodes?.filter(n => n.id !== selectedInvestor.id).map(node => (
                  <div key={node.id} className="investor-item">
                    <div>
                      <span className="investor-name">{node.name}</span>
                      <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem', marginLeft: '0.5rem' }}>
                        {node.location}
                      </span>
                    </div>
                    <span
                      className="investor-type"
                      style={{ background: getTypeColor(node.type), color: 'white' }}
                    >
                      {node.subtype || node.type?.replace('_', ' ')}
                    </span>
                  </div>
                ))}
              </div>

              {/* Shared Investments */}
              {investorNetwork.edges?.length > 0 && (
                <>
                  <h4 style={{ marginTop: '1rem', marginBottom: '0.75rem' }}>Shared Investments</h4>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
                    {[...new Set(investorNetwork.edges.flatMap(e => e.shared_companies || []))].slice(0, 10).map(company => (
                      <span key={company} className="stat">{company}</span>
                    ))}
                  </div>
                </>
              )}
            </>
          ) : (
            <div className="empty">
              <div className="empty-icon">⚠️</div>
              <p>Failed to load network data</p>
            </div>
          )}
        </div>
      </div>

      {/* Network Overview */}
      <div className="card">
        <h3 className="card-title">📊 Network Statistics</h3>
        <div className="network-stats">
          <div className="stat-card">
            <div className="stat-value">{centralInvestors.length}</div>
            <div className="stat-label">Investors Mapped</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">
              {centralInvestors.filter(i => i.type === 'lp').length}
            </div>
            <div className="stat-label">LPs</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">
              {centralInvestors.filter(i => i.type === 'family_office').length}
            </div>
            <div className="stat-label">Family Offices</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">
              {centralInvestors.reduce((sum, i) => sum + (i.degree || 0), 0)}
            </div>
            <div className="stat-label">Total Connections</div>
          </div>
        </div>
      </div>
    </div>
  )
}

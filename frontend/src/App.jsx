import { useState, useEffect } from 'react'
import CompanyResearch from './components/CompanyResearch'
import DealPipeline from './components/DealPipeline'
import MarketIntel from './components/MarketIntel'
import NetworkGraph from './components/NetworkGraph'
import DataSources from './components/DataSources'

const API_BASE = '/api/v1'

function App() {
  const [activeTab, setActiveTab] = useState('research')
  const [health, setHealth] = useState(null)
  const [stats, setStats] = useState(null)

  useEffect(() => {
    // Check API health
    fetch('/health')
      .then(r => r.json())
      .then(setHealth)
      .catch(() => setHealth({ status: 'error' }))

    // Get search stats
    fetch(`${API_BASE}/search/stats`)
      .then(r => r.json())
      .then(setStats)
      .catch(() => {})
  }, [])

  const tabs = [
    { id: 'research', label: '🔬 Research', icon: '🔬' },
    { id: 'deals', label: '💼 Deals', icon: '💼' },
    { id: 'market', label: '📊 Market', icon: '📊' },
    { id: 'network', label: '🕸️ Network', icon: '🕸️' },
    { id: 'sources', label: '📡 Sources', icon: '📡' },
  ]

  return (
    <div className="app">
      <header className="header">
        <div className="logo">
          <span className="logo-icon">📈</span>
          <h1>Nexdata</h1>
          <span className="tagline">Investment Intelligence</span>
        </div>
        <div className="status">
          <span className={`health-dot ${health?.status === 'healthy' ? 'healthy' : 'error'}`} />
          <span>{health?.status === 'healthy' ? 'API Connected' : 'API Offline'}</span>
          {stats?.total_indexed > 0 && (
            <span className="stat">{stats.total_indexed.toLocaleString()} records</span>
          )}
        </div>
      </header>

      <nav className="tabs">
        {tabs.map(tab => (
          <button
            key={tab.id}
            className={`tab ${activeTab === tab.id ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      <main className="content">
        {activeTab === 'research' && <CompanyResearch />}
        {activeTab === 'deals' && <DealPipeline />}
        {activeTab === 'market' && <MarketIntel />}
        {activeTab === 'network' && <NetworkGraph />}
        {activeTab === 'sources' && <DataSources />}
      </main>

      <footer className="footer">
        <a href="http://localhost:8001/docs" target="_blank" rel="noreferrer">API Docs</a>
        <span>•</span>
        <a href="http://localhost:8001/graphql" target="_blank" rel="noreferrer">GraphQL</a>
        <span>•</span>
        <span>100+ Endpoints • 25+ Data Sources</span>
      </footer>
    </div>
  )
}

export default App

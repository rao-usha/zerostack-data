# Nexdata Demo Suite

Interactive demos for showcasing Nexdata's AI-powered investment intelligence platform.

## Quick Start

```bash
# Ensure API is running
docker-compose up -d

# Option 1: Run the interactive Python demo (RECOMMENDED for investors)
python demo/investor_demo.py

# Option 2: Run quick shell demo
bash demo/quick_demo.sh

# Option 3: Web dashboard
cd demo && python -m http.server 8080
# Then open http://localhost:8080
```

## Demo Scripts

### Interactive Python Demo (Best for Investor Meetings)
```bash
# Full interactive demo with pauses between sections
python demo/investor_demo.py

# Quick demo without pauses
python demo/investor_demo.py --quick

# Run specific section only
python demo/investor_demo.py --section overview    # Platform stats
python demo/investor_demo.py --section web         # Web traffic intelligence
python demo/investor_demo.py --section github      # GitHub analytics
python demo/investor_demo.py --section markets     # Prediction markets
python demo/investor_demo.py --section research    # Company research
python demo/investor_demo.py --section competitive # Scoring & rankings
```

### Shell Demo (Quick Terminal Demo)
```bash
bash demo/quick_demo.sh
```

## Data Highlights

| Metric | Value |
|--------|-------|
| LPs Tracked | 564 |
| Family Offices | 308 |
| Portfolio Companies | 5,236 |
| SEC 13F Holdings | 4,291 |
| API Endpoints | 400+ |
| Data Sources | 25+ |
| Prediction Markets | 18 (live) |
| AUM Tracked | $32T+ |

## Features Showcased

### Core Features
- **Company Research Agent** - Autonomous AI that queries 9+ data sources in parallel
- **Due Diligence** - Automated DD with risk scoring and IC-ready memos
- **Health Scores** - ML-powered scoring with A-F tiers

### NEW: Phase 5 AI Agents
- **Report Writer** - Generate investor memos and company profiles instantly
- **Anomaly Detection** - AI monitors data 24/7 and alerts on unusual patterns
- **Competitive Intelligence** - Automated moat scoring and landscape analysis
- **Data Hunter** - Identifies and fills data gaps automatically

### Data Sources
- **Glassdoor** - Employee sentiment and ratings
- **App Store** - Mobile presence and rankings
- **Web Traffic** - Tranco domain rankings
- **News Feed** - Real-time news monitoring

### Portfolio Intelligence
- **Investor Network** - Co-investment relationships and central investors
- **Deal Predictions** - ML-powered win probability scoring

## Demo Flow (5 minutes)

1. **Open the dashboard** and show the header stats (559 endpoints, 40+ sources, 8 AI agents)

2. **Company Research** (1 min)
   - Type "Stripe" and click "Run Research"
   - Show the parallel data gathering from multiple sources
   - Highlight the health score and confidence level

3. **Due Diligence** (1 min)
   - Switch to DD tab
   - Run DD on "Anthropic" with "quick" template
   - Show risk score, red flags, and memo generation

4. **AI Report Writer** (1 min)
   - Switch to Report Writer tab
   - Generate a "Company Profile" for "OpenAI"
   - Show the instant markdown report generation

5. **Competitive Intelligence** (1 min)
   - Switch to Competitive Intel tab
   - Analyze "Stripe"
   - Show moat scoring (network effects, switching costs, brand)

6. **Data Sources** (30 sec)
   - Show Glassdoor ratings
   - Show App Store rankings
   - Show web traffic data

7. **Pipeline Predictions** (30 sec)
   - Show ML-powered win probability
   - Show deal pipeline summary

## Key Talking Points

| Feature | One-Liner |
|---------|-----------|
| Company Research | "9 data sources queried in parallel, synthesized in 5 seconds" |
| Due Diligence | "What takes analysts days, done in under a minute" |
| Report Writer | "Investor memos and DD reports generated instantly from live data" |
| Anomaly Detection | "AI monitors 24/7 and alerts you when things change" |
| Competitive Intel | "Automated moat scoring - network effects, switching costs, brand" |

## Metrics to Mention

- **559 API endpoints** across 40+ data sources
- **8 autonomous AI agents** for research and analysis
- **Sub-second response times** for most queries
- **Real-time data synthesis** from SEC, GitHub, Glassdoor, App Store

## Requirements

- Docker & docker-compose
- Python 3.8+ (for serving static files)
- Modern web browser (Chrome, Firefox, Edge)
- API running on `localhost:8001`

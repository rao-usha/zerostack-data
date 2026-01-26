# Nexdata Quick Demo Commands

> Copy-paste ready commands for live demo. Base URL: `http://localhost:8001`

---

## 🚀 The Big Three (Show These First)

### 1. Autonomous Company Research
```bash
curl -X POST http://localhost:8001/api/v1/agents/research/company \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe"}'
```

### 2. Automated Due Diligence
```bash
curl -X POST http://localhost:8001/api/v1/diligence/start \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe", "template": "quick"}'
```

### 3. Company Health Score
```bash
curl http://localhost:8001/api/v1/scores/company/Stripe
```

---

## 📊 Data Sources (Pick Any)

```bash
# SEC Form D (Private placements)
curl "http://localhost:8001/api/v1/form-d/recent?limit=5"

# GitHub Analytics
curl http://localhost:8001/api/v1/github/org/stripe

# Glassdoor Ratings
curl http://localhost:8001/api/v1/glassdoor/company/Stripe

# App Store
curl "http://localhost:8001/api/v1/apps/search?q=stripe"

# Web Traffic Rankings
curl http://localhost:8001/api/v1/web-traffic/domain/stripe.com
```

---

## 🔍 Search & Discovery

```bash
# Full-text search
curl "http://localhost:8001/api/v1/search?q=fintech"

# Autocomplete suggestions
curl "http://localhost:8001/api/v1/search/suggest?prefix=strip"

# Find similar investors
curl http://localhost:8001/api/v1/discover/similar/1

# Investment trends
curl http://localhost:8001/api/v1/trends/emerging
```

---

## 🕸️ Network Analysis

```bash
# Most connected investors (shows real data)
curl http://localhost:8001/api/v1/network/central

# Co-investor network for CalPERS
curl "http://localhost:8001/api/v1/network/investor/1?investor_type=lp"

# Deal predictions with win probability
curl http://localhost:8001/api/v1/predictions/pipeline
```

---

## 📈 Check Job Status

```bash
# Research job status
curl http://localhost:8001/api/v1/agents/research/{JOB_ID}

# DD job status
curl http://localhost:8001/api/v1/diligence/{JOB_ID}
```

---

## 🔗 Useful URLs

- **API Docs:** http://localhost:8001/docs
- **GraphQL:** http://localhost:8001/graphql
- **Health Check:** http://localhost:8001/health

---

---

## 🤖 Phase 5: Agentic AI Features

```bash
# Anomaly Detection - AI spots unusual patterns
curl -X POST http://localhost:8001/api/v1/anomalies/scan \
  -d '{"company_name": "Stripe"}' -H "Content-Type: application/json"

# AI Report Writer - Generate instant reports
curl -X POST http://localhost:8001/api/v1/ai-reports/generate \
  -d '{"report_type": "company_profile", "entity_name": "Stripe"}' -H "Content-Type: application/json"

# Competitive Intelligence - Moat and landscape analysis
curl http://localhost:8001/api/v1/competitive/Stripe

# Data Hunter - Find missing data automatically
curl http://localhost:8001/api/v1/hunter/stats

# News Monitoring - Real-time alerts
curl http://localhost:8001/api/v1/monitors/news/stats
```

---

## 💬 One-Liners for Each Feature

| Feature | What to Say |
|---------|-------------|
| Company Research | "Queries 9 data sources in parallel, synthesizes in seconds" |
| Due Diligence | "Automated DD with risk scoring - what takes days, done in a minute" |
| Health Scores | "ML-powered scoring with A-F tiers and confidence levels" |
| Data Breadth | "SEC, GitHub, Glassdoor, App Store, web traffic - all unified" |
| Network Graph | "See who invests alongside who, find the most connected LPs" |
| Deal Predictions | "Win probability scoring to prioritize your pipeline" |
| **Anomaly Detection** | "AI monitors 24/7 and alerts you when things change" |
| **Report Writer** | "Generate investor memos and DD reports instantly" |
| **Competitive Intel** | "Automated moat scoring and landscape analysis" |

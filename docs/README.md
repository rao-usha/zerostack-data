# Nexdata Documentation

> AI-powered investment intelligence platform with 400+ API endpoints across 28 data sources.

---

## Quick Links

| Resource | Description |
|----------|-------------|
| **[MASTER_CHECKLIST.md](MASTER_CHECKLIST.md)** | Complete feature list with status, plans, and data counts |
| **[Swagger UI](http://localhost:8001/docs)** | Interactive API documentation |
| **[Demo Guide](demo/)** | Investor demo materials |

---

## Documentation Structure

```
docs/
├── MASTER_CHECKLIST.md    # ⭐ Complete feature checklist
├── getting-started/       # Setup and quickstart guides
├── api/                   # API documentation
├── data-sources/          # Per-source documentation
├── demo/                  # Investor demo materials
├── plans/                 # Implementation plans (39 completed)
│   ├── completed/         # Archived completed plans
│   └── README.md          # Plans index
└── historical/            # Archived docs (architecture, changelog)
```

---

## Getting Started

| Guide | Description |
|-------|-------------|
| [Quickstart](getting-started/QUICKSTART.md) | Get up and running in 5 minutes |
| [Getting Started](getting-started/GETTING_STARTED.md) | Detailed setup instructions |
| [Usage Guide](getting-started/USAGE.md) | How to use the API |

---

## Platform Summary

| Metric | Count |
|--------|-------|
| Features Implemented | 50+ |
| Data Sources | 28 |
| API Endpoints | 400+ |
| LPs Tracked | 564 |
| Family Offices | 308 |
| Portfolio Companies | 5,236 |

---

## Key Capabilities

### Agentic AI (Phase 5)
Autonomous AI agents for investment research:
- **Company Research** - Multi-source company intelligence
- **Due Diligence** - Automated DD with risk scoring
- **News Monitor** - Real-time news tracking
- **Market Scanner** - Signal detection

### Data Sources
- **Regulatory**: SEC Form D, Form ADV, EDGAR
- **Alternative**: GitHub, Glassdoor, Web Traffic, Prediction Markets
- **Economic**: FRED, BLS, Census, EIA
- **Financial**: FDIC, Treasury, USPTO

### Analytics
- Full-text search with fuzzy matching
- Investor similarity and recommendations
- Portfolio comparison tools
- Deal pipeline and scoring

---

## Demo

For investor presentations:
- [Investor Demo Guide](demo/INVESTOR_DEMO.md)
- [Quick Commands](demo/QUICK_COMMANDS.md)
- [Feature Highlights](demo/FEATURE_HIGHLIGHTS.md)

Run the demo:
```bash
python demo/investor_demo.py --quick
```

---

## Implementation Plans

All 39 completed plans are in [plans/](plans/):
- **Phase 1**: Core Infrastructure (T01-T10)
- **Phase 2**: Data Delivery (T11-T20)
- **Phase 3**: Investment Intelligence (T21-T30)
- **Phase 4**: Data Expansion (T31-T40)
- **Phase 5**: Agentic AI (T41-T50)

See [plans/README.md](plans/README.md) for the complete index.

---

## Historical Docs

Archived documentation is in [historical/](historical/):
- Architecture and system design
- Changelog and release notes
- Internal development notes

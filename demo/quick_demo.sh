#!/bin/bash
# Nexdata Quick Demo - Shell Script
# Run with: bash demo/quick_demo.sh

API="http://localhost:8001/api/v1"
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "=============================================="
echo -e "${YELLOW}       NEXDATA - Quick API Demo${NC}"
echo "=============================================="
echo ""

# Health check
echo -e "${BLUE}>>> Checking API health...${NC}"
curl -s "$API/../health" | python -m json.tool 2>/dev/null | head -5
echo ""

# Platform Overview
echo -e "${BLUE}>>> Platform Overview${NC}"
echo -e "${GREEN}Total LPs, Family Offices, and Portfolio Companies:${NC}"
curl -s "$API/analytics/overview" | python -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  LPs: {d.get('total_lps', 0)}\")
print(f\"  Family Offices: {d.get('total_family_offices', 0)}\")
print(f\"  Portfolio Companies: {d.get('total_portfolio_companies', 0)}\")
print(f\"  Unique Companies: {d.get('unique_companies', 0)}\")
"
echo ""

# Web Traffic
echo -e "${BLUE}>>> Web Traffic Intelligence${NC}"
echo -e "${GREEN}Top 5 Global Domains:${NC}"
curl -s "$API/web-traffic/rankings?limit=5" | python -c "
import json,sys
d=json.load(sys.stdin)
for r in d.get('rankings', []):
    print(f\"  #{r['rank']}: {r['domain']}\")
"
echo ""

echo -e "${GREEN}Fintech Comparison:${NC}"
curl -s "$API/web-traffic/compare?domains=stripe.com&domains=paypal.com&domains=shopify.com" | python -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('comparison', []):
    print(f\"  {c['domain']}: Rank #{c.get('tranco_rank', 'N/A')}\")
"
echo ""

# GitHub Intelligence
echo -e "${BLUE}>>> GitHub Intelligence${NC}"
echo -e "${GREEN}OpenAI Developer Activity:${NC}"
curl -s "$API/github/org/openai" | python -c "
import json,sys
d=json.load(sys.stdin)
m=d.get('metrics', {})
print(f\"  Repos: {d.get('public_repos', 0)}\")
print(f\"  Stars: {m.get('total_stars', 0):,}\")
print(f\"  Forks: {m.get('total_forks', 0):,}\")
print(f\"  Followers: {d.get('followers', 0):,}\")
print(f\"  Velocity Score: {d.get('velocity_score', 0)}\")
"
echo ""

# Prediction Markets
echo -e "${BLUE}>>> Prediction Markets${NC}"
echo -e "${GREEN}Dashboard:${NC}"
curl -s "$API/prediction-markets/dashboard" | python -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  Total Markets: {d.get('total_markets', 0)}\")
print(f\"  High Priority Markets:\")
for m in d.get('high_priority_markets', [])[:3]:
    prob = m.get('yes_probability', 0) * 100
    vol = m.get('volume_usd', 0) / 1e6
    print(f\"    - {m['question'][:50]}...\")
    print(f\"      Prob: {prob:.1f}% | Volume: \${vol:.1f}M\")
"
echo ""

# Company Research
echo -e "${BLUE}>>> Company Research (Stripe)${NC}"
echo -e "${GREEN}Glassdoor Data:${NC}"
curl -s "$API/glassdoor/company/Stripe" | python -c "
import json,sys
d=json.load(sys.stdin)
r=d.get('ratings', {})
s=d.get('sentiment', {})
print(f\"  Overall Rating: {r.get('overall', 0)}/5.0\")
print(f\"  CEO Approval: {s.get('ceo_approval', 0)*100:.0f}%\")
print(f\"  Recommend: {s.get('recommend_to_friend', 0)*100:.0f}%\")
"
echo ""

# LP Coverage
echo -e "${BLUE}>>> LP Coverage${NC}"
curl -s "$API/lp-collection/coverage" | python -c "
import json,sys
d=json.load(sys.stdin)
print(f\"  Total LPs: {d.get('total_lps', 0)}\")
print(f\"  With Data: {d.get('lps_with_data', 0)}\")
print(f\"  Coverage by Type:\")
for t, s in d.get('coverage_by_type', {}).items():
    print(f\"    {t}: {s.get('coverage_pct', 0):.1f}%\")
"
echo ""

echo "=============================================="
echo -e "${GREEN}Demo Complete!${NC}"
echo "Full docs: http://localhost:8001/docs"
echo "=============================================="

#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# generate_report.sh — trigger any registered Nexdata report template
#
# Usage:
#   ./scripts/generate_report.sh                        # default: les_schwab_av
#   ./scripts/generate_report.sh les_schwab_av
#   ./scripts/generate_report.sh pe_market_brief
#   ./scripts/generate_report.sh les_schwab_av excel    # html (default) or excel
#
# Output: prints the full report JSON (id, status, file_path, download_url)
# The generated file lives in /app/data/reports/ inside the container.
# Access it at: http://localhost:8001/api/v1/reports/<id>/download
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

API="${NEXDATA_API:-http://localhost:8001/api/v1}"
TEMPLATE="${1:-les_schwab_av}"
FORMAT="${2:-html}"
DATE=$(date +%Y-%m-%d)

# Pretty-print the template name for the title
TITLE_NAME=$(echo "$TEMPLATE" | tr '_' ' ' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2)); print}')
TITLE="${TITLE_NAME} - ${DATE}"

echo "⚡ Generating '${TEMPLATE}' (${FORMAT}) as: ${TITLE}"
echo "   API: ${API}"
echo ""

PAYLOAD=$(printf '{"template":"%s","format":"%s","params":{},"title":"%s"}' \
  "$TEMPLATE" "$FORMAT" "$TITLE")

RESPONSE=$(curl -s -X POST "${API}/reports/generate" \
  -H "Content-Type: application/json" \
  --data-raw "$PAYLOAD")

STATUS=$(echo "$RESPONSE" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','unknown'))" 2>/dev/null || echo "unknown")

if [ "$STATUS" = "complete" ]; then
  ID=$(echo "$RESPONSE" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('id','?'))")
  SIZE=$(echo "$RESPONSE" | python -c "import sys,json; d=json.load(sys.stdin); s=d.get('file_size',0); print(f'{s/1024:.0f} KB' if s else '?')")
  echo "✅ Report #${ID} complete (${SIZE})"
  echo "   Open: ${API}/reports/${ID}/download"
  echo ""
  echo "$RESPONSE" | python -m json.tool
elif [ "$STATUS" = "failed" ]; then
  echo "❌ Report generation failed:"
  echo "$RESPONSE" | python -m json.tool
  exit 1
else
  echo "⏳ Report queued or unknown status: ${STATUS}"
  echo "$RESPONSE" | python -m json.tool
fi

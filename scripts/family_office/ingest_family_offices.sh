#!/bin/bash
# Family Office Form ADV Ingestion Script using curl
# This script ingests all family offices and tracks job status

BASE_URL="http://localhost:8001"

echo "================================================================================"
echo "FAMILY OFFICE FORM ADV COMPREHENSIVE INGESTION"
echo "================================================================================"
echo ""

# Function to trigger ingestion
ingest_batch() {
    local batch_name="$1"
    shift
    local offices=("$@")
    
    echo ""
    echo "================================================================================"
    echo "BATCH: $batch_name"
    echo "================================================================================"
    echo "Ingesting ${#offices[@]} family offices..."
    
    # Build JSON array
    local json_array="["
    for office in "${offices[@]}"; do
        json_array+="\"$office\","
    done
    json_array="${json_array%,}]"  # Remove trailing comma
    
    # Make API call
    local response=$(curl -s -X POST "$BASE_URL/api/v1/sec/form-adv/ingest/family-offices" \
        -H "Content-Type: application/json" \
        -d "{\"family_office_names\": $json_array, \"max_concurrency\": 1, \"max_requests_per_second\": 2.0}")
    
    local job_id=$(echo "$response" | grep -o '"job_id":[0-9]*' | grep -o '[0-9]*')
    
    if [ -z "$job_id" ]; then
        echo "❌ Failed to create job"
        echo "Response: $response"
        return 1
    fi
    
    echo "✅ Job created: ID=$job_id"
    echo ""
    
    # Wait for completion
    echo "⏳ Waiting for job to complete..."
    local max_wait=600
    local elapsed=0
    local poll_interval=10
    
    while [ $elapsed -lt $max_wait ]; do
        sleep $poll_interval
        elapsed=$((elapsed + poll_interval))
        
        local status_response=$(curl -s "$BASE_URL/api/v1/jobs/$job_id")
        local status=$(echo "$status_response" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
        
        echo "   [$elapsed s] Status: $status"
        
        if [ "$status" = "success" ] || [ "$status" = "failed" ]; then
            echo ""
            echo "Job $status!"
            
            # Extract results
            local ingested=$(echo "$status_response" | grep -o '"total_ingested":[0-9]*' | grep -o '[0-9]*')
            local found=$(echo "$status_response" | grep -o '"total_matches_found":[0-9]*' | grep -o '[0-9]*')
            
            echo "   Matches found: ${found:-0}"
            echo "   Successfully ingested: ${ingested:-0}"
            
            break
        fi
    done
    
    echo ""
}

# US Family Offices
US_OFFICES=(
    "Soros Fund Management"
    "Cohen Private Ventures"
    "MSD Capital"
    "MSD Partners"
    "Cascade Investment"
    "Walton Family Office"
    "Bezos Expeditions"
    "Emerson Collective"
    "Shad Khan Family Office"
    "Perot Investments"
    "Pritzker Group"
    "Ballmer Group"
    "Arnold Ventures"
    "Hewlett Foundation"
    "Packard Foundation"
    "Raine Group"
)

# Europe Family Offices
EUROPE_OFFICES=(
    "Cevian Capital"
    "LGT Group"
    "Bertelsmann"
    "Mohn Family Office"
    "JAB Holding Company"
    "Exor"
    "Ferrero Family Office"
    "Heineken Family Office"
)

# Middle East & Asia
ASIA_OFFICES=(
    "Kingdom Holding"
    "Olayan Group"
    "Tata Group"
    "Kuok Group"
)

# Latin America
LATAM_OFFICES=(
    "Safra Family Office"
    "Lemann Family"
    "Santo Domingo Family Office"
    "Luksic Family Office"
)

# Process each region
ingest_batch "US Family Offices" "${US_OFFICES[@]}"
sleep 5

ingest_batch "Europe Family Offices" "${EUROPE_OFFICES[@]}"
sleep 5

ingest_batch "Middle East & Asia" "${ASIA_OFFICES[@]}"
sleep 5

ingest_batch "Latin America" "${LATAM_OFFICES[@]}"

echo ""
echo "================================================================================"
echo "ALL BATCHES COMPLETE"
echo "================================================================================"
echo ""
echo "Query your data:"
echo ""
echo "docker-compose exec postgres psql -U nexdata -d nexdata -c \\"
echo "  \"SELECT firm_name, business_phone, business_email, website, assets_under_management \\"
echo "   FROM sec_form_adv ORDER BY assets_under_management DESC NULLS LAST LIMIT 20;\""
echo ""
echo "✅ DONE!"


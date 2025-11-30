# Monitor ongoing family office ingestion
# Shows progress and final results

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "MONITORING FAMILY OFFICE INGESTION" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

$BASE_URL = "http://localhost:8001"

# Function to check latest jobs
function Get-RecentJobs {
    $allJobs = Invoke-RestMethod -Uri "$BASE_URL/api/v1/jobs"
    $formAdvJobs = $allJobs | Where-Object { $_.config.type -eq "form_adv" } | Sort-Object -Property id -Descending | Select-Object -First 10
    return $formAdvJobs
}

# Function to get statistics
function Get-IngestionStats {
    Write-Host "Recent Form ADV Ingestion Jobs:" -ForegroundColor Yellow
    Write-Host ""
    
    $jobs = Get-RecentJobs
    
    $totalIngested = 0
    $totalSuccesses = 0
    $totalFailures = 0
    
    foreach ($job in $jobs) {
        $status = $job.status
        $statusColor = switch ($status) {
            "success" { "Green" }
            "failed" { "Red" }
            "running" { "Yellow" }
            default { "Gray" }
        }
        
        Write-Host "Job $($job.id): " -NoNewline
        Write-Host $status -ForegroundColor $statusColor
        
        if ($job.metadata) {
            $ingested = $job.metadata.total_ingested
            if ($ingested) {
                Write-Host "  → Ingested: $ingested firms" -ForegroundColor Green
                $totalIngested += $ingested
            }
            
            $found = $job.metadata.total_matches_found
            if ($found) {
                Write-Host "  → Found: $found matches"
            }
        }
        
        if ($status -eq "success") {
            $totalSuccesses++
        } elseif ($status -eq "failed") {
            $totalFailures++
        }
        
        Write-Host ""
    }
    
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host "SUMMARY" -ForegroundColor Cyan
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host "Total Successful Jobs: $totalSuccesses" -ForegroundColor Green
    Write-Host "Total Failed Jobs: $totalFailures" -ForegroundColor $(if ($totalFailures -gt 0) { "Red" } else { "Gray" })
    Write-Host "Total Firms Ingested: $totalIngested" -ForegroundColor Green
    Write-Host ""
}

# Check if terminal output exists
if (Test-Path "c:\Users\awron\.cursor\projects\c-Users-awron-projects-Nexdata\terminals\21.txt") {
    Write-Host "Background ingestion is running. Terminal output:" -ForegroundColor Yellow
    Write-Host ""
    Get-Content "c:\Users\awron\.cursor\projects\c-Users-awron-projects-Nexdata\terminals\21.txt" -Tail 30
    Write-Host ""
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host ""
}

# Get stats
Get-IngestionStats

# Query database for ingested data
Write-Host "DATABASE QUERY RESULTS:" -ForegroundColor Cyan
Write-Host ""

try {
    Write-Host "Querying sec_form_adv table..." -ForegroundColor Yellow
    
    $result = docker-compose exec postgres psql -U nexdata -d nexdata -t -c "SELECT COUNT(*) FROM sec_form_adv;" 2>&1
    
    if ($result -match '\d+') {
        $count = [int]($result | Select-String -Pattern '\d+').Matches[0].Value
        Write-Host "✅ Total firms in database: " -NoNewline
        Write-Host $count -ForegroundColor Green
    }
    
    Write-Host ""
    Write-Host "Sample data (top 5 firms):" -ForegroundColor Yellow
    docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT firm_name, business_phone, business_email, business_address_state FROM sec_form_adv LIMIT 5;"
    
} catch {
    Write-Host "⚠️  Could not query database" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "To query all data:" -ForegroundColor Cyan
Write-Host '  docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT * FROM sec_form_adv;"' -ForegroundColor Gray
Write-Host ""
Write-Host "To export to CSV:" -ForegroundColor Cyan
Write-Host '  docker-compose exec postgres psql -U nexdata -d nexdata -c "\copy sec_form_adv TO ''/tmp/family_offices.csv'' CSV HEADER;"' -ForegroundColor Gray
Write-Host "================================================================================" -ForegroundColor Cyan


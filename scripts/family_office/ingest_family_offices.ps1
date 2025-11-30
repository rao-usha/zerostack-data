# Family Office Form ADV Ingestion Script (PowerShell)
# This script ingests all family offices and tracks job status

$BASE_URL = "http://localhost:8001"

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "FAMILY OFFICE FORM ADV COMPREHENSIVE INGESTION" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

function Ingest-Batch {
    param(
        [string]$BatchName,
        [string[]]$Offices
    )
    
    Write-Host ""
    Write-Host "================================================================================" -ForegroundColor Yellow
    Write-Host "BATCH: $BatchName" -ForegroundColor Yellow
    Write-Host "================================================================================" -ForegroundColor Yellow
    Write-Host "Ingesting $($Offices.Count) family offices..."
    
    # Build JSON payload
    $officeList = $Offices | ForEach-Object { "`"$_`"" }
    $jsonArray = "[" + ($officeList -join ",") + "]"
    $payload = @{
        family_office_names = $Offices
        max_concurrency = 1
        max_requests_per_second = 2.0
    } | ConvertTo-Json
    
    try {
        # Make API call
        $response = Invoke-RestMethod -Uri "$BASE_URL/api/v1/sec/form-adv/ingest/family-offices" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30
        
        $jobId = $response.job_id
        Write-Host "✅ Job created: ID=$jobId" -ForegroundColor Green
        Write-Host ""
        
        # Wait for completion
        Write-Host "⏳ Waiting for job to complete..." -ForegroundColor Cyan
        $maxWait = 600
        $elapsed = 0
        $pollInterval = 10
        
        while ($elapsed -lt $maxWait) {
            Start-Sleep -Seconds $pollInterval
            $elapsed += $pollInterval
            
            try {
                $statusResponse = Invoke-RestMethod -Uri "$BASE_URL/api/v1/jobs/$jobId" -Method Get -TimeoutSec 10
                $status = $statusResponse.status
                
                Write-Host "   [$elapsed s] Status: $status"
                
                if ($status -eq "success" -or $status -eq "failed") {
                    Write-Host ""
                    if ($status -eq "success") {
                        Write-Host "Job completed successfully!" -ForegroundColor Green
                    } else {
                        Write-Host "Job failed!" -ForegroundColor Red
                    }
                    
                    # Extract results
                    if ($statusResponse.metadata) {
                        $ingested = $statusResponse.metadata.total_ingested
                        $found = $statusResponse.metadata.total_matches_found
                        $searched = $statusResponse.metadata.searched_offices
                        
                        Write-Host "   Searched: $searched"
                        Write-Host "   Matches found: $found"
                        Write-Host "   Successfully ingested: $ingested" -ForegroundColor Green
                        
                        if ($statusResponse.metadata.errors) {
                            Write-Host "   Errors: $($statusResponse.metadata.errors.Count)" -ForegroundColor Yellow
                        }
                    }
                    
                    if ($statusResponse.error_message) {
                        Write-Host "   Error: $($statusResponse.error_message)" -ForegroundColor Red
                    }
                    
                    break
                }
            } catch {
                Write-Host "   Error checking status: $_" -ForegroundColor Red
            }
        }
        
        if ($elapsed -ge $maxWait) {
            Write-Host "⚠️  Timeout reached!" -ForegroundColor Yellow
        }
        
        Write-Host ""
        return $statusResponse
        
    } catch {
        Write-Host "❌ Failed to create job: $_" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        return $null
    }
}

# Define all family offices
$USOffices = @(
    "Soros Fund Management",
    "Cohen Private Ventures",
    "MSD Capital",
    "MSD Partners", 
    "Cascade Investment",
    "Walton Family Office",
    "Bezos Expeditions",
    "Emerson Collective",
    "Shad Khan Family Office",
    "Perot Investments",
    "Pritzker Group",
    "Ballmer Group",
    "Arnold Ventures",
    "Hewlett Foundation",
    "Packard Foundation",
    "Raine Group"
)

$EuropeOffices = @(
    "Cevian Capital",
    "LGT Group",
    "Bertelsmann",
    "Mohn Family Office",
    "JAB Holding Company",
    "Exor",
    "Ferrero Family Office",
    "Heineken Family Office"
)

$AsiaOffices = @(
    "Kingdom Holding",
    "Olayan Group",
    "Tata Group",
    "Kuok Group"
)

$LatAmOffices = @(
    "Safra Family Office",
    "Lemann Family",
    "Santo Domingo Family Office",
    "Luksic Family Office"
)

Write-Host "⚠️  IMPORTANT NOTES:" -ForegroundColor Yellow
Write-Host "   - Many family offices have SEC registration exemptions"
Write-Host "   - Only registered investment advisers will be found"
Write-Host "   - This is normal and expected behavior"
Write-Host "   - We retrieve BUSINESS contact info only (not personal PII)"
Write-Host ""
Write-Host "🕐 Estimated time: 10-20 minutes (rate limited for API safety)"
Write-Host ""

# Process each region
$allResults = @{}

$allResults["US"] = Ingest-Batch -BatchName "US Family Offices" -Offices $USOffices
Start-Sleep -Seconds 5

$allResults["Europe"] = Ingest-Batch -BatchName "Europe Family Offices" -Offices $EuropeOffices
Start-Sleep -Seconds 5

$allResults["Asia"] = Ingest-Batch -BatchName "Middle East & Asia" -Offices $AsiaOffices
Start-Sleep -Seconds 5

$allResults["LatAm"] = Ingest-Batch -BatchName "Latin America" -Offices $LatAmOffices

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "ALL BATCHES COMPLETE" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Calculate totals
$totalIngested = 0
$totalFound = 0
$totalSearched = 0

foreach ($result in $allResults.Values) {
    if ($result -and $result.metadata) {
        $totalIngested += $result.metadata.total_ingested
        $totalFound += $result.metadata.total_matches_found
        $totalSearched += $result.metadata.searched_offices
    }
}

Write-Host "SUMMARY:" -ForegroundColor Cyan
Write-Host "  Total Searched: $totalSearched"
Write-Host "  Total Matches Found: $totalFound"
Write-Host "  Total Ingested: $totalIngested" -ForegroundColor Green
if ($totalSearched -gt 0) {
    $successRate = [math]::Round(($totalIngested / $totalSearched) * 100, 1)
    Write-Host "  Success Rate: $successRate%" -ForegroundColor Green
}
Write-Host ""

Write-Host "NEXT STEPS - QUERY YOUR DATA:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Query all ingested firms:"
Write-Host '   docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT firm_name, business_phone, business_email, website, assets_under_management FROM sec_form_adv ORDER BY assets_under_management DESC NULLS LAST LIMIT 20;"' -ForegroundColor Gray
Write-Host ""
Write-Host "2. Query family offices specifically:"
Write-Host '   docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT firm_name, business_phone, business_email FROM sec_form_adv WHERE is_family_office = TRUE;"' -ForegroundColor Gray
Write-Host ""
Write-Host "3. Query key personnel:"
Write-Host '   docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT f.firm_name, p.full_name, p.title, p.email FROM sec_form_adv f JOIN sec_form_adv_personnel p ON f.crd_number = p.crd_number LIMIT 50;"' -ForegroundColor Gray
Write-Host ""
Write-Host "✅ INGESTION COMPLETE!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan


# Trigger FRED ingestion via API endpoint

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "FRED Data Ingestion via API" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Ingest Interest Rates
Write-Host "1. Ingesting Interest Rates..." -ForegroundColor Yellow

$body = @{
    category = "interest_rates"
    observation_start = "2024-10-01"
    observation_end = "2025-11-28"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/fred/ingest" `
        -Method Post `
        -Body $body `
        -ContentType "application/json"
    
    $jobId = $response.job_id
    Write-Host "   Job created: $jobId" -ForegroundColor Green
    
    # Wait and check status
    Start-Sleep -Seconds 5
    
    $jobStatus = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/jobs/$jobId"
    Write-Host "   Status: $($jobStatus.status)" -ForegroundColor $(if ($jobStatus.status -eq "success") { "Green" } else { "Yellow" })
    
    if ($jobStatus.rows_inserted) {
        Write-Host "   Rows inserted: $($jobStatus.rows_inserted)" -ForegroundColor Green
    }
    
} catch {
    Write-Host "   Error: $_" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Checking database..." -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check data in database
docker-compose exec postgres psql -U nexdata -d nexdata -c "SELECT COUNT(*) as total_rows, COUNT(DISTINCT series_id) as series_count FROM fred_interest_rates;"

Write-Host "`n"


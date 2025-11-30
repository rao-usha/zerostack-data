# Load family office data from JSON file
param(
    [string]$JsonFile = "data/family_offices_data.json",
    [string]$ApiUrl = "http://localhost:8001"
)

Write-Host "=" * 80
Write-Host "FAMILY OFFICE DATA LOADER" -ForegroundColor Cyan
Write-Host "=" * 80
Write-Host ""

# Read JSON file
Write-Host "📂 Reading: $JsonFile"
$offices = Get-Content $JsonFile -Raw | ConvertFrom-Json

Write-Host "📊 Found $($offices.Count) family offices to load`n"

$loaded = 0
$errors = @()

foreach ($office in $offices) {
    $name = $office.name
    
    try {
        # Convert to JSON
        $body = $office | ConvertTo-Json -Depth 5 -Compress
        
        # Post to API
        $response = Invoke-RestMethod -Uri "$ApiUrl/api/v1/family-offices/" `
            -Method Post `
            -Body $body `
            -ContentType "application/json" `
            -ErrorAction Stop
        
        Write-Host "  ✅ $($loaded + 1). $name" -ForegroundColor Green
        $loaded++
    }
    catch {
        $errorMsg = "$name : $($_.Exception.Message)"
        Write-Host "  ❌ $($loaded + $errors.Count + 1). $errorMsg" -ForegroundColor Red
        $errors += $errorMsg
        
        # Try to get detail
        if ($_.ErrorDetails) {
            try {
                $detail = ($_.ErrorDetails.Message | ConvertFrom-Json).detail
                Write-Host "     Detail: $detail" -ForegroundColor Yellow
            }
            catch {}
        }
    }
}

Write-Host "`n$('=' * 80)"
Write-Host "LOAD COMPLETE" -ForegroundColor Cyan
Write-Host "=" * 80
Write-Host "✅ Successfully loaded: $loaded/$($offices.Count)" -ForegroundColor Green

if ($errors.Count -gt 0) {
    Write-Host "❌ Errors: $($errors.Count)" -ForegroundColor Red
    foreach ($error in $errors) {
        Write-Host "   - $error" -ForegroundColor Yellow
    }
}

if ($loaded -gt 0) {
    Write-Host "`n🎉 Family offices loaded successfully!" -ForegroundColor Green
    Write-Host "`nQuery your data:"
    Write-Host "   Invoke-RestMethod $ApiUrl/api/v1/family-offices" -ForegroundColor Cyan
    Write-Host "   Invoke-RestMethod $ApiUrl/api/v1/family-offices/stats/overview" -ForegroundColor Cyan
    Write-Host "`nOr open Swagger UI:"
    Write-Host "   Start-Process $ApiUrl/docs" -ForegroundColor Cyan
}


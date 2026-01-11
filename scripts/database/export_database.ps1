# Export Database Script
# This script exports the entire PostgreSQL database to a backup file

param(
    [string]$OutputPath = ".\backup\nexdata_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql",
    [switch]$CompressGzip = $false,
    [switch]$DataOnly = $false,
    [switch]$SchemaOnly = $false
)

Write-Host "=== Nexdata Database Export Script ===" -ForegroundColor Cyan
Write-Host ""

# Database connection details from docker-compose.yml
$DB_HOST = "localhost"
$DB_PORT = "5433"
$DB_NAME = "nexdata"
$DB_USER = "nexdata"
$DB_PASSWORD = "nexdata_dev_password"

# Create backup directory if it doesn't exist
$BackupDir = Split-Path -Parent $OutputPath
if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
    Write-Host "Created backup directory: $BackupDir" -ForegroundColor Green
}

# Check if PostgreSQL container is running
Write-Host "Checking if PostgreSQL container is running..." -ForegroundColor Yellow
$ContainerStatus = docker ps --filter "name=postgres" --format "{{.Names}}" 2>$null
if (-not $ContainerStatus) {
    Write-Host "ERROR: PostgreSQL container is not running!" -ForegroundColor Red
    Write-Host "Please start it with: docker-compose up -d postgres" -ForegroundColor Yellow
    exit 1
}
Write-Host "PostgreSQL container is running: $ContainerStatus" -ForegroundColor Green
Write-Host ""

# Build pg_dump command options
$DumpOptions = @()
if ($DataOnly) {
    $DumpOptions += "--data-only"
    Write-Host "Mode: Data only (no schema)" -ForegroundColor Yellow
} elseif ($SchemaOnly) {
    $DumpOptions += "--schema-only"
    Write-Host "Mode: Schema only (no data)" -ForegroundColor Yellow
} else {
    Write-Host "Mode: Full backup (schema + data)" -ForegroundColor Yellow
}

$DumpOptions += "--verbose"
$DumpOptions += "--no-owner"
$DumpOptions += "--no-acl"

$OptionsString = $DumpOptions -join " "

Write-Host "Output file: $OutputPath" -ForegroundColor Yellow
Write-Host "Starting export..." -ForegroundColor Cyan
Write-Host ""

# Set environment variable for password
$env:PGPASSWORD = $DB_PASSWORD

try {
    if ($CompressGzip) {
        # Export with gzip compression
        $OutputPath = $OutputPath -replace '\.sql$', '.sql.gz'
        docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U $DB_USER -d $DB_NAME $OptionsString | gzip > $OutputPath
    } else {
        # Export without compression using docker exec
        docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U $DB_USER -d $DB_NAME $OptionsString > $OutputPath
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "=== Export Successful ===" -ForegroundColor Green
        $FileInfo = Get-Item $OutputPath
        Write-Host "File: $($FileInfo.FullName)" -ForegroundColor Green
        Write-Host "Size: $([math]::Round($FileInfo.Length / 1MB, 2)) MB" -ForegroundColor Green
        Write-Host ""
        Write-Host "To restore this backup, use:" -ForegroundColor Cyan
        Write-Host "  .\scripts\database\restore_database.ps1 -BackupPath '$OutputPath'" -ForegroundColor White
    } else {
        Write-Host ""
        Write-Host "ERROR: Export failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host ""
    Write-Host "ERROR: Export failed - $_" -ForegroundColor Red
    exit 1
} finally {
    # Clear password from environment
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}


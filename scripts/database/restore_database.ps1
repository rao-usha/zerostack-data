# Restore Database Script
# This script restores a PostgreSQL database from a backup file

param(
    [Parameter(Mandatory=$true)]
    [string]$BackupPath,
    [switch]$DropExisting = $false,
    [string]$TargetDatabase = "nexdata"
)

Write-Host "=== Nexdata Database Restore Script ===" -ForegroundColor Cyan
Write-Host ""

# Database connection details from docker-compose.yml
$DB_HOST = "localhost"
$DB_PORT = "5433"
$DB_USER = "nexdata"
$DB_PASSWORD = "nexdata_dev_password"

# Check if backup file exists
if (-not (Test-Path $BackupPath)) {
    Write-Host "ERROR: Backup file not found: $BackupPath" -ForegroundColor Red
    exit 1
}

$FileInfo = Get-Item $BackupPath
Write-Host "Backup file: $($FileInfo.FullName)" -ForegroundColor Yellow
Write-Host "Size: $([math]::Round($FileInfo.Length / 1MB, 2)) MB" -ForegroundColor Yellow
Write-Host ""

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

# Set environment variable for password
$env:PGPASSWORD = $DB_PASSWORD

try {
    if ($DropExisting) {
        Write-Host "WARNING: Dropping existing database '$TargetDatabase'..." -ForegroundColor Red
        Write-Host "Press Ctrl+C within 5 seconds to cancel..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
        
        # Drop and recreate database
        docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U $DB_USER -d postgres -c "DROP DATABASE IF EXISTS $TargetDatabase;"
        docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U $DB_USER -d postgres -c "CREATE DATABASE $TargetDatabase OWNER $DB_USER;"
        Write-Host "Database recreated." -ForegroundColor Green
        Write-Host ""
    }
    
    Write-Host "Starting restore to database '$TargetDatabase'..." -ForegroundColor Cyan
    Write-Host ""
    
    # Check if file is gzipped
    if ($BackupPath -match '\.gz$') {
        Write-Host "Detected gzip-compressed backup file" -ForegroundColor Yellow
        Get-Content $BackupPath | gunzip | docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U $DB_USER -d $TargetDatabase
    } else {
        # Restore from plain SQL file
        Get-Content $BackupPath -Raw | docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U $DB_USER -d $TargetDatabase
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "=== Restore Successful ===" -ForegroundColor Green
        Write-Host "Database '$TargetDatabase' has been restored from backup." -ForegroundColor Green
        Write-Host ""
        Write-Host "You can now start the API with:" -ForegroundColor Cyan
        Write-Host "  docker-compose up -d" -ForegroundColor White
    } else {
        Write-Host ""
        Write-Host "WARNING: Restore completed with warnings (exit code $LASTEXITCODE)" -ForegroundColor Yellow
        Write-Host "This is normal if the database already had some tables." -ForegroundColor Yellow
    }
} catch {
    Write-Host ""
    Write-Host "ERROR: Restore failed - $_" -ForegroundColor Red
    exit 1
} finally {
    # Clear password from environment
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}


# External Data Ingestion Service - Startup Script (PowerShell)
# 
# Features:
# - Starts PostgreSQL via Docker Compose
# - Waits for database readiness with timeout
# - Starts FastAPI application with health checks
# - Auto-restarts on failures
# - Graceful shutdown handling

param(
    [int]$MaxRestartAttempts = 3,
    [int]$DbStartupTimeout = 60,
    [int]$AppStartupTimeout = 30,
    [int]$RestartDelay = 5,
    [switch]$StopDbOnExit
)

# Configuration
$ErrorActionPreference = "Continue"
$HealthCheckUrl = "http://localhost:8000/health"
$ApiDocsUrl = "http://localhost:8000/docs"
$LogFile = "service_startup.log"

# Global state
$AppProcess = $null
$RestartAttempt = 0
$ShutdownRequested = $false

# Logging function
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp - $Level - $Message"
    
    # Color coding
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        default { Write-Host $logMessage }
    }
    
    # Write to log file
    Add-Content -Path $LogFile -Value $logMessage
}

# Check prerequisites
function Test-Prerequisites {
    Write-Log "Checking prerequisites..."
    
    # Check for docker-compose
    try {
        $dcVersion = docker-compose --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Log "docker-compose not found or not working" "ERROR"
            return $false
        }
        Write-Log "✓ Docker Compose: $dcVersion" "SUCCESS"
    }
    catch {
        Write-Log "docker-compose not available: $_" "ERROR"
        return $false
    }
    
    # Check for docker-compose.yml
    if (-not (Test-Path "docker-compose.yml")) {
        Write-Log "docker-compose.yml not found in current directory" "ERROR"
        return $false
    }
    Write-Log "✓ docker-compose.yml found" "SUCCESS"
    
    # Check for app/main.py
    if (-not (Test-Path "app\main.py")) {
        Write-Log "app\main.py not found" "ERROR"
        return $false
    }
    Write-Log "✓ app\main.py found" "SUCCESS"
    
    # Check for virtual environment
    if (Test-Path "venv\Scripts\activate.ps1") {
        Write-Log "✓ Virtual environment found" "SUCCESS"
    }
    else {
        Write-Log "Virtual environment not found" "WARNING"
    }
    
    return $true
}

# Start database
function Start-Database {
    Write-Log "Starting PostgreSQL database..."
    
    try {
        $result = docker-compose up -d db 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Failed to start database: $result" "ERROR"
            return $false
        }
        
        Write-Log "✓ Database container started" "SUCCESS"
        return $true
    }
    catch {
        Write-Log "Error starting database: $_" "ERROR"
        return $false
    }
}

# Wait for database
function Wait-ForDatabase {
    Write-Log "Waiting for database to be ready (timeout: $DbStartupTimeout seconds)..."
    
    $startTime = Get-Date
    $timeoutTime = $startTime.AddSeconds($DbStartupTimeout)
    
    while ((Get-Date) -lt $timeoutTime) {
        if ($ShutdownRequested) {
            return $false
        }
        
        try {
            $result = docker-compose exec -T db pg_isready -U postgres 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Log "✓ Database is ready" "SUCCESS"
                return $true
            }
        }
        catch {
            # Ignore errors during startup
        }
        
        Start-Sleep -Seconds 2
    }
    
    Write-Log "Database failed to become ready within timeout period" "ERROR"
    return $false
}

# Start application
function Start-Application {
    Write-Log "Starting FastAPI application..."
    
    try {
        # Activate virtual environment if it exists
        if (Test-Path "venv\Scripts\activate.ps1") {
            . "venv\Scripts\activate.ps1"
        }
        
        # Start uvicorn as a background job
        $script:AppProcess = Start-Process -FilePath "python" `
            -ArgumentList "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000" `
            -PassThru `
            -NoNewWindow `
            -RedirectStandardOutput "app_stdout.log" `
            -RedirectStandardError "app_stderr.log"
        
        Write-Log "✓ Application process started (PID: $($AppProcess.Id))" "SUCCESS"
        return $true
    }
    catch {
        Write-Log "Error starting application: $_" "ERROR"
        return $false
    }
}

# Wait for application
function Wait-ForApplication {
    Write-Log "Waiting for application to be ready (timeout: $AppStartupTimeout seconds)..."
    
    $startTime = Get-Date
    $timeoutTime = $startTime.AddSeconds($AppStartupTimeout)
    
    while ((Get-Date) -lt $timeoutTime) {
        if ($ShutdownRequested) {
            return $false
        }
        
        # Check if process is still running
        if ($AppProcess -and $AppProcess.HasExited) {
            Write-Log "Application process terminated unexpectedly (Exit Code: $($AppProcess.ExitCode))" "ERROR"
            
            # Show error log
            if (Test-Path "app_stderr.log") {
                $stderr = Get-Content "app_stderr.log" -Tail 20
                Write-Log "Last 20 lines of stderr:" "ERROR"
                $stderr | ForEach-Object { Write-Log $_ "ERROR" }
            }
            
            return $false
        }
        
        try {
            $response = Invoke-WebRequest -Uri $HealthCheckUrl -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                Write-Log "✓ Application is ready and healthy" "SUCCESS"
                Write-Log "  API Documentation: $ApiDocsUrl" "SUCCESS"
                return $true
            }
        }
        catch {
            # Ignore errors during startup
        }
        
        Start-Sleep -Seconds 2
    }
    
    Write-Log "Application failed to become ready within timeout period" "ERROR"
    return $false
}

# Monitor application
function Monitor-Application {
    Write-Log "Monitoring application health..."
    $consecutiveFailures = 0
    
    while (-not $ShutdownRequested) {
        Start-Sleep -Seconds 10
        
        # Check if process is still running
        if ($AppProcess -and $AppProcess.HasExited) {
            Write-Log "Application process has terminated (Exit Code: $($AppProcess.ExitCode))" "ERROR"
            return $false
        }
        
        try {
            $response = Invoke-WebRequest -Uri $HealthCheckUrl -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                $consecutiveFailures = 0
            }
            else {
                $consecutiveFailures++
                Write-Log "Health check failed with status $($response.StatusCode) (failures: $consecutiveFailures)" "WARNING"
            }
        }
        catch {
            $consecutiveFailures++
            Write-Log "Health check failed: $($_.Exception.Message) (failures: $consecutiveFailures)" "WARNING"
        }
        
        # Restart if too many failures
        if ($consecutiveFailures -ge 3) {
            Write-Log "Too many consecutive health check failures" "ERROR"
            return $false
        }
    }
    
    return $true
}

# Stop application
function Stop-Application {
    if ($AppProcess) {
        Write-Log "Stopping application..."
        try {
            # Try graceful shutdown first
            $AppProcess | Stop-Process -ErrorAction Stop
            $AppProcess.WaitForExit(10000)
            Write-Log "✓ Application stopped" "SUCCESS"
        }
        catch {
            Write-Log "Application didn't stop gracefully, forcing..." "WARNING"
            $AppProcess | Stop-Process -Force -ErrorAction SilentlyContinue
        }
        finally {
            $script:AppProcess = $null
        }
    }
}

# Stop database
function Stop-Database {
    Write-Log "Stopping database..."
    try {
        docker-compose down 2>&1 | Out-Null
        Write-Log "✓ Database stopped" "SUCCESS"
    }
    catch {
        Write-Log "Error stopping database: $_" "ERROR"
    }
}

# Cleanup
function Invoke-Cleanup {
    Write-Log "Cleaning up..."
    Stop-Application
    
    if ($StopDbOnExit) {
        Stop-Database
    }
    else {
        Write-Log "Database left running (use -StopDbOnExit to stop it)" "INFO"
    }
    
    Write-Log "✓ Cleanup complete" "SUCCESS"
}

# Main execution
function Main {
    Write-Log "=" * 60
    Write-Log "External Data Ingestion Service - Startup Script"
    Write-Log "=" * 60
    
    # Register cleanup on exit
    $null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
        $script:ShutdownRequested = $true
        Invoke-Cleanup
    }
    
    # Check prerequisites
    if (-not (Test-Prerequisites)) {
        Write-Log "Prerequisites check failed. Exiting." "ERROR"
        return 1
    }
    
    # Start database
    if (-not (Start-Database)) {
        Write-Log "Failed to start database. Exiting." "ERROR"
        return 1
    }
    
    # Wait for database
    if (-not (Wait-ForDatabase)) {
        Write-Log "Database not ready. Exiting." "ERROR"
        Invoke-Cleanup
        return 1
    }
    
    # Application restart loop
    $script:RestartAttempt = 0
    
    while ($RestartAttempt -lt $MaxRestartAttempts -and -not $ShutdownRequested) {
        if ($RestartAttempt -gt 0) {
            Write-Log "Restart attempt $RestartAttempt/$MaxRestartAttempts" "WARNING"
            Write-Log "Waiting $RestartDelay seconds before restart..."
            Start-Sleep -Seconds $RestartDelay
        }
        
        # Start application
        if (-not (Start-Application)) {
            $script:RestartAttempt++
            continue
        }
        
        # Wait for application to be ready
        if (-not (Wait-ForApplication)) {
            Stop-Application
            $script:RestartAttempt++
            continue
        }
        
        Write-Log "=" * 60 "SUCCESS"
        Write-Log "✓ SERVICE RUNNING" "SUCCESS"
        Write-Log "=" * 60 "SUCCESS"
        Write-Log "API Server: http://localhost:8000" "SUCCESS"
        Write-Log "API Docs: $ApiDocsUrl" "SUCCESS"
        Write-Log "Press Ctrl+C to stop" "SUCCESS"
        Write-Log "=" * 60 "SUCCESS"
        
        # Monitor application
        if (-not (Monitor-Application)) {
            Stop-Application
            $script:RestartAttempt++
            continue
        }
        
        # If we get here, shutdown was requested
        break
    }
    
    if ($RestartAttempt -ge $MaxRestartAttempts) {
        Write-Log "Maximum restart attempts ($MaxRestartAttempts) reached. Giving up." "ERROR"
    }
    
    # Cleanup
    Invoke-Cleanup
    
    Write-Log "Service stopped"
    return 0
}

# Handle Ctrl+C gracefully
try {
    $exitCode = Main
    exit $exitCode
}
catch {
    Write-Log "Unhandled error: $_" "ERROR"
    Invoke-Cleanup
    exit 1
}
finally {
    # Ensure cleanup happens
    if (-not $ShutdownRequested) {
        $script:ShutdownRequested = $true
        Invoke-Cleanup
    }
}


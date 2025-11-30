#!/bin/bash
#
# External Data Ingestion Service - Startup Script (Bash)
# 
# Features:
# - Starts PostgreSQL via Docker Compose
# - Waits for database readiness with timeout
# - Starts FastAPI application with health checks
# - Auto-restarts on failures
# - Graceful shutdown handling

set -o pipefail

# Configuration
MAX_RESTART_ATTEMPTS=3
DB_STARTUP_TIMEOUT=60
APP_STARTUP_TIMEOUT=30
RESTART_DELAY=5
STOP_DB_ON_EXIT=false

HEALTH_CHECK_URL="http://localhost:8000/health"
API_DOCS_URL="http://localhost:8000/docs"
LOG_FILE="service_startup.log"

# Global state
APP_PID=""
SHUTDOWN_REQUESTED=false
RESTART_ATTEMPT=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    local level="$1"
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        ERROR)
            echo -e "${RED}$timestamp - ERROR - $message${NC}" | tee -a "$LOG_FILE"
            ;;
        WARNING)
            echo -e "${YELLOW}$timestamp - WARNING - $message${NC}" | tee -a "$LOG_FILE"
            ;;
        SUCCESS)
            echo -e "${GREEN}$timestamp - SUCCESS - $message${NC}" | tee -a "$LOG_FILE"
            ;;
        *)
            echo "$timestamp - INFO - $message" | tee -a "$LOG_FILE"
            ;;
    esac
}

# Signal handler for graceful shutdown
signal_handler() {
    log INFO "Received shutdown signal, initiating graceful shutdown..."
    SHUTDOWN_REQUESTED=true
}

# Register signal handlers
trap signal_handler SIGINT SIGTERM

# Check prerequisites
check_prerequisites() {
    log INFO "Checking prerequisites..."
    
    # Check for docker-compose
    if ! command -v docker-compose &> /dev/null; then
        log ERROR "docker-compose not found"
        return 1
    fi
    local dc_version=$(docker-compose --version)
    log SUCCESS "✓ Docker Compose: $dc_version"
    
    # Check for docker-compose.yml
    if [ ! -f "docker-compose.yml" ]; then
        log ERROR "docker-compose.yml not found in current directory"
        return 1
    fi
    log SUCCESS "✓ docker-compose.yml found"
    
    # Check for app/main.py
    if [ ! -f "app/main.py" ]; then
        log ERROR "app/main.py not found"
        return 1
    fi
    log SUCCESS "✓ app/main.py found"
    
    # Check for virtual environment
    if [ -f "venv/bin/activate" ]; then
        log SUCCESS "✓ Virtual environment found"
    else
        log WARNING "Virtual environment not found"
    fi
    
    # Check for python
    if ! command -v python3 &> /dev/null; then
        log ERROR "python3 not found"
        return 1
    fi
    log SUCCESS "✓ Python found"
    
    return 0
}

# Start database
start_database() {
    log INFO "Starting PostgreSQL database..."
    
    if docker-compose up -d db 2>&1 | tee -a "$LOG_FILE"; then
        log SUCCESS "✓ Database container started"
        return 0
    else
        log ERROR "Failed to start database"
        return 1
    fi
}

# Wait for database
wait_for_database() {
    log INFO "Waiting for database to be ready (timeout: ${DB_STARTUP_TIMEOUT}s)..."
    
    local start_time=$(date +%s)
    local timeout_time=$((start_time + DB_STARTUP_TIMEOUT))
    
    while [ $(date +%s) -lt $timeout_time ]; do
        if [ "$SHUTDOWN_REQUESTED" = true ]; then
            return 1
        fi
        
        if docker-compose exec -T db pg_isready -U postgres &>/dev/null; then
            log SUCCESS "✓ Database is ready"
            return 0
        fi
        
        sleep 2
    done
    
    log ERROR "Database failed to become ready within timeout period"
    return 1
}

# Start application
start_application() {
    log INFO "Starting FastAPI application..."
    
    # Activate virtual environment if it exists
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
    fi
    
    # Start uvicorn in background
    python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 > app_stdout.log 2> app_stderr.log &
    APP_PID=$!
    
    # Check if process started
    if [ -n "$APP_PID" ] && kill -0 $APP_PID 2>/dev/null; then
        log SUCCESS "✓ Application process started (PID: $APP_PID)"
        return 0
    else
        log ERROR "Failed to start application"
        return 1
    fi
}

# Wait for application
wait_for_application() {
    log INFO "Waiting for application to be ready (timeout: ${APP_STARTUP_TIMEOUT}s)..."
    
    local start_time=$(date +%s)
    local timeout_time=$((start_time + APP_STARTUP_TIMEOUT))
    
    while [ $(date +%s) -lt $timeout_time ]; do
        if [ "$SHUTDOWN_REQUESTED" = true ]; then
            return 1
        fi
        
        # Check if process is still running
        if [ -n "$APP_PID" ] && ! kill -0 $APP_PID 2>/dev/null; then
            log ERROR "Application process terminated unexpectedly"
            if [ -f "app_stderr.log" ]; then
                log ERROR "Last 20 lines of stderr:"
                tail -20 app_stderr.log | while read line; do
                    log ERROR "  $line"
                done
            fi
            return 1
        fi
        
        # Try health check
        if curl -s -f "$HEALTH_CHECK_URL" -o /dev/null 2>/dev/null; then
            log SUCCESS "✓ Application is ready and healthy"
            log SUCCESS "  API Documentation: $API_DOCS_URL"
            return 0
        fi
        
        sleep 2
    done
    
    log ERROR "Application failed to become ready within timeout period"
    return 1
}

# Monitor application
monitor_application() {
    log INFO "Monitoring application health..."
    local consecutive_failures=0
    
    while [ "$SHUTDOWN_REQUESTED" = false ]; do
        sleep 10
        
        # Check if process is still running
        if [ -n "$APP_PID" ] && ! kill -0 $APP_PID 2>/dev/null; then
            log ERROR "Application process has terminated"
            return 1
        fi
        
        # Health check
        if curl -s -f "$HEALTH_CHECK_URL" -o /dev/null 2>/dev/null; then
            consecutive_failures=0
        else
            consecutive_failures=$((consecutive_failures + 1))
            log WARNING "Health check failed (failures: $consecutive_failures)"
        fi
        
        # Restart if too many failures
        if [ $consecutive_failures -ge 3 ]; then
            log ERROR "Too many consecutive health check failures"
            return 1
        fi
    done
    
    return 0
}

# Stop application
stop_application() {
    if [ -n "$APP_PID" ]; then
        log INFO "Stopping application..."
        
        # Try graceful shutdown
        if kill -TERM $APP_PID 2>/dev/null; then
            # Wait up to 10 seconds for graceful shutdown
            local count=0
            while kill -0 $APP_PID 2>/dev/null && [ $count -lt 10 ]; do
                sleep 1
                count=$((count + 1))
            done
            
            # Force kill if still running
            if kill -0 $APP_PID 2>/dev/null; then
                log WARNING "Application didn't stop gracefully, forcing..."
                kill -KILL $APP_PID 2>/dev/null
            fi
        fi
        
        log SUCCESS "✓ Application stopped"
        APP_PID=""
    fi
}

# Stop database
stop_database() {
    log INFO "Stopping database..."
    if docker-compose down 2>&1 | tee -a "$LOG_FILE" >/dev/null; then
        log SUCCESS "✓ Database stopped"
    else
        log ERROR "Error stopping database"
    fi
}

# Cleanup
cleanup() {
    log INFO "Cleaning up..."
    stop_application
    
    if [ "$STOP_DB_ON_EXIT" = true ]; then
        stop_database
    else
        log INFO "Database left running (use --stop-db to stop it)"
    fi
    
    log SUCCESS "✓ Cleanup complete"
}

# Main execution
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --max-restarts)
                MAX_RESTART_ATTEMPTS="$2"
                shift 2
                ;;
            --db-timeout)
                DB_STARTUP_TIMEOUT="$2"
                shift 2
                ;;
            --app-timeout)
                APP_STARTUP_TIMEOUT="$2"
                shift 2
                ;;
            --restart-delay)
                RESTART_DELAY="$2"
                shift 2
                ;;
            --stop-db)
                STOP_DB_ON_EXIT=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --max-restarts N    Maximum restart attempts (default: 3)"
                echo "  --db-timeout N      Database startup timeout in seconds (default: 60)"
                echo "  --app-timeout N     Application startup timeout in seconds (default: 30)"
                echo "  --restart-delay N   Delay between restarts in seconds (default: 5)"
                echo "  --stop-db           Stop database on exit (default: false)"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                log ERROR "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    log INFO "============================================================"
    log INFO "External Data Ingestion Service - Startup Script"
    log INFO "============================================================"
    
    # Check prerequisites
    if ! check_prerequisites; then
        log ERROR "Prerequisites check failed. Exiting."
        return 1
    fi
    
    # Start database
    if ! start_database; then
        log ERROR "Failed to start database. Exiting."
        return 1
    fi
    
    # Wait for database
    if ! wait_for_database; then
        log ERROR "Database not ready. Exiting."
        cleanup
        return 1
    fi
    
    # Application restart loop
    RESTART_ATTEMPT=0
    
    while [ $RESTART_ATTEMPT -lt $MAX_RESTART_ATTEMPTS ] && [ "$SHUTDOWN_REQUESTED" = false ]; do
        if [ $RESTART_ATTEMPT -gt 0 ]; then
            log WARNING "Restart attempt $RESTART_ATTEMPT/$MAX_RESTART_ATTEMPTS"
            log INFO "Waiting $RESTART_DELAY seconds before restart..."
            sleep $RESTART_DELAY
        fi
        
        # Start application
        if ! start_application; then
            RESTART_ATTEMPT=$((RESTART_ATTEMPT + 1))
            continue
        fi
        
        # Wait for application to be ready
        if ! wait_for_application; then
            stop_application
            RESTART_ATTEMPT=$((RESTART_ATTEMPT + 1))
            continue
        fi
        
        log SUCCESS "============================================================"
        log SUCCESS "✓ SERVICE RUNNING"
        log SUCCESS "============================================================"
        log SUCCESS "API Server: http://localhost:8000"
        log SUCCESS "API Docs: $API_DOCS_URL"
        log SUCCESS "Press Ctrl+C to stop"
        log SUCCESS "============================================================"
        
        # Monitor application
        if ! monitor_application; then
            stop_application
            RESTART_ATTEMPT=$((RESTART_ATTEMPT + 1))
            continue
        fi
        
        # If we get here, shutdown was requested
        break
    done
    
    if [ $RESTART_ATTEMPT -ge $MAX_RESTART_ATTEMPTS ]; then
        log ERROR "Maximum restart attempts ($MAX_RESTART_ATTEMPTS) reached. Giving up."
    fi
    
    # Cleanup
    cleanup
    
    log INFO "Service stopped"
    return 0
}

# Run main function
main "$@"
exit $?


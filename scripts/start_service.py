#!/usr/bin/env python3
"""
Robust startup script for the External Data Ingestion Service.

Features:
- Starts PostgreSQL via Docker Compose
- Waits for database readiness with timeout
- Starts FastAPI application with health checks
- Auto-restarts on failures
- Graceful shutdown handling
"""

import os
import sys
import time
import signal
import subprocess
import logging
import requests
from typing import Optional
from pathlib import Path

# Configuration
DB_STARTUP_TIMEOUT = 60  # seconds
DB_HEALTH_CHECK_INTERVAL = 2  # seconds
APP_STARTUP_TIMEOUT = 30  # seconds
APP_HEALTH_CHECK_INTERVAL = 2  # seconds
APP_RESTART_DELAY = 5  # seconds
MAX_RESTART_ATTEMPTS = 3
HEALTH_CHECK_URL = "http://localhost:8001/health"
API_DOCS_URL = "http://localhost:8001/docs"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('service_startup.log')
    ]
)
logger = logging.getLogger(__name__)

# Global process references
db_process: Optional[subprocess.Popen] = None
app_process: Optional[subprocess.Popen] = None
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


def check_prerequisites():
    """Check that required tools and files exist."""
    logger.info("Checking prerequisites...")
    
    # Check for docker-compose
    try:
        result = subprocess.run(
            ["docker-compose", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            logger.error("docker-compose not found or not working")
            return False
        logger.info(f"✓ Docker Compose: {result.stdout.strip()}")
    except (subprocess.TimeoutExpired, FileNotFoundError):
        logger.error("docker-compose not available")
        return False
    
    # Check for docker-compose.yml
    if not Path("docker-compose.yml").exists():
        logger.error("docker-compose.yml not found in current directory")
        return False
    logger.info("✓ docker-compose.yml found")
    
    # Check for app/main.py
    if not Path("app/main.py").exists():
        logger.error("app/main.py not found")
        return False
    logger.info("✓ app/main.py found")
    
    # Check for requirements.txt
    if not Path("requirements.txt").exists():
        logger.warning("requirements.txt not found")
    else:
        logger.info("✓ requirements.txt found")
    
    return True


def start_database():
    """Start PostgreSQL database via Docker Compose."""
    global db_process
    
    logger.info("Starting PostgreSQL database...")
    
    try:
        # Start docker-compose in detached mode
        result = subprocess.run(
            ["docker-compose", "up", "-d", "db"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.error(f"Failed to start database: {result.stderr}")
            return False
        
        logger.info("✓ Database container started")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Database startup timed out")
        return False
    except Exception as e:
        logger.error(f"Error starting database: {e}")
        return False


def wait_for_database():
    """Wait for database to be ready."""
    logger.info(f"Waiting for database to be ready (timeout: {DB_STARTUP_TIMEOUT}s)...")
    
    start_time = time.time()
    
    while time.time() - start_time < DB_STARTUP_TIMEOUT:
        if shutdown_requested:
            return False
        
        try:
            # Try to connect using docker-compose exec
            result = subprocess.run(
                ["docker-compose", "exec", "-T", "db", "pg_isready", "-U", "postgres"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                logger.info("✓ Database is ready")
                return True
                
        except (subprocess.TimeoutExpired, Exception) as e:
            pass
        
        time.sleep(DB_HEALTH_CHECK_INTERVAL)
    
    logger.error("Database failed to become ready within timeout period")
    return False


def start_application():
    """Start the FastAPI application."""
    global app_process
    
    logger.info("Starting FastAPI application...")
    
    try:
        # Start uvicorn
        app_process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        logger.info(f"✓ Application process started (PID: {app_process.pid})")
        return True
        
    except Exception as e:
        logger.error(f"Error starting application: {e}")
        return False


def wait_for_application():
    """Wait for application to be ready by checking health endpoint."""
    logger.info(f"Waiting for application to be ready (timeout: {APP_STARTUP_TIMEOUT}s)...")
    
    start_time = time.time()
    
    while time.time() - start_time < APP_STARTUP_TIMEOUT:
        if shutdown_requested:
            return False
        
        # Check if process is still running
        if app_process and app_process.poll() is not None:
            logger.error("Application process terminated unexpectedly")
            # Log stderr
            if app_process.stderr:
                stderr = app_process.stderr.read()
                if stderr:
                    logger.error(f"Application stderr: {stderr}")
            return False
        
        try:
            # Try to connect to health endpoint
            response = requests.get(HEALTH_CHECK_URL, timeout=2)
            if response.status_code == 200:
                logger.info("✓ Application is ready and healthy")
                logger.info(f"  API Documentation: {API_DOCS_URL}")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(APP_HEALTH_CHECK_INTERVAL)
    
    logger.error("Application failed to become ready within timeout period")
    return False


def monitor_application():
    """Monitor application health and restart if needed."""
    logger.info("Monitoring application health...")
    consecutive_failures = 0
    
    while not shutdown_requested:
        time.sleep(10)  # Check every 10 seconds
        
        # Check if process is still running
        if app_process and app_process.poll() is not None:
            logger.error("Application process has terminated")
            return False
        
        try:
            # Health check
            response = requests.get(HEALTH_CHECK_URL, timeout=5)
            if response.status_code == 200:
                consecutive_failures = 0
                logger.debug("Health check passed")
            else:
                consecutive_failures += 1
                logger.warning(f"Health check failed with status {response.status_code} (failures: {consecutive_failures})")
        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            logger.warning(f"Health check failed: {e} (failures: {consecutive_failures})")
        
        # Restart if too many failures
        if consecutive_failures >= 3:
            logger.error("Too many consecutive health check failures")
            return False
    
    return True


def stop_application():
    """Stop the application gracefully."""
    global app_process
    
    if app_process:
        logger.info("Stopping application...")
        try:
            app_process.terminate()
            app_process.wait(timeout=10)
            logger.info("✓ Application stopped")
        except subprocess.TimeoutExpired:
            logger.warning("Application didn't stop gracefully, forcing...")
            app_process.kill()
            app_process.wait()
        except Exception as e:
            logger.error(f"Error stopping application: {e}")
        finally:
            app_process = None


def stop_database():
    """Stop the database."""
    logger.info("Stopping database...")
    try:
        subprocess.run(
            ["docker-compose", "down"],
            capture_output=True,
            text=True,
            timeout=30
        )
        logger.info("✓ Database stopped")
    except Exception as e:
        logger.error(f"Error stopping database: {e}")


def cleanup():
    """Clean up resources."""
    logger.info("Cleaning up...")
    stop_application()
    # Note: We don't stop the database by default, as it may contain data
    # Uncomment the next line if you want to stop the database on exit
    # stop_database()
    logger.info("✓ Cleanup complete")


def main():
    """Main execution function."""
    global shutdown_requested
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("=" * 60)
    logger.info("External Data Ingestion Service - Startup Script")
    logger.info("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        logger.error("Prerequisites check failed. Exiting.")
        return 1
    
    # Start database
    if not start_database():
        logger.error("Failed to start database. Exiting.")
        return 1
    
    # Wait for database
    if not wait_for_database():
        logger.error("Database not ready. Exiting.")
        cleanup()
        return 1
    
    # Application restart loop
    restart_attempt = 0
    
    while restart_attempt < MAX_RESTART_ATTEMPTS and not shutdown_requested:
        if restart_attempt > 0:
            logger.info(f"Restart attempt {restart_attempt}/{MAX_RESTART_ATTEMPTS}")
            logger.info(f"Waiting {APP_RESTART_DELAY} seconds before restart...")
            time.sleep(APP_RESTART_DELAY)
        
        # Start application
        if not start_application():
            restart_attempt += 1
            continue
        
        # Wait for application to be ready
        if not wait_for_application():
            stop_application()
            restart_attempt += 1
            continue
        
        logger.info("=" * 60)
        logger.info("✓ SERVICE RUNNING")
        logger.info("=" * 60)
        logger.info(f"API Server: http://localhost:8001")
        logger.info(f"API Docs: {API_DOCS_URL}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        # Monitor application
        if not monitor_application():
            stop_application()
            restart_attempt += 1
            continue
        
        # If we get here, shutdown was requested
        break
    
    if restart_attempt >= MAX_RESTART_ATTEMPTS:
        logger.error(f"Maximum restart attempts ({MAX_RESTART_ATTEMPTS}) reached. Giving up.")
    
    # Cleanup
    cleanup()
    
    logger.info("Service stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())


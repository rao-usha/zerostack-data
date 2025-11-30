"""
Main FastAPI application.

Source-agnostic entry point that routes to appropriate adapters.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.database import create_tables
from app.api.v1 import jobs, census_geo, census_batch, metadata, fred, eia, sec, realestate, geojson

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.
    
    Runs on startup and shutdown.
    """
    # Startup
    settings = get_settings()
    logger.info(f"Starting External Data Ingestion Service")
    logger.info(f"Log level: {settings.log_level}")
    logger.info(f"Max concurrency: {settings.max_concurrency}")
    
    # Create core tables
    try:
        create_tables()
        logger.info("Database tables ready")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down")


# Create FastAPI app
app = FastAPI(
    title="External Data Ingestion Service",
    description="Multi-source data ingestion service for public data providers",
    version="0.1.0",
    lifespan=lifespan
)

# CORS middleware (configure as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(census_geo.router, prefix="/api/v1")
app.include_router(census_batch.router, prefix="/api/v1")
app.include_router(metadata.router, prefix="/api/v1")
app.include_router(geojson.router, prefix="/api/v1")
app.include_router(fred.router, prefix="/api/v1")
app.include_router(eia.router, prefix="/api/v1")
app.include_router(sec.router, prefix="/api/v1")
app.include_router(realestate.router, prefix="/api/v1")


@app.get("/")
def root():
    """Root endpoint with service info."""
    return {
        "service": "External Data Ingestion Service",
        "version": "0.1.0",
        "sources": ["census", "fred", "eia", "sec", "realestate"],  # Update as sources are added
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    
    Returns status of the service and database connectivity.
    """
    from app.core.database import get_engine
    from sqlalchemy import text
    
    health_status = {
        "status": "healthy",
        "service": "running",
        "database": "unknown"
    }
    
    # Check database connectivity
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["database"] = f"error: {str(e)}"
        logger.warning(f"Database health check failed: {e}")
    
    return health_status



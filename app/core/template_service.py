"""
Bulk Ingestion Template Service.

Provides template management and execution for multi-source data ingestion.
"""
import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session

from app.core.models import (
    IngestionTemplate, TemplateExecution, TemplateCategory,
    IngestionJob, JobStatus, JobDependency, DependencyCondition
)

logger = logging.getLogger(__name__)


# =============================================================================
# Pre-built Templates
# =============================================================================

BUILTIN_TEMPLATES = {
    "demographics_snapshot": {
        "display_name": "Demographics Snapshot",
        "description": "Collect key demographic data from Census API including population, income, and housing statistics.",
        "category": TemplateCategory.DEMOGRAPHICS,
        "tags": ["census", "population", "income", "housing"],
        "jobs_definition": [
            {
                "source": "census",
                "config": {
                    "dataset": "acs/acs5",
                    "year": "{{year}}",
                    "variables": ["B01003_001E", "B19013_001E", "B25077_001E"],
                    "geography": "state:*"
                }
            }
        ],
        "variables": {
            "year": {"type": "integer", "default": 2022, "description": "Census data year"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "economic_indicators": {
        "display_name": "Economic Indicators Bundle",
        "description": "Collect key economic indicators from FRED, BEA, and BLS.",
        "category": TemplateCategory.ECONOMIC,
        "tags": ["gdp", "unemployment", "inflation", "fed"],
        "jobs_definition": [
            {
                "source": "fred",
                "config": {
                    "series_id": "GDP",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "fred",
                "config": {
                    "series_id": "UNRATE",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "fred",
                "config": {
                    "series_id": "CPIAUCSL",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "bea",
                "config": {
                    "dataset": "NIPA",
                    "table_name": "T10101",
                    "year": "{{year}}"
                }
            }
        ],
        "variables": {
            "start_date": {"type": "string", "default": "2020-01-01", "description": "Start date (YYYY-MM-DD)"},
            "end_date": {"type": "string", "default": "2024-01-01", "description": "End date (YYYY-MM-DD)"},
            "year": {"type": "integer", "default": 2023, "description": "Year for annual data"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "energy_markets": {
        "display_name": "Energy Markets Data",
        "description": "Collect energy data from EIA including prices, production, and consumption.",
        "category": TemplateCategory.ENERGY,
        "tags": ["oil", "gas", "electricity", "prices"],
        "jobs_definition": [
            {
                "source": "eia",
                "config": {
                    "route": "petroleum/pri/spt",
                    "frequency": "daily",
                    "start": "{{start_date}}",
                    "end": "{{end_date}}"
                }
            },
            {
                "source": "eia",
                "config": {
                    "route": "natural-gas/pri/sum",
                    "frequency": "monthly",
                    "start": "{{start_date}}",
                    "end": "{{end_date}}"
                }
            },
            {
                "source": "eia",
                "config": {
                    "route": "electricity/retail-sales",
                    "frequency": "monthly",
                    "start": "{{start_date}}",
                    "end": "{{end_date}}"
                }
            }
        ],
        "variables": {
            "start_date": {"type": "string", "default": "2023-01-01", "description": "Start date"},
            "end_date": {"type": "string", "default": "2024-01-01", "description": "End date"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "financial_sec_filings": {
        "display_name": "SEC Financial Filings",
        "description": "Collect SEC filings for a company including 10-K, 10-Q, and 8-K forms.",
        "category": TemplateCategory.FINANCIAL,
        "tags": ["sec", "10k", "10q", "filings"],
        "jobs_definition": [
            {
                "source": "sec",
                "config": {
                    "cik": "{{cik}}",
                    "form_type": "10-K",
                    "limit": "{{limit}}"
                }
            },
            {
                "source": "sec",
                "config": {
                    "cik": "{{cik}}",
                    "form_type": "10-Q",
                    "limit": "{{limit}}"
                }
            },
            {
                "source": "sec",
                "config": {
                    "cik": "{{cik}}",
                    "form_type": "8-K",
                    "limit": "{{limit}}"
                }
            }
        ],
        "variables": {
            "cik": {"type": "string", "required": True, "description": "Company CIK number"},
            "limit": {"type": "integer", "default": 10, "description": "Number of filings per type"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "healthcare_cms_data": {
        "display_name": "CMS Healthcare Data",
        "description": "Collect CMS healthcare data including provider information and quality metrics.",
        "category": TemplateCategory.HEALTHCARE,
        "tags": ["cms", "medicare", "providers", "quality"],
        "jobs_definition": [
            {
                "source": "cms",
                "config": {
                    "dataset": "provider-data",
                    "state": "{{state}}"
                }
            },
            {
                "source": "cms",
                "config": {
                    "dataset": "hospital-compare",
                    "state": "{{state}}"
                }
            }
        ],
        "variables": {
            "state": {"type": "string", "default": "CA", "description": "State code (e.g., CA, NY, TX)"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "trade_statistics": {
        "display_name": "International Trade Statistics",
        "description": "Collect trade data from Census and BTS including imports, exports, and freight.",
        "category": TemplateCategory.TRADE,
        "tags": ["imports", "exports", "freight", "trade"],
        "jobs_definition": [
            {
                "source": "census",
                "config": {
                    "dataset": "timeseries/intltrade/imports/hs",
                    "year": "{{year}}",
                    "month": "{{month}}"
                }
            },
            {
                "source": "census",
                "config": {
                    "dataset": "timeseries/intltrade/exports/hs",
                    "year": "{{year}}",
                    "month": "{{month}}"
                }
            },
            {
                "source": "bts",
                "config": {
                    "dataset": "freight",
                    "year": "{{year}}"
                }
            }
        ],
        "variables": {
            "year": {"type": "integer", "default": 2023, "description": "Trade data year"},
            "month": {"type": "integer", "default": 12, "description": "Trade data month (1-12)"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "real_estate_market": {
        "display_name": "Real Estate Market Data",
        "description": "Collect real estate data from Census and FRED including housing starts, prices, and permits.",
        "category": TemplateCategory.REAL_ESTATE,
        "tags": ["housing", "permits", "prices", "construction"],
        "jobs_definition": [
            {
                "source": "fred",
                "config": {
                    "series_id": "HOUST",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "fred",
                "config": {
                    "series_id": "CSUSHPISA",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "census",
                "config": {
                    "dataset": "acs/acs5",
                    "year": "{{year}}",
                    "variables": ["B25077_001E", "B25064_001E"],
                    "geography": "state:*"
                }
            }
        ],
        "variables": {
            "start_date": {"type": "string", "default": "2020-01-01", "description": "Start date"},
            "end_date": {"type": "string", "default": "2024-01-01", "description": "End date"},
            "year": {"type": "integer", "default": 2022, "description": "Census ACS year"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "weather_climate": {
        "display_name": "Weather and Climate Data",
        "description": "Collect weather and climate data from NOAA for specified stations.",
        "category": TemplateCategory.CUSTOM,
        "tags": ["weather", "climate", "noaa", "temperature"],
        "jobs_definition": [
            {
                "source": "noaa",
                "config": {
                    "dataset": "GHCND",
                    "station_id": "{{station_id}}",
                    "start_date": "{{start_date}}",
                    "end_date": "{{end_date}}",
                    "data_types": ["TMAX", "TMIN", "PRCP"]
                }
            }
        ],
        "variables": {
            "station_id": {"type": "string", "default": "GHCND:USW00094728", "description": "NOAA station ID"},
            "start_date": {"type": "string", "default": "2023-01-01", "description": "Start date"},
            "end_date": {"type": "string", "default": "2024-01-01", "description": "End date"}
        },
        "use_chain": False,
        "parallel_execution": True
    },
    "sequential_analysis_pipeline": {
        "display_name": "Sequential Analysis Pipeline",
        "description": "Example chained template: First collect GDP data, then collect related employment data.",
        "category": TemplateCategory.ECONOMIC,
        "tags": ["pipeline", "sequential", "gdp", "employment"],
        "jobs_definition": [
            {
                "source": "fred",
                "config": {
                    "series_id": "GDP",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "fred",
                "config": {
                    "series_id": "PAYEMS",
                    "observation_start": "{{start_date}}",
                    "observation_end": "{{end_date}}"
                }
            },
            {
                "source": "bls",
                "config": {
                    "series_id": "LNS14000000",
                    "start_year": "{{year}}",
                    "end_year": "{{year}}"
                }
            }
        ],
        "variables": {
            "start_date": {"type": "string", "default": "2020-01-01", "description": "Start date"},
            "end_date": {"type": "string", "default": "2024-01-01", "description": "End date"},
            "year": {"type": "integer", "default": 2023, "description": "BLS data year"}
        },
        "use_chain": True,
        "parallel_execution": False
    }
}


# =============================================================================
# Variable Substitution
# =============================================================================

def substitute_variables(config: Any, variables: Dict[str, Any]) -> Any:
    """
    Recursively substitute {{variable}} placeholders in config.

    Args:
        config: Configuration value (dict, list, string, or primitive)
        variables: Dictionary of variable values

    Returns:
        Config with variables substituted
    """
    if isinstance(config, str):
        # Find all {{variable}} patterns
        pattern = r"\{\{(\w+)\}\}"
        matches = re.findall(pattern, config)

        result = config
        for var_name in matches:
            if var_name in variables:
                value = variables[var_name]
                placeholder = "{{" + var_name + "}}"

                # If the entire string is just the placeholder, return the value directly
                # This preserves types (int, bool, etc.)
                if result == placeholder:
                    return value

                # Otherwise do string substitution
                result = result.replace(placeholder, str(value))

        return result

    elif isinstance(config, dict):
        return {k: substitute_variables(v, variables) for k, v in config.items()}

    elif isinstance(config, list):
        return [substitute_variables(item, variables) for item in config]

    else:
        return config


def validate_variables(
    template: IngestionTemplate,
    provided_variables: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validate and merge provided variables with template defaults.

    Args:
        template: The ingestion template
        provided_variables: Variables provided by the user

    Returns:
        Tuple of (merged variables, list of errors)
    """
    errors = []
    merged = {}

    variable_definitions = template.variables or {}

    # Apply defaults and validate required variables
    for var_name, var_def in variable_definitions.items():
        if var_name in provided_variables:
            value = provided_variables[var_name]

            # Type validation
            expected_type = var_def.get("type", "string")
            if expected_type == "integer" and not isinstance(value, int):
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    errors.append(f"Variable '{var_name}' must be an integer")
                    continue
            elif expected_type == "float" and not isinstance(value, (int, float)):
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    errors.append(f"Variable '{var_name}' must be a number")
                    continue
            elif expected_type == "boolean" and not isinstance(value, bool):
                if isinstance(value, str):
                    value = value.lower() in ("true", "1", "yes")
                else:
                    value = bool(value)

            merged[var_name] = value

        elif "default" in var_def:
            merged[var_name] = var_def["default"]

        elif var_def.get("required", False):
            errors.append(f"Required variable '{var_name}' is missing")

    # Warn about unknown variables
    for var_name in provided_variables:
        if var_name not in variable_definitions:
            logger.warning(f"Unknown variable '{var_name}' provided (will be ignored)")

    return merged, errors


# =============================================================================
# Template Service
# =============================================================================

class TemplateService:
    """Service for managing and executing ingestion templates."""

    def __init__(self, db: Session):
        self.db = db

    def get_template(self, name: str) -> Optional[IngestionTemplate]:
        """Get a template by name."""
        return self.db.query(IngestionTemplate).filter(
            IngestionTemplate.name == name
        ).first()

    def list_templates(
        self,
        category: Optional[TemplateCategory] = None,
        tags: Optional[List[str]] = None,
        enabled_only: bool = True
    ) -> List[IngestionTemplate]:
        """List templates with optional filtering."""
        query = self.db.query(IngestionTemplate)

        if enabled_only:
            query = query.filter(IngestionTemplate.is_enabled == 1)

        if category:
            query = query.filter(IngestionTemplate.category == category)

        templates = query.order_by(IngestionTemplate.name).all()

        # Filter by tags if specified
        if tags:
            templates = [
                t for t in templates
                if t.tags and any(tag in t.tags for tag in tags)
            ]

        return templates

    def create_template(
        self,
        name: str,
        jobs_definition: List[Dict],
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        category: TemplateCategory = TemplateCategory.CUSTOM,
        tags: Optional[List[str]] = None,
        variables: Optional[Dict] = None,
        use_chain: bool = False,
        parallel_execution: bool = True
    ) -> IngestionTemplate:
        """Create a new template."""
        template = IngestionTemplate(
            name=name,
            display_name=display_name or name,
            description=description,
            category=category,
            tags=tags,
            jobs_definition=jobs_definition,
            variables=variables,
            use_chain=1 if use_chain else 0,
            parallel_execution=1 if parallel_execution else 0,
            is_builtin=0,
            is_enabled=1
        )

        self.db.add(template)
        self.db.commit()
        self.db.refresh(template)

        return template

    def update_template(
        self,
        name: str,
        **updates
    ) -> Optional[IngestionTemplate]:
        """Update an existing template."""
        template = self.get_template(name)
        if not template:
            return None

        # Don't allow updating builtin templates
        if template.is_builtin:
            raise ValueError("Cannot modify built-in templates")

        allowed_fields = {
            "display_name", "description", "category", "tags",
            "jobs_definition", "variables", "use_chain",
            "parallel_execution", "is_enabled"
        }

        for field, value in updates.items():
            if field in allowed_fields:
                if field in ("use_chain", "parallel_execution", "is_enabled"):
                    value = 1 if value else 0
                setattr(template, field, value)

        self.db.commit()
        self.db.refresh(template)

        return template

    def delete_template(self, name: str) -> bool:
        """Delete a template."""
        template = self.get_template(name)
        if not template:
            return False

        if template.is_builtin:
            raise ValueError("Cannot delete built-in templates")

        self.db.delete(template)
        self.db.commit()
        return True

    def execute_template(
        self,
        name: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> TemplateExecution:
        """
        Execute a template, creating ingestion jobs.

        Args:
            name: Template name
            variables: Variable values for substitution

        Returns:
            TemplateExecution tracking object
        """
        template = self.get_template(name)
        if not template:
            raise ValueError(f"Template not found: {name}")

        if not template.is_enabled:
            raise ValueError(f"Template is disabled: {name}")

        # Validate and merge variables
        merged_vars, errors = validate_variables(template, variables or {})
        if errors:
            raise ValueError(f"Variable validation failed: {'; '.join(errors)}")

        # Substitute variables in job definitions
        jobs_config = substitute_variables(template.jobs_definition, merged_vars)

        # Create execution tracking record
        execution = TemplateExecution(
            template_id=template.id,
            template_name=template.name,
            parameters=merged_vars,
            status="running",
            job_ids=[],
            total_jobs=len(jobs_config),
            completed_jobs=0,
            successful_jobs=0,
            failed_jobs=0
        )
        self.db.add(execution)
        self.db.flush()  # Get execution ID

        job_ids = []

        try:
            if template.use_chain:
                # Create jobs as a sequential chain with dependencies
                prev_job_id = None
                for i, job_config in enumerate(jobs_config):
                    # First job is PENDING, subsequent jobs are BLOCKED
                    initial_status = JobStatus.BLOCKED if i > 0 else JobStatus.PENDING

                    job = IngestionJob(
                        source=job_config["source"],
                        config=job_config.get("config", {}),
                        status=initial_status
                    )
                    self.db.add(job)
                    self.db.flush()
                    job_ids.append(job.id)

                    # Create dependency on previous job
                    if prev_job_id is not None:
                        dependency = JobDependency(
                            job_id=job.id,
                            depends_on_job_id=prev_job_id,
                            condition=DependencyCondition.ON_SUCCESS
                        )
                        self.db.add(dependency)

                    prev_job_id = job.id

            else:
                # Create independent jobs (parallel execution)
                for job_config in jobs_config:
                    job = IngestionJob(
                        source=job_config["source"],
                        config=job_config.get("config", {}),
                        status=JobStatus.PENDING
                    )
                    self.db.add(job)
                    self.db.flush()
                    job_ids.append(job.id)

            execution.job_ids = job_ids

            # Update template usage stats
            template.times_executed += 1
            template.last_executed_at = datetime.utcnow()

            self.db.commit()
            self.db.refresh(execution)

            logger.info(f"Executed template '{name}' - created {len(job_ids)} jobs")
            return execution

        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to execute template '{name}': {e}")
            raise

    def get_execution(self, execution_id: int) -> Optional[TemplateExecution]:
        """Get a template execution by ID."""
        return self.db.query(TemplateExecution).filter(
            TemplateExecution.id == execution_id
        ).first()

    def list_executions(
        self,
        template_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[TemplateExecution]:
        """List template executions."""
        query = self.db.query(TemplateExecution)

        if template_name:
            query = query.filter(TemplateExecution.template_name == template_name)

        if status:
            query = query.filter(TemplateExecution.status == status)

        return query.order_by(TemplateExecution.started_at.desc()).limit(limit).all()

    def update_execution_status(self, execution_id: int) -> Optional[TemplateExecution]:
        """
        Update execution status based on job statuses.

        Calculates completed/successful/failed counts from actual job states.
        """
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        if not execution.job_ids:
            return execution

        # Query job statuses
        jobs = self.db.query(IngestionJob).filter(
            IngestionJob.id.in_(execution.job_ids)
        ).all()

        completed = 0
        successful = 0
        failed = 0

        for job in jobs:
            if job.status in (JobStatus.SUCCESS, JobStatus.FAILED):
                completed += 1
                if job.status == JobStatus.SUCCESS:
                    successful += 1
                else:
                    failed += 1

        execution.completed_jobs = completed
        execution.successful_jobs = successful
        execution.failed_jobs = failed

        # Determine overall status
        if completed == execution.total_jobs:
            if failed == 0:
                execution.status = "completed"
            elif successful == 0:
                execution.status = "failed"
            else:
                execution.status = "partial"
            execution.completed_at = datetime.utcnow()
        elif failed > 0:
            execution.status = "running_with_errors"

        self.db.commit()
        self.db.refresh(execution)

        return execution


# =============================================================================
# Initialization
# =============================================================================

def init_builtin_templates(db: Session) -> int:
    """
    Initialize database with built-in templates.

    Creates templates that don't already exist.

    Returns:
        Number of templates created
    """
    created = 0

    for name, config in BUILTIN_TEMPLATES.items():
        existing = db.query(IngestionTemplate).filter(
            IngestionTemplate.name == name
        ).first()

        if not existing:
            template = IngestionTemplate(
                name=name,
                display_name=config["display_name"],
                description=config["description"],
                category=config["category"],
                tags=config["tags"],
                jobs_definition=config["jobs_definition"],
                variables=config["variables"],
                use_chain=1 if config["use_chain"] else 0,
                parallel_execution=1 if config["parallel_execution"] else 0,
                is_builtin=1,
                is_enabled=1
            )
            db.add(template)
            created += 1
            logger.info(f"Created built-in template: {name}")

    if created > 0:
        db.commit()

    return created

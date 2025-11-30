"""
Unit tests for database models.
"""
import pytest
from datetime import datetime
from app.core.models import IngestionJob, DatasetRegistry, JobStatus


@pytest.mark.unit
def test_ingestion_job_creation(test_db):
    """Test creating an ingestion job."""
    job = IngestionJob(
        source="census",
        status=JobStatus.PENDING,
        config={"survey": "acs5", "year": 2023, "table": "B01001"}
    )
    
    test_db.add(job)
    test_db.commit()
    test_db.refresh(job)
    
    assert job.id is not None
    assert job.source == "census"
    assert job.status == JobStatus.PENDING
    assert job.config["survey"] == "acs5"
    assert isinstance(job.created_at, datetime)


@pytest.mark.unit
def test_ingestion_job_status_enum(test_db):
    """Test job status enumeration."""
    # All valid statuses
    for status in [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.FAILED]:
        job = IngestionJob(
            source="test",
            status=status,
            config={}
        )
        test_db.add(job)
        test_db.commit()
        test_db.refresh(job)
        assert job.status == status


@pytest.mark.unit
def test_dataset_registry_creation(test_db):
    """Test creating a dataset registry entry."""
    dataset = DatasetRegistry(
        source="census",
        dataset_id="acs5_2023_b01001",
        table_name="acs5_2023_b01001",
        display_name="ACS 5-Year 2023 - Sex by Age",
        description="Detailed age and sex data from ACS 5-year estimates",
        metadata={"survey": "acs5", "year": 2023, "table_id": "B01001"}
    )
    
    test_db.add(dataset)
    test_db.commit()
    test_db.refresh(dataset)
    
    assert dataset.id is not None
    assert dataset.source == "census"
    assert dataset.dataset_id == "acs5_2023_b01001"
    assert isinstance(dataset.created_at, datetime)
    assert isinstance(dataset.last_updated_at, datetime)


@pytest.mark.unit
def test_dataset_registry_unique_table_name(test_db):
    """Test that table_name is unique."""
    dataset1 = DatasetRegistry(
        source="census",
        dataset_id="acs5_2023_b01001",
        table_name="acs5_2023_b01001"
    )
    test_db.add(dataset1)
    test_db.commit()
    
    # Try to add another with same table_name
    dataset2 = DatasetRegistry(
        source="census",
        dataset_id="different_id",
        table_name="acs5_2023_b01001"  # Same table name
    )
    test_db.add(dataset2)
    
    with pytest.raises(Exception):  # IntegrityError
        test_db.commit()






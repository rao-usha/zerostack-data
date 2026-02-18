"""
SEC Form ADV ingestion orchestration.

High-level functions that coordinate:
- Searching for investment advisers
- Fetching Form ADV data
- Creating database tables
- Loading data with proper job tracking
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.sec.formadv_client import FormADVClient, search_family_offices
from app.sources.sec import formadv_metadata

logger = logging.getLogger(__name__)


async def prepare_formadv_tables(db: Session) -> Dict[str, str]:
    """
    Prepare database tables for Form ADV data.

    Creates:
    - sec_form_adv (main table)
    - sec_form_adv_personnel (personnel table)

    Args:
        db: Database session

    Returns:
        Dictionary with table names
    """
    try:
        logger.info("Creating Form ADV tables")

        # Create main table
        main_table_sql = formadv_metadata.generate_create_table_sql()
        db.execute(text(main_table_sql))

        # Create personnel table
        personnel_table_sql = formadv_metadata.generate_personnel_table_sql()
        db.execute(text(personnel_table_sql))

        db.commit()

        # Register in dataset_registry
        dataset_id = "sec_form_adv"

        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == "sec_form_adv")
            .first()
        )

        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "description": "SEC Form ADV - Investment Adviser Registration",
                "includes": [
                    "business_contact",
                    "personnel",
                    "aum",
                    "registration_info",
                ],
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="sec",
                dataset_id=dataset_id,
                table_name="sec_form_adv",
                display_name="SEC Form ADV - Investment Advisers",
                description="Investment adviser registration data from SEC Form ADV, including business contact information, key personnel, assets under management, and registration details",
                source_metadata={
                    "form_type": "ADV",
                    "includes": [
                        "business_contact",
                        "personnel",
                        "aum",
                        "registration_info",
                    ],
                },
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")

        return {
            "main_table": "sec_form_adv",
            "personnel_table": "sec_form_adv_personnel",
        }

    except Exception as e:
        logger.error(f"Failed to prepare Form ADV tables: {e}")
        db.rollback()
        raise


async def ingest_family_offices(
    db: Session,
    job_id: int,
    family_office_names: List[str],
    max_concurrency: int = 1,
    max_requests_per_second: float = 2.0,
) -> Dict[str, Any]:
    """
    Ingest Form ADV data for specified family offices.

    Steps:
    1. Search for each family office by name
    2. Fetch detailed Form ADV data for each
    3. Parse and insert into database
    4. Track progress in ingestion_jobs table

    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        family_office_names: List of family office names to search
        max_concurrency: Max concurrent API requests
        max_requests_per_second: Rate limit

    Returns:
        Summary of ingestion results
    """
    client = None

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        # Initialize client
        client = FormADVClient(
            max_concurrency=max_concurrency,
            max_requests_per_second=max_requests_per_second,
        )

        # Prepare tables
        await prepare_formadv_tables(db)

        # Search for family offices
        logger.info(f"Searching for {len(family_office_names)} family offices")
        search_results = await search_family_offices(client, family_office_names)

        # Track results
        total_found = 0
        total_ingested = 0
        errors = []

        # Process each family office
        for office_name, results in search_results.items():
            logger.info(f"Processing '{office_name}': found {len(results)} matches")
            total_found += len(results)

            for result in results:
                crd_number = result.get("crd_number")
                if not crd_number:
                    logger.warning(f"No CRD number for result: {result}")
                    continue

                try:
                    # Fetch detailed data
                    firm_details = await client.get_firm_details(crd_number)

                    if not firm_details:
                        logger.warning(f"No details found for CRD {crd_number}")
                        continue

                    # Parse adviser info
                    parsed_adviser = formadv_metadata.parse_adviser_info(firm_details)

                    # Insert/update main record
                    await _upsert_adviser(db, parsed_adviser)

                    # Parse and insert personnel
                    key_personnel = firm_details.get(
                        "key_personnel"
                    ) or firm_details.get("keyPersonnel")
                    if key_personnel and isinstance(key_personnel, list):
                        parsed_personnel = formadv_metadata.parse_key_personnel(
                            crd_number, key_personnel
                        )
                        await _insert_personnel(db, parsed_personnel)

                    total_ingested += 1

                    logger.info(
                        f"Successfully ingested: {parsed_adviser.get('firm_name')} "
                        f"(CRD: {crd_number})"
                    )

                except Exception as e:
                    error_msg = f"Error ingesting CRD {crd_number}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_ingested = total_ingested
            job.metadata = {
                "searched_offices": len(family_office_names),
                "total_matches_found": total_found,
                "total_ingested": total_ingested,
                "errors": errors[:10],  # Store first 10 errors
            }
            db.commit()

        summary = {
            "searched_offices": len(family_office_names),
            "total_matches_found": total_found,
            "total_ingested": total_ingested,
            "errors": errors,
        }

        logger.info(f"Form ADV ingestion complete: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Form ADV ingestion failed: {e}")

        # Update job status to failed
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        if client:
            await client.close()


async def ingest_firm_by_crd(
    db: Session, job_id: int, crd_number: str
) -> Dict[str, Any]:
    """
    Ingest Form ADV data for a specific firm by CRD number.

    Args:
        db: Database session
        job_id: Ingestion job ID
        crd_number: Firm CRD number

    Returns:
        Ingestion result
    """
    client = None

    try:
        # Update job status
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        # Initialize client
        client = FormADVClient()

        # Prepare tables
        await prepare_formadv_tables(db)

        # Fetch firm details
        logger.info(f"Fetching Form ADV data for CRD {crd_number}")
        firm_details = await client.get_firm_details(crd_number)

        if not firm_details:
            raise Exception(f"No Form ADV data found for CRD {crd_number}")

        # Parse and insert
        parsed_adviser = formadv_metadata.parse_adviser_info(firm_details)
        await _upsert_adviser(db, parsed_adviser)

        # Parse and insert personnel
        key_personnel = firm_details.get("key_personnel") or firm_details.get(
            "keyPersonnel"
        )
        if key_personnel and isinstance(key_personnel, list):
            parsed_personnel = formadv_metadata.parse_key_personnel(
                crd_number, key_personnel
            )
            await _insert_personnel(db, parsed_personnel)

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_ingested = 1
            job.metadata = {
                "crd_number": crd_number,
                "firm_name": parsed_adviser.get("firm_name"),
            }
            db.commit()

        logger.info(f"Successfully ingested CRD {crd_number}")
        return {"crd_number": crd_number, "status": "success"}

    except Exception as e:
        logger.error(f"Failed to ingest CRD {crd_number}: {e}")

        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        if client:
            await client.close()


async def _upsert_adviser(db: Session, adviser: Dict[str, Any]):
    """
    Insert or update adviser record in database.

    Uses ON CONFLICT to update existing records.
    """
    # Convert arrays and dicts to proper format
    state_registrations = adviser.get("state_registrations") or []
    if isinstance(state_registrations, list):
        state_registrations = (
            "{" + ",".join(f'"{s}"' for s in state_registrations) + "}"
        )

    key_personnel = adviser.get("key_personnel")
    if key_personnel and not isinstance(key_personnel, str):
        key_personnel = json.dumps(key_personnel)

    sql = text("""
        INSERT INTO sec_form_adv (
            crd_number, sec_number, firm_name, legal_name, doing_business_as,
            business_address_street1, business_address_street2,
            business_address_city, business_address_state, business_address_zip,
            business_address_country, business_phone, business_fax, business_email,
            website, mailing_address_street1, mailing_address_street2,
            mailing_address_city, mailing_address_state, mailing_address_zip,
            mailing_address_country, registration_status, registration_date,
            state_registrations, is_registered_with_sec, is_registered_with_state,
            assets_under_management, aum_date, aum_currency,
            total_client_count, individual_client_count, high_net_worth_client_count,
            pooled_investment_vehicle_count, is_family_office,
            key_personnel, form_adv_url, filing_date, last_amended_date,
            last_updated_at
        ) VALUES (
            :crd_number, :sec_number, :firm_name, :legal_name, :doing_business_as,
            :business_address_street1, :business_address_street2,
            :business_address_city, :business_address_state, :business_address_zip,
            :business_address_country, :business_phone, :business_fax, :business_email,
            :website, :mailing_address_street1, :mailing_address_street2,
            :mailing_address_city, :mailing_address_state, :mailing_address_zip,
            :mailing_address_country, :registration_status, :registration_date,
            :state_registrations::text[], :is_registered_with_sec, :is_registered_with_state,
            :assets_under_management, :aum_date, :aum_currency,
            :total_client_count, :individual_client_count, :high_net_worth_client_count,
            :pooled_investment_vehicle_count, :is_family_office,
            :key_personnel::jsonb, :form_adv_url, :filing_date, :last_amended_date,
            NOW()
        )
        ON CONFLICT (crd_number) DO UPDATE SET
            sec_number = EXCLUDED.sec_number,
            firm_name = EXCLUDED.firm_name,
            legal_name = EXCLUDED.legal_name,
            doing_business_as = EXCLUDED.doing_business_as,
            business_address_street1 = EXCLUDED.business_address_street1,
            business_address_street2 = EXCLUDED.business_address_street2,
            business_address_city = EXCLUDED.business_address_city,
            business_address_state = EXCLUDED.business_address_state,
            business_address_zip = EXCLUDED.business_address_zip,
            business_address_country = EXCLUDED.business_address_country,
            business_phone = EXCLUDED.business_phone,
            business_fax = EXCLUDED.business_fax,
            business_email = EXCLUDED.business_email,
            website = EXCLUDED.website,
            mailing_address_street1 = EXCLUDED.mailing_address_street1,
            mailing_address_street2 = EXCLUDED.mailing_address_street2,
            mailing_address_city = EXCLUDED.mailing_address_city,
            mailing_address_state = EXCLUDED.mailing_address_state,
            mailing_address_zip = EXCLUDED.mailing_address_zip,
            mailing_address_country = EXCLUDED.mailing_address_country,
            registration_status = EXCLUDED.registration_status,
            registration_date = EXCLUDED.registration_date,
            state_registrations = EXCLUDED.state_registrations,
            is_registered_with_sec = EXCLUDED.is_registered_with_sec,
            is_registered_with_state = EXCLUDED.is_registered_with_state,
            assets_under_management = EXCLUDED.assets_under_management,
            aum_date = EXCLUDED.aum_date,
            aum_currency = EXCLUDED.aum_currency,
            total_client_count = EXCLUDED.total_client_count,
            individual_client_count = EXCLUDED.individual_client_count,
            high_net_worth_client_count = EXCLUDED.high_net_worth_client_count,
            pooled_investment_vehicle_count = EXCLUDED.pooled_investment_vehicle_count,
            is_family_office = EXCLUDED.is_family_office,
            key_personnel = EXCLUDED.key_personnel,
            form_adv_url = EXCLUDED.form_adv_url,
            filing_date = EXCLUDED.filing_date,
            last_amended_date = EXCLUDED.last_amended_date,
            last_updated_at = NOW()
    """)

    params = {
        **adviser,
        "state_registrations": state_registrations,
        "key_personnel": key_personnel,
    }
    db.execute(sql, params)
    db.commit()


async def _insert_personnel(db: Session, personnel_list: List[Dict[str, Any]]):
    """
    Insert personnel records into database.

    Deletes existing personnel for the firm and inserts new records.
    """
    if not personnel_list:
        return

    crd_number = personnel_list[0].get("crd_number")
    if not crd_number:
        return

    # Delete existing personnel
    delete_sql = text("""
        DELETE FROM sec_form_adv_personnel
        WHERE crd_number = :crd_number
    """)
    db.execute(delete_sql, {"crd_number": crd_number})

    # Insert new personnel
    insert_sql = text("""
        INSERT INTO sec_form_adv_personnel (
            crd_number, individual_crd_number,
            first_name, middle_name, last_name, full_name,
            title, position_type, email, phone
        ) VALUES (
            :crd_number, :individual_crd_number,
            :first_name, :middle_name, :last_name, :full_name,
            :title, :position_type, :email, :phone
        )
    """)

    for person in personnel_list:
        db.execute(insert_sql, person)

    db.commit()
    logger.info(
        f"Inserted {len(personnel_list)} personnel records for CRD {crd_number}"
    )

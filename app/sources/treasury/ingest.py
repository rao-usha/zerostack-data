"""
Treasury FiscalData ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.treasury.client import TreasuryClient, TREASURY_DATASETS
from app.sources.treasury import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_treasury_data(db: Session, dataset: str) -> Dict[str, Any]:
    """
    Prepare database table for Treasury data ingestion.

    Steps:
    1. Generate table name based on dataset
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry

    Args:
        db: Database session
        dataset: Dataset identifier (e.g., "daily_balance", "debt_outstanding")

    Returns:
        Dictionary with:
        - table_name: Generated Postgres table name

    Raises:
        Exception: On table creation errors
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(dataset)

        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for Treasury {dataset} data")
        create_sql = metadata.generate_create_table_sql(table_name, dataset)

        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()

        # 4. Register in dataset_registry
        dataset_id = f"treasury_{dataset.lower()}"

        # Check if already registered
        existing = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.table_name == table_name)
            .first()
        )

        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "dataset": dataset,
                "endpoint": TREASURY_DATASETS.get(dataset, {}).get("endpoint"),
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="treasury",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_dataset_display_name(dataset),
                description=metadata.get_dataset_description(dataset),
                source_metadata={
                    "dataset": dataset,
                    "endpoint": TREASURY_DATASETS.get(dataset, {}).get("endpoint"),
                },
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")

        return {"table_name": table_name}

    except Exception as e:
        logger.error(f"Failed to prepare table for Treasury data: {e}")
        raise


async def ingest_treasury_daily_balance(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Daily Treasury Balance data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)

    Returns:
        Dictionary with ingestion results
    """
    return await _ingest_treasury_dataset(
        db=db,
        job_id=job_id,
        dataset="daily_balance",
        fetch_func="get_daily_treasury_balance",
        start_date=start_date,
        end_date=end_date,
    )


async def ingest_treasury_debt_outstanding(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Debt Outstanding data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)

    Returns:
        Dictionary with ingestion results
    """
    return await _ingest_treasury_dataset(
        db=db,
        job_id=job_id,
        dataset="debt_outstanding",
        fetch_func="get_debt_outstanding",
        start_date=start_date,
        end_date=end_date,
    )


async def ingest_treasury_interest_rates(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    security_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Treasury Interest Rates data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        security_type: Filter by security type (optional)

    Returns:
        Dictionary with ingestion results
    """
    return await _ingest_treasury_dataset(
        db=db,
        job_id=job_id,
        dataset="interest_rates",
        fetch_func="get_interest_rates",
        start_date=start_date,
        end_date=end_date,
        extra_params={"security_type": security_type} if security_type else None,
    )


async def ingest_treasury_monthly_statement(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    classification: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Monthly Treasury Statement data into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        classification: Filter by classification (optional)

    Returns:
        Dictionary with ingestion results
    """
    return await _ingest_treasury_dataset(
        db=db,
        job_id=job_id,
        dataset="monthly_statement",
        fetch_func="get_monthly_treasury_statement",
        start_date=start_date,
        end_date=end_date,
        extra_params={"classification": classification} if classification else None,
    )


async def ingest_treasury_auctions(
    db: Session,
    job_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    security_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest Treasury Auction Results into Postgres.

    Args:
        db: Database session
        job_id: Ingestion job ID
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        security_type: Filter by security type (optional)

    Returns:
        Dictionary with ingestion results
    """
    return await _ingest_treasury_dataset(
        db=db,
        job_id=job_id,
        dataset="auctions",
        fetch_func="get_auction_results",
        start_date=start_date,
        end_date=end_date,
        extra_params={"security_type": security_type} if security_type else None,
    )


async def _ingest_treasury_dataset(
    db: Session,
    job_id: int,
    dataset: str,
    fetch_func: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generic Treasury dataset ingestion.

    Args:
        db: Database session
        job_id: Ingestion job ID
        dataset: Dataset identifier
        fetch_func: Client method name to call
        start_date: Start date
        end_date: End date
        extra_params: Additional parameters for the API call

    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()

    # Initialize Treasury client
    client = TreasuryClient(
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor,
    )

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()

        # Set default date range if not provided
        if not start_date or not end_date:
            default_start, default_end = metadata.get_default_date_range()
            start_date = start_date or default_start
            end_date = end_date or default_end

        # Validate date formats
        if not metadata.validate_date_format(start_date):
            raise ValueError(f"Invalid start date format: {start_date}. Use YYYY-MM-DD")
        if not metadata.validate_date_format(end_date):
            raise ValueError(f"Invalid end date format: {end_date}. Use YYYY-MM-DD")

        logger.info(f"Ingesting Treasury {dataset}: " f"{start_date} to {end_date}")

        # Prepare table
        table_info = await prepare_table_for_treasury_data(db, dataset)
        table_name = table_info["table_name"]

        # Fetch data from Treasury API (with pagination)
        logger.info(f"Fetching {dataset} data from Treasury API")

        all_parsed_data = []
        page_number = 1
        page_size = 10000  # Treasury API max

        # Get the fetch method
        fetch_method = getattr(client, fetch_func)

        while True:
            # Build params
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "page_size": page_size,
                "page_number": page_number,
            }

            # Add extra params if provided
            if extra_params:
                params.update(extra_params)

            api_response = await fetch_method(**params)

            # Parse data
            parsed = metadata.parse_treasury_response(api_response, dataset)
            all_parsed_data.extend(parsed)

            logger.info(f"Parsed {len(parsed)} records (page {page_number})")

            # Check if we got all data
            meta = api_response.get("meta", {})
            total_count = meta.get("total-count") or meta.get("count", 0)

            if len(parsed) < page_size:
                break

            # Check if we've fetched all records
            if len(all_parsed_data) >= int(total_count) if total_count else True:
                break

            page_number += 1

        # Insert data
        rows = metadata.build_insert_values(all_parsed_data)
        rows_inserted = 0

        if not rows:
            logger.warning("No data to insert")
        else:
            logger.info(f"Inserting {len(rows)} rows into {table_name}")

            # Get insert SQL based on dataset
            insert_sql = _get_insert_sql(table_name, dataset)

            # Execute in batches
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()

                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(rows)} rows")

            logger.info(f"Successfully inserted {rows_inserted} rows")

        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()

        return {
            "table_name": table_name,
            "dataset": dataset,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}",
        }

    except Exception as e:
        logger.error(f"Treasury {dataset} ingestion failed: {e}", exc_info=True)

        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()

        raise

    finally:
        await client.close()


def _get_insert_sql(table_name: str, dataset: str) -> str:
    """
    Get INSERT SQL statement for a dataset.

    Args:
        table_name: Target table name
        dataset: Dataset identifier

    Returns:
        INSERT SQL with ON CONFLICT handling
    """
    if dataset == "daily_balance":
        return f"""
            INSERT INTO {table_name} (
                record_date, account_type, close_today_bal, open_today_bal,
                open_month_bal, open_fiscal_year_bal, transaction_type, transaction_catg,
                transaction_catg_desc, transaction_today_amt, transaction_mtd_amt,
                transaction_fytd_amt, table_nbr, table_nm, sub_table_name, src_line_nbr,
                record_fiscal_year, record_fiscal_quarter, record_calendar_year,
                record_calendar_quarter, record_calendar_month, record_calendar_day
            ) VALUES (
                :record_date, :account_type, :close_today_bal, :open_today_bal,
                :open_month_bal, :open_fiscal_year_bal, :transaction_type, :transaction_catg,
                :transaction_catg_desc, :transaction_today_amt, :transaction_mtd_amt,
                :transaction_fytd_amt, :table_nbr, :table_nm, :sub_table_name, :src_line_nbr,
                :record_fiscal_year, :record_fiscal_quarter, :record_calendar_year,
                :record_calendar_quarter, :record_calendar_month, :record_calendar_day
            )
            ON CONFLICT (record_date, account_type, transaction_type, transaction_catg, src_line_nbr)
            DO UPDATE SET
                close_today_bal = EXCLUDED.close_today_bal,
                open_today_bal = EXCLUDED.open_today_bal,
                open_month_bal = EXCLUDED.open_month_bal,
                open_fiscal_year_bal = EXCLUDED.open_fiscal_year_bal,
                transaction_catg_desc = EXCLUDED.transaction_catg_desc,
                transaction_today_amt = EXCLUDED.transaction_today_amt,
                transaction_mtd_amt = EXCLUDED.transaction_mtd_amt,
                transaction_fytd_amt = EXCLUDED.transaction_fytd_amt,
                ingested_at = NOW()
        """

    elif dataset == "debt_outstanding":
        return f"""
            INSERT INTO {table_name} (
                record_date, debt_held_public_amt, intragov_hold_amt, tot_pub_debt_out_amt,
                src_line_nbr, record_fiscal_year, record_fiscal_quarter, record_calendar_year,
                record_calendar_quarter, record_calendar_month, record_calendar_day
            ) VALUES (
                :record_date, :debt_held_public_amt, :intragov_hold_amt, :tot_pub_debt_out_amt,
                :src_line_nbr, :record_fiscal_year, :record_fiscal_quarter, :record_calendar_year,
                :record_calendar_quarter, :record_calendar_month, :record_calendar_day
            )
            ON CONFLICT (record_date)
            DO UPDATE SET
                debt_held_public_amt = EXCLUDED.debt_held_public_amt,
                intragov_hold_amt = EXCLUDED.intragov_hold_amt,
                tot_pub_debt_out_amt = EXCLUDED.tot_pub_debt_out_amt,
                ingested_at = NOW()
        """

    elif dataset == "interest_rates":
        return f"""
            INSERT INTO {table_name} (
                record_date, security_type_desc, security_desc, avg_interest_rate_amt,
                src_line_nbr, record_fiscal_year, record_fiscal_quarter, record_calendar_year,
                record_calendar_quarter, record_calendar_month, record_calendar_day
            ) VALUES (
                :record_date, :security_type_desc, :security_desc, :avg_interest_rate_amt,
                :src_line_nbr, :record_fiscal_year, :record_fiscal_quarter, :record_calendar_year,
                :record_calendar_quarter, :record_calendar_month, :record_calendar_day
            )
            ON CONFLICT (record_date, security_type_desc, security_desc)
            DO UPDATE SET
                avg_interest_rate_amt = EXCLUDED.avg_interest_rate_amt,
                ingested_at = NOW()
        """

    elif dataset == "monthly_statement":
        return f"""
            INSERT INTO {table_name} (
                record_date, classification_desc, current_month_net_rcpt_outly_amt,
                fiscal_year_to_date_net_rcpt_outly_amt, prior_fiscal_year_to_date_net_rcpt_outly_amt,
                current_fytd_net_outly_rcpt_amt, prior_fytd_net_outly_rcpt_amt, category_desc,
                table_nbr, table_nm, sub_table_desc, src_line_nbr, record_fiscal_year,
                record_fiscal_quarter, record_calendar_year, record_calendar_quarter,
                record_calendar_month, record_calendar_day
            ) VALUES (
                :record_date, :classification_desc, :current_month_net_rcpt_outly_amt,
                :fiscal_year_to_date_net_rcpt_outly_amt, :prior_fiscal_year_to_date_net_rcpt_outly_amt,
                :current_fytd_net_outly_rcpt_amt, :prior_fytd_net_outly_rcpt_amt, :category_desc,
                :table_nbr, :table_nm, :sub_table_desc, :src_line_nbr, :record_fiscal_year,
                :record_fiscal_quarter, :record_calendar_year, :record_calendar_quarter,
                :record_calendar_month, :record_calendar_day
            )
            ON CONFLICT (record_date, classification_desc, category_desc, src_line_nbr)
            DO UPDATE SET
                current_month_net_rcpt_outly_amt = EXCLUDED.current_month_net_rcpt_outly_amt,
                fiscal_year_to_date_net_rcpt_outly_amt = EXCLUDED.fiscal_year_to_date_net_rcpt_outly_amt,
                prior_fiscal_year_to_date_net_rcpt_outly_amt = EXCLUDED.prior_fiscal_year_to_date_net_rcpt_outly_amt,
                current_fytd_net_outly_rcpt_amt = EXCLUDED.current_fytd_net_outly_rcpt_amt,
                prior_fytd_net_outly_rcpt_amt = EXCLUDED.prior_fytd_net_outly_rcpt_amt,
                ingested_at = NOW()
        """

    elif dataset == "auctions":
        return f"""
            INSERT INTO {table_name} (
                auction_date, issue_date, maturity_date, security_type, security_term, cusip,
                high_investment_rate, interest_rate, allotted_pct, avg_med_disc_rate,
                avg_med_invest_rate, avg_med_price, avg_med_yield, bid_to_cover_ratio,
                competitive_accepted, competitive_tendered, non_competitive_accepted,
                non_competitive_tendered, total_accepted, total_tendered, primary_dealer_accepted,
                primary_dealer_tendered, direct_bidder_accepted, direct_bidder_tendered,
                indirect_bidder_accepted, indirect_bidder_tendered, fima_noncomp_accepted,
                fima_noncomp_tendered, soma_accepted, soma_tendered, price_per_100, reopening,
                security_term_day_month, security_term_week_year, spread, treasury_direct_accepted,
                treasury_direct_tendered, record_date
            ) VALUES (
                :auction_date, :issue_date, :maturity_date, :security_type, :security_term, :cusip,
                :high_investment_rate, :interest_rate, :allotted_pct, :avg_med_disc_rate,
                :avg_med_invest_rate, :avg_med_price, :avg_med_yield, :bid_to_cover_ratio,
                :competitive_accepted, :competitive_tendered, :non_competitive_accepted,
                :non_competitive_tendered, :total_accepted, :total_tendered, :primary_dealer_accepted,
                :primary_dealer_tendered, :direct_bidder_accepted, :direct_bidder_tendered,
                :indirect_bidder_accepted, :indirect_bidder_tendered, :fima_noncomp_accepted,
                :fima_noncomp_tendered, :soma_accepted, :soma_tendered, :price_per_100, :reopening,
                :security_term_day_month, :security_term_week_year, :spread, :treasury_direct_accepted,
                :treasury_direct_tendered, :record_date
            )
            ON CONFLICT (auction_date, cusip)
            DO UPDATE SET
                high_investment_rate = EXCLUDED.high_investment_rate,
                interest_rate = EXCLUDED.interest_rate,
                allotted_pct = EXCLUDED.allotted_pct,
                bid_to_cover_ratio = EXCLUDED.bid_to_cover_ratio,
                total_accepted = EXCLUDED.total_accepted,
                total_tendered = EXCLUDED.total_tendered,
                ingested_at = NOW()
        """

    else:
        raise ValueError(f"Unknown dataset: {dataset}")


async def ingest_all_treasury_data(
    db: Session, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest all Treasury datasets.

    Args:
        db: Database session
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)

    Returns:
        Dictionary with results for each dataset
    """
    datasets = [
        "daily_balance",
        "debt_outstanding",
        "interest_rates",
        "monthly_statement",
        "auctions",
    ]
    results = {}

    for dataset in datasets:
        logger.info(f"Starting ingestion for Treasury {dataset}")

        # Create job
        job_config = {
            "source": "treasury",
            "dataset": dataset,
            "start_date": start_date,
            "end_date": end_date,
        }

        job = IngestionJob(
            source="treasury", status=JobStatus.PENDING, config=job_config
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        try:
            # Get the appropriate ingest function
            ingest_funcs = {
                "daily_balance": ingest_treasury_daily_balance,
                "debt_outstanding": ingest_treasury_debt_outstanding,
                "interest_rates": ingest_treasury_interest_rates,
                "monthly_statement": ingest_treasury_monthly_statement,
                "auctions": ingest_treasury_auctions,
            }

            result = await ingest_funcs[dataset](
                db=db, job_id=job.id, start_date=start_date, end_date=end_date
            )

            results[dataset] = {"status": "success", "job_id": job.id, **result}

        except Exception as e:
            logger.error(f"Failed to ingest {dataset}: {e}")
            results[dataset] = {"status": "failed", "job_id": job.id, "error": str(e)}

    return results

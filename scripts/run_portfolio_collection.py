"""
Script to run portfolio collection for LPs and Family Offices.

Run this after the server is started to collect portfolio data.
"""
import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings
from app.core.database import get_engine, create_tables
from app.agentic.portfolio_agent import PortfolioResearchAgent, InvestorContext


async def collect_for_investor(db, investor_id: int, investor_type: str, investor_data: dict):
    """Collect portfolio for a single investor."""
    context = InvestorContext(
        investor_id=investor_id,
        investor_type=investor_type,
        investor_name=investor_data.get("name", "Unknown"),
        formal_name=investor_data.get("formal_name"),
        lp_type=investor_data.get("lp_type"),
        jurisdiction=investor_data.get("jurisdiction"),
        website_url=investor_data.get("website_url")
    )
    
    agent = PortfolioResearchAgent(db)
    result = await agent.collect_portfolio(context)
    return result


async def main():
    print("=" * 60)
    print("AGENTIC PORTFOLIO COLLECTION")
    print("=" * 60)
    
    # Create tables first
    print("\n[*] Ensuring database tables exist...")
    create_tables()
    
    # Get database connection
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # Get LPs - prioritize those without portfolio data
        print("\n[*] Fetching LPs (prioritizing those without portfolio data)...")
        lp_result = db.execute(text("""
            SELECT lf.id, lf.name, lf.formal_name, lf.lp_type, lf.jurisdiction, lf.website_url 
            FROM lp_fund lf
            LEFT JOIN (
                SELECT DISTINCT investor_id FROM portfolio_companies WHERE investor_type = 'lp'
            ) pc ON lf.id = pc.investor_id
            ORDER BY CASE WHEN pc.investor_id IS NULL THEN 0 ELSE 1 END, lf.id
            LIMIT 30
        """)).fetchall()
        
        print(f"   Found {len(lp_result)} LPs to process")
        
        # Get Family Offices - prioritize those without portfolio data
        print("\n[*] Fetching Family Offices (prioritizing those without portfolio data)...")
        fo_result = db.execute(text("""
            SELECT fo.id, fo.name, fo.legal_name, NULL as lp_type, fo.state_province, fo.website 
            FROM family_offices fo
            LEFT JOIN (
                SELECT DISTINCT investor_id FROM portfolio_companies WHERE investor_type = 'family_office'
            ) pc ON fo.id = pc.investor_id
            ORDER BY CASE WHEN pc.investor_id IS NULL THEN 0 ELSE 1 END, fo.id
            LIMIT 30
        """)).fetchall()
        
        print(f"   Found {len(fo_result)} Family Offices")
        
        # Process LPs
        print("\n" + "=" * 60)
        print("COLLECTING PORTFOLIO DATA FOR LPs")
        print("=" * 60)
        
        total_lp_companies = 0
        for row in lp_result:
            investor_id = row[0]
            investor_data = {
                "name": row[1],
                "formal_name": row[2],
                "lp_type": row[3],
                "jurisdiction": row[4],
                "website_url": row[5]
            }
            
            print(f"\n[>] Processing LP: {investor_data['name']} (id={investor_id})")
            
            try:
                result = await collect_for_investor(db, investor_id, "lp", investor_data)
                companies_found = result.get("companies_found", 0)
                total_lp_companies += companies_found
                status = result.get("status", "unknown")
                strategies = result.get("strategies_used", [])
                
                print(f"   [OK] Status: {status}")
                print(f"   [+] Companies found: {companies_found}")
                print(f"   [i] Strategies: {strategies}")
                
                if result.get("errors"):
                    print(f"   [!] Errors: {result['errors']}")
                    
            except Exception as e:
                print(f"   [X] Error: {str(e)}")
        
        # Process Family Offices
        print("\n" + "=" * 60)
        print("COLLECTING PORTFOLIO DATA FOR FAMILY OFFICES")
        print("=" * 60)
        
        total_fo_companies = 0
        for row in fo_result:
            investor_id = row[0]
            investor_data = {
                "name": row[1],
                "formal_name": row[2],
                "lp_type": row[3],
                "jurisdiction": row[4],
                "website_url": row[5]
            }
            
            print(f"\n[>] Processing FO: {investor_data['name']} (id={investor_id})")
            
            try:
                result = await collect_for_investor(db, investor_id, "family_office", investor_data)
                companies_found = result.get("companies_found", 0)
                total_fo_companies += companies_found
                status = result.get("status", "unknown")
                strategies = result.get("strategies_used", [])
                
                print(f"   [OK] Status: {status}")
                print(f"   [+] Companies found: {companies_found}")
                print(f"   [i] Strategies: {strategies}")
                
                if result.get("errors"):
                    print(f"   [!] Errors: {result['errors']}")
                    
            except Exception as e:
                print(f"   [X] Error: {str(e)}")
        
        # Summary
        print("\n" + "=" * 60)
        print("COLLECTION SUMMARY")
        print("=" * 60)
        print(f"   LPs processed: {len(lp_result)}")
        print(f"   LP portfolio companies found: {total_lp_companies}")
        print(f"   Family Offices processed: {len(fo_result)}")
        print(f"   FO portfolio companies found: {total_fo_companies}")
        print(f"   TOTAL companies: {total_lp_companies + total_fo_companies}")
        
        # Check database
        print("\n[*] Verifying database...")
        portfolio_count = db.execute(text("SELECT COUNT(*) FROM portfolio_companies")).fetchone()[0]
        print(f"   Total portfolio_companies records: {portfolio_count}")
        
    finally:
        db.close()
    
    print("\n[DONE] Collection complete!")


if __name__ == "__main__":
    asyncio.run(main())

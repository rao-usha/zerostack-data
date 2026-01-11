"""Quick script to check portfolio data."""
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata')

with engine.connect() as conn:
    total = conn.execute(text('SELECT COUNT(*) FROM portfolio_companies')).fetchone()[0]
    print(f'TOTAL PORTFOLIO RECORDS: {total:,}')
    
    print('\nBy Source:')
    for row in conn.execute(text('SELECT source_type, COUNT(*) FROM portfolio_companies GROUP BY source_type')):
        print(f'  {row[0]}: {row[1]:,}')
    
    print('\nBy Investor Type:')
    for row in conn.execute(text('SELECT investor_type, COUNT(*) FROM portfolio_companies GROUP BY investor_type')):
        print(f'  {row[0]}: {row[1]:,}')
    
    print('\nSample SEC 13F Holdings (real pension fund data):')
    for row in conn.execute(text("SELECT company_name, shares_held FROM portfolio_companies WHERE source_type='sec_13f' LIMIT 10")):
        print(f'  {row[0]}: {row[1]} shares')

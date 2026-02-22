"""Quick test for PublicCompsCollector."""
import asyncio
import app.sources.pe_collection  # noqa: F401
from app.sources.pe_collection.financial_collectors.public_comps_collector import PublicCompsCollector


def fmt_b(val):
    if val is None:
        return "N/A"
    return f"${val/1e9:.1f}B"


def fmt_t(val):
    if val is None:
        return "N/A"
    return f"${val/1e12:.2f}T"


def fmt_pct(val):
    if val is None:
        return "N/A"
    return f"{val*100:.1f}%"


async def test():
    collector = PublicCompsCollector(rate_limit_delay=1.0)

    tests = [
        (1, "Apple Inc.", "AAPL"),
        (2, "Blackstone Inc.", "BX"),
        (3, "Microsoft", None),  # ticker search test
    ]

    for eid, name, ticker in tests:
        print(f"=== {name} (ticker={ticker or 'search'}) ===")
        result = await collector.collect(
            entity_id=eid, entity_name=name, ticker=ticker
        )
        print(f"Success: {result.success}  Items: {result.items_found}  Duration: {result.duration_seconds:.1f}s")
        if result.error_message:
            print(f"Error: {result.error_message}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")
        for item in result.items:
            d = item.data
            if item.item_type == "company_financial":
                print(f"  [financial] Revenue: {fmt_b(d.get('revenue'))}  "
                      f"EBITDA: {fmt_b(d.get('ebitda'))}  "
                      f"Net Income: {fmt_b(d.get('net_income'))}  "
                      f"FCF: {fmt_b(d.get('free_cash_flow'))}  "
                      f"Op Margin: {fmt_pct(d.get('operating_margin'))}")
            elif item.item_type == "company_valuation":
                print(f"  [valuation] Mkt Cap: {fmt_t(d.get('market_cap'))}  "
                      f"EV: {fmt_t(d.get('enterprise_value'))}  "
                      f"EV/Rev: {d.get('ev_to_revenue', 'N/A')}  "
                      f"EV/EBITDA: {d.get('ev_to_ebitda', 'N/A')}  "
                      f"P/E: {d.get('trailing_pe', 'N/A')}")
            elif item.item_type == "company_update":
                print(f"  [profile] {d.get('industry')} | {d.get('sector')} | "
                      f"{d.get('employee_count')} employees | "
                      f"{d.get('headquarters_city')}, {d.get('headquarters_state')}")
        print()


asyncio.run(test())

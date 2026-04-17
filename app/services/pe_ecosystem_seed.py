"""
PE Intelligence Platform — Ecosystem Seed (PLAN_060 Phase 0).

Deterministic, idempotent generator that populates all PE tables with
realistic, internally-consistent synthetic data so every downstream
engine (capital deployment, portfolio ops, exit decisions) has fuel.

Usage:
    seeder = PEEcosystemSeeder(db)
    stats = seeder.seed(seed=42)

All rows are tagged with `data_source='pe_ecosystem_seed'` for
traceability. Calling `purge()` removes only seeded data.
"""

from __future__ import annotations

import logging
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List

from sqlalchemy import String as SAString
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PECashFlow,
    PECompanyFinancials,
    PECompanyLeadership,
    PEDeal,
    PEDealParticipant,
    PEFirm,
    PEFund,
    PEFundInvestment,
    PEFundPerformance,
    PEMarketSignal,
    PEPerson,
    PEPortfolioCompany,
    PEPortfolioSnapshot,
)

logger = logging.getLogger(__name__)

DATA_SOURCE = "pe_ecosystem_seed"

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

FIRM_TEMPLATES = [
    ("Summit Point Capital", "Buyout", ["Healthcare", "Technology"], 8500, "NY"),
    ("Ridgeline Partners", "Growth Equity", ["Technology", "Consumer"], 4200, "CA"),
    ("Ironbridge Capital", "Buyout", ["Industrial", "Energy"], 12000, "TX"),
    ("Northstar Equity", "Growth Equity", ["Healthcare", "Finance"], 3100, "MA"),
    ("Cascade Ventures", "Venture", ["Technology", "AI"], 1800, "CA"),
    ("Blackpine Group", "Buyout", ["Consumer", "Retail"], 6700, "IL"),
    ("Meridian Capital Partners", "Buyout", ["Healthcare", "Industrial"], 15000, "NY"),
    ("Vanguard Growth Fund", "Growth Equity", ["Technology", "Finance"], 2400, "CA"),
    ("Pinnacle Infrastructure", "Buyout", ["Energy", "Industrial"], 9200, "TX"),
    ("Atlas Strategic Partners", "Buyout", ["Technology", "Healthcare"], 5500, "NY"),
]

SECTORS = ["Healthcare", "Technology", "Industrial", "Consumer", "Finance",
           "Energy", "Retail", "AI", "Services"]

COMPANY_TEMPLATES = [
    # (name, sector, employees, founded, status_profile)
    # status_profile: "exit_ready" | "growing" | "stable" | "stressed" | "early"
    ("MedVista Health Systems", "Healthcare", 1200, 2008, "exit_ready"),
    ("Apex Cloud Platform", "Technology", 340, 2017, "growing"),
    ("Precision Machining Corp", "Industrial", 850, 1995, "stable"),
    ("TrueNorth Financial", "Finance", 420, 2012, "exit_ready"),
    ("GreenField Energy Solutions", "Energy", 680, 2010, "stable"),
    ("UrbanLux Brands", "Consumer", 520, 2014, "growing"),
    ("DataForge Analytics", "Technology", 180, 2019, "early"),
    ("CareBridge Senior Living", "Healthcare", 2200, 2005, "exit_ready"),
    ("SteelRidge Manufacturing", "Industrial", 1100, 1988, "stressed"),
    ("NovaPay Systems", "Finance", 290, 2016, "growing"),
    ("CleanAir Technologies", "Energy", 380, 2015, "growing"),
    ("QuickServe Logistics", "Services", 950, 2009, "stable"),
    ("BrightMind AI", "AI", 120, 2020, "early"),
    ("SecureNet Cyber", "Technology", 210, 2018, "growing"),
    ("Vitality Pharma", "Healthcare", 780, 2011, "exit_ready"),
    ("Pacific Coast Distribution", "Retail", 1400, 2001, "stable"),
    ("Summit Analytics Group", "Technology", 160, 2019, "early"),
    ("Heritage Foods Inc", "Consumer", 3200, 1992, "stressed"),
    ("Clearwater Insurance", "Finance", 540, 2007, "stable"),
    ("ProBuild Construction", "Industrial", 2800, 1996, "stressed"),
    ("Aether Robotics", "AI", 95, 2021, "early"),
    ("MedTech Innovations", "Healthcare", 450, 2013, "growing"),
    ("Silverline Wealth", "Finance", 180, 2018, "early"),
    ("EcoVolt Power", "Energy", 520, 2012, "stable"),
    ("CoreLogic Systems", "Technology", 310, 2015, "growing"),
    ("RapidCare Urgent", "Healthcare", 680, 2010, "exit_ready"),
    ("TerraFirm Mining", "Industrial", 1600, 1985, "stressed"),
    ("PixelPerfect Media", "Consumer", 140, 2020, "early"),
    ("CrossBorder Payments", "Finance", 220, 2017, "growing"),
    ("NexGen Manufacturing", "Industrial", 900, 2008, "stable"),
    ("Beacon Health IT", "Healthcare", 350, 2016, "growing"),
    ("Catalyst Chemical", "Industrial", 750, 2003, "stable"),
    ("FreshMarket Foods", "Retail", 1800, 1998, "stressed"),
    ("AlphaEdge Capital Tech", "Technology", 130, 2021, "early"),
    ("Reliable Staffing", "Services", 2100, 2004, "stable"),
    ("CloudNine SaaS", "Technology", 260, 2018, "growing"),
    ("Patriot Defense Systems", "Industrial", 440, 2011, "stable"),
    ("WellSpring Clinics", "Healthcare", 580, 2009, "exit_ready"),
    ("MetroConnect Telecom", "Technology", 720, 2006, "stable"),
    ("AgriStar Farms", "Consumer", 3500, 1990, "stressed"),
    ("Vertex Biotech", "Healthcare", 200, 2019, "early"),
    ("IronClad Security", "Technology", 310, 2014, "growing"),
    ("GoldLeaf Financial", "Finance", 250, 2015, "growing"),
    ("BlueSky Aviation", "Services", 600, 2010, "stable"),
    ("Quantum Computing Labs", "AI", 80, 2022, "early"),
    ("OceanView Hotels", "Consumer", 1500, 2000, "stable"),
    ("SwiftShip Logistics", "Services", 1100, 2007, "exit_ready"),
    ("Titan Steel Works", "Industrial", 2400, 1980, "stressed"),
    ("NeuraTech AI", "AI", 150, 2020, "early"),
    ("PrimaCare Dental", "Healthcare", 400, 2013, "growing"),
]

SECTOR_MARGIN_PRIORS = {
    "Healthcare":  {"gross": (0.55, 0.10), "ebitda": (0.18, 0.06)},
    "Technology":  {"gross": (0.65, 0.10), "ebitda": (0.22, 0.08)},
    "Industrial":  {"gross": (0.35, 0.08), "ebitda": (0.14, 0.05)},
    "Consumer":    {"gross": (0.45, 0.08), "ebitda": (0.12, 0.05)},
    "Finance":     {"gross": (0.70, 0.10), "ebitda": (0.30, 0.10)},
    "Energy":      {"gross": (0.40, 0.10), "ebitda": (0.16, 0.06)},
    "Retail":      {"gross": (0.30, 0.06), "ebitda": (0.08, 0.04)},
    "AI":          {"gross": (0.72, 0.08), "ebitda": (0.15, 0.10)},
    "Services":    {"gross": (0.40, 0.08), "ebitda": (0.14, 0.05)},
}

PROFILE_GROWTH = {
    "exit_ready": (0.10, 0.05),
    "growing":    (0.18, 0.08),
    "stable":     (0.05, 0.03),
    "stressed":   (-0.05, 0.06),
    "early":      (0.30, 0.15),
}

DEAL_TYPES = ["LBO", "LBO", "LBO", "LBO", "Growth Equity", "Growth Equity",
              "Add-on", "Add-on", "Exit", "Recap"]

C_SUITE_TITLES = [
    ("CEO", "Chief Executive Officer"),
    ("CFO", "Chief Financial Officer"),
    ("COO", "Chief Operating Officer"),
    ("CTO", "Chief Technology Officer"),
    ("CMO", "Chief Marketing Officer"),
    ("CHRO", "Chief Human Resources Officer"),
]


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------


class PEEcosystemSeeder:
    """Populate PE tables with a realistic, cross-linked synthetic ecosystem."""

    def __init__(self, db: Session):
        self.db = db

    def seed(self, seed: int = 42) -> Dict:
        """Run full seed. Returns row-count summary."""
        self.purge()
        rng = random.Random(seed)
        stats: Dict[str, int] = {}

        firms = self._seed_firms(rng)
        stats["pe_firms"] = len(firms)

        funds = self._seed_funds(rng, firms)
        stats["pe_funds"] = len(funds)

        perf = self._seed_fund_performance(rng, funds)
        stats["pe_fund_performance"] = len(perf)

        companies = self._seed_companies(rng)
        stats["pe_portfolio_companies"] = len(companies)

        investments = self._seed_investments(rng, funds, companies)
        stats["pe_fund_investments"] = len(investments)

        financials = self._seed_financials(rng, companies)
        stats["pe_company_financials"] = len(financials)

        leadership = self._seed_leadership(rng, companies)
        stats["pe_company_leadership"] = len(leadership)

        deals = self._seed_deals(rng, companies, firms)
        stats["pe_deals"] = len(deals)

        signals = self._seed_market_signals(rng)
        stats["pe_market_signals"] = len(signals)

        cash_flows = self._seed_cash_flows(rng, funds)
        stats["pe_cash_flows"] = len(cash_flows)

        snapshots = self._seed_snapshots(rng, companies, firms)
        stats["pe_portfolio_snapshots"] = len(snapshots)

        self.db.commit()
        logger.info("PE ecosystem seed complete: %s", stats)
        return stats

    def purge(self) -> Dict:
        """Remove all seeded data (idempotent)."""
        counts = {}
        # Delete in FK-safe order; use data_source where available, batch_id or name otherwise
        for model in [PEPortfolioSnapshot, PECashFlow, PEDealParticipant, PEDeal,
                      PECompanyLeadership, PECompanyFinancials, PEFundPerformance, PEFund]:
            if hasattr(model, "data_source"):
                n = self.db.query(model).filter_by(data_source=DATA_SOURCE).delete()
                counts[model.__tablename__] = n

        # PEFundInvestment has no data_source — delete by linked fund
        seed_fund_ids = [f.id for f in self.db.query(PEFund).filter_by(data_source=DATA_SOURCE).all()] if False else []
        # Just delete all seeded fund investments by company name match instead
        seed_company_ids = [c.id for c in self.db.query(PEPortfolioCompany).filter_by(data_source=DATA_SOURCE).all()]
        if seed_company_ids:
            n = self.db.query(PEFundInvestment).filter(PEFundInvestment.company_id.in_(seed_company_ids)).delete()
            counts["pe_fund_investments"] = n

        # PEMarketSignal uses batch_id
        n = self.db.query(PEMarketSignal).filter_by(batch_id="pe_ecosystem_seed").delete()
        counts["pe_market_signals"] = n

        # People (data_sources is JSON — filter by name pattern from seed)
        n = self.db.query(PEPerson).filter(PEPerson.data_sources.cast(SAString).like('%pe_ecosystem_seed%')).delete()
        counts["pe_people"] = n

        # Companies
        n = self.db.query(PEPortfolioCompany).filter_by(data_source=DATA_SOURCE).delete()
        counts["pe_portfolio_companies"] = n

        # Firms: delete by known seed names
        seed_names = [t[0] for t in FIRM_TEMPLATES]
        n = self.db.query(PEFirm).filter(PEFirm.name.in_(seed_names)).delete()
        counts["pe_firms"] = n

        self.db.commit()
        return counts

    def status(self) -> Dict:
        """Current row counts for all PE tables."""
        tables = {
            "pe_firms": PEFirm,
            "pe_funds": PEFund,
            "pe_fund_performance": PEFundPerformance,
            "pe_portfolio_companies": PEPortfolioCompany,
            "pe_fund_investments": PEFundInvestment,
            "pe_company_financials": PECompanyFinancials,
            "pe_company_leadership": PECompanyLeadership,
            "pe_deals": PEDeal,
            "pe_market_signals": PEMarketSignal,
            "pe_cash_flows": PECashFlow,
            "pe_portfolio_snapshots": PEPortfolioSnapshot,
        }
        return {name: self.db.query(model).count() for name, model in tables.items()}

    # -------------------------------------------------------------------
    # Seeders for each table
    # -------------------------------------------------------------------

    def _seed_firms(self, rng: random.Random) -> List[PEFirm]:
        firms = []
        for name, strategy, sectors, aum, state in FIRM_TEMPLATES:
            f = PEFirm(
                name=name,
                firm_type="PE" if "Buyout" in strategy else ("VC" if "Venture" in strategy else "Growth"),
                primary_strategy=strategy,
                sector_focus=sectors,
                aum_usd_millions=Decimal(str(aum)),
                headquarters_state=state,
                headquarters_country="USA",
                employee_count=rng.randint(30, 200),
                typical_check_size_min=Decimal(str(aum // 40)),
                typical_check_size_max=Decimal(str(aum // 10)),
                founded_year=rng.randint(1990, 2015),
                status="Active",
                confidence_score=Decimal("0.90"),
                data_sources=["pe_ecosystem_seed"],
            )
            self.db.add(f)
            firms.append(f)
        self.db.flush()
        return firms

    def _seed_funds(self, rng: random.Random, firms: List[PEFirm]) -> List[PEFund]:
        funds = []
        for firm in firms:
            for i in range(2):
                vintage = 2018 + i * 3 + rng.randint(-1, 1)
                target = float(firm.aum_usd_millions or 5000) * rng.uniform(0.15, 0.35)
                fund = PEFund(
                    firm_id=firm.id,
                    name=f"{firm.name} Fund {'I' if i == 0 else 'II'}",
                    fund_number=i + 1,
                    vintage_year=vintage,
                    target_size_usd_millions=Decimal(str(round(target))),
                    final_close_usd_millions=Decimal(str(round(target * rng.uniform(0.9, 1.3)))),
                    strategy=firm.primary_strategy,
                    sector_focus=firm.sector_focus,
                    management_fee_pct=Decimal("2.00"),
                    carried_interest_pct=Decimal("20.00"),
                    preferred_return_pct=Decimal("8.00"),
                    fund_life_years=10,
                    investment_period_years=5,
                    status="Active" if vintage >= 2020 else "Harvesting",
                    data_source=DATA_SOURCE,
                )
                self.db.add(fund)
                funds.append(fund)
        self.db.flush()
        return funds

    def _seed_fund_performance(self, rng: random.Random, funds: List[PEFund]) -> list:
        rows = []
        for fund in funds:
            age = 2026 - (fund.vintage_year or 2020)
            base_irr = rng.uniform(8, 25)
            committed = float(fund.final_close_usd_millions or 500)
            for q_idx in range(min(age, 3)):
                as_of = date(2024 + q_idx, 6, 30)
                # TVPI grows with age; DPI grows slower
                tvpi = 1.0 + (base_irr / 100) * (age - 2 + q_idx) * rng.uniform(0.8, 1.2)
                dpi = tvpi * rng.uniform(0.2, 0.6) if age > 4 else tvpi * 0.05
                rvpi = tvpi - dpi
                called_pct = min(1.0, 0.3 + age * 0.12)
                perf = PEFundPerformance(
                    fund_id=fund.id,
                    as_of_date=as_of,
                    reporting_quarter=f"Q2 {2024 + q_idx}",
                    net_irr_pct=Decimal(str(round(base_irr + q_idx * 0.5, 2))),
                    gross_irr_pct=Decimal(str(round(base_irr * 1.15 + q_idx * 0.5, 2))),
                    tvpi=Decimal(str(round(tvpi, 3))),
                    dpi=Decimal(str(round(dpi, 3))),
                    rvpi=Decimal(str(round(rvpi, 3))),
                    committed_capital=Decimal(str(round(committed, 2))),
                    called_capital=Decimal(str(round(committed * called_pct, 2))),
                    distributed_capital=Decimal(str(round(committed * float(dpi) * called_pct, 2))),
                    remaining_value=Decimal(str(round(committed * float(rvpi) * called_pct, 2))),
                    active_investments=rng.randint(3, 8),
                    realized_investments=rng.randint(0, 3) if age > 3 else 0,
                    data_source=DATA_SOURCE,
                )
                self.db.add(perf)
                rows.append(perf)
        self.db.flush()
        return rows

    def _seed_companies(self, rng: random.Random) -> List[PEPortfolioCompany]:
        companies = []
        for name, sector, emps, founded, profile in COMPANY_TEMPLATES:
            base_rev = emps * rng.uniform(80, 250) * 1000
            c = PEPortfolioCompany(
                name=name,
                industry=sector,
                sector=sector,
                headquarters_state=rng.choice(["CA", "NY", "TX", "MA", "IL", "OH", "PA", "FL"]),
                headquarters_country="USA",
                employee_count=emps,
                founded_year=founded,
                ownership_status="PE-Backed",
                status="Active",
                naics_code=str(rng.randint(300000, 600000)),
                data_source=DATA_SOURCE,
            )
            c._profile = profile
            c._base_rev = base_rev
            self.db.add(c)
            companies.append(c)
        self.db.flush()
        return companies

    def _seed_investments(self, rng, funds, companies) -> list:
        rows = []
        fund_cycle = list(funds)
        rng.shuffle(fund_cycle)
        for i, company in enumerate(companies):
            fund = fund_cycle[i % len(fund_cycle)]
            inv_year = max((fund.vintage_year or 2020), (company.founded_year or 2010) + 2)
            inv_date = date(inv_year, rng.randint(1, 12), rng.randint(1, 28))
            entry_ev = company._base_rev * rng.uniform(6, 14)
            invested = entry_ev * rng.uniform(0.3, 0.8)
            is_exited = getattr(company, "_profile", "") == "exit_ready" and rng.random() < 0.3
            inv = PEFundInvestment(
                fund_id=fund.id,
                company_id=company.id,
                investment_date=inv_date,
                investment_type="Platform" if rng.random() > 0.3 else "Add-on",
                invested_amount_usd=Decimal(str(round(invested, 2))),
                ownership_pct=Decimal(str(round(rng.uniform(30, 90), 2))),
                entry_ev_usd=Decimal(str(round(entry_ev, 2))),
                entry_ev_ebitda_multiple=Decimal(str(round(rng.uniform(7, 14), 2))),
                has_board_seat=True,
                board_seats=rng.randint(1, 3),
                status="Exited" if is_exited else "Active",
                exit_date=date(2025, rng.randint(1, 6), 15) if is_exited else None,
                exit_type="Strategic Sale" if is_exited else None,
                exit_multiple=Decimal(str(round(rng.uniform(1.8, 3.5), 2))) if is_exited else None,
                exit_irr_pct=Decimal(str(round(rng.uniform(15, 35), 2))) if is_exited else None,
            )
            self.db.add(inv)
            rows.append(inv)
        self.db.flush()
        return rows

    def _seed_financials(self, rng, companies) -> list:
        rows = []
        for company in companies:
            sector = company.sector or "Services"
            priors = SECTOR_MARGIN_PRIORS.get(sector, SECTOR_MARGIN_PRIORS["Services"])
            profile = getattr(company, "_profile", "stable")
            growth_mu, growth_sd = PROFILE_GROWTH.get(profile, (0.05, 0.03))
            base_rev = getattr(company, "_base_rev", 50_000_000)

            for yr_offset in range(4):
                fy = 2021 + yr_offset
                growth = rng.gauss(growth_mu, growth_sd)
                rev = base_rev * (1 + growth) ** yr_offset
                gm = max(0.1, rng.gauss(*priors["gross"]))
                em = max(0.02, rng.gauss(*priors["ebitda"]))
                ebitda = rev * em
                debt_ratio = rng.uniform(2.0, 6.0)

                fin = PECompanyFinancials(
                    company_id=company.id,
                    fiscal_year=fy,
                    fiscal_period="FY",
                    period_end_date=date(fy, 12, 31),
                    revenue_usd=Decimal(str(round(rev, 2))),
                    revenue_growth_pct=Decimal(str(round(growth * 100, 2))),
                    gross_profit_usd=Decimal(str(round(rev * gm, 2))),
                    gross_margin_pct=Decimal(str(round(gm * 100, 2))),
                    ebitda_usd=Decimal(str(round(ebitda, 2))),
                    ebitda_margin_pct=Decimal(str(round(em * 100, 2))),
                    net_income_usd=Decimal(str(round(ebitda * rng.uniform(0.3, 0.7), 2))),
                    total_debt_usd=Decimal(str(round(ebitda * debt_ratio, 2))),
                    cash_usd=Decimal(str(round(rev * rng.uniform(0.05, 0.15), 2))),
                    free_cash_flow_usd=Decimal(str(round(ebitda * rng.uniform(0.3, 0.6), 2))),
                    debt_to_ebitda=Decimal(str(round(debt_ratio, 2))),
                    data_source=DATA_SOURCE,
                    confidence="medium",
                )
                self.db.add(fin)
                rows.append(fin)
        self.db.flush()
        return rows

    def _seed_leadership(self, rng, companies) -> list:
        rows = []
        first_names = ['James','Sarah','Michael','Jennifer','David','Emily','Robert','Lisa','Andrew','Maria','Daniel','Rachel','Thomas','Catherine','William','Laura']
        last_names = ['Chen','Smith','Patel','Johnson','Williams','Brown','Davis','Wilson','Lee','Martin','Thompson','Garcia','Miller','Anderson','Taylor','Harris']

        for company in companies:
            n_execs = rng.randint(2, 4)
            titles = list(C_SUITE_TITLES[:n_execs])
            for short, full in titles:
                first = rng.choice(first_names)
                last = rng.choice(last_names)
                full_name = f"{first} {last}"

                # Create PEPerson first (required FK)
                person = PEPerson(
                    full_name=full_name,
                    first_name=first,
                    last_name=last,
                    current_title=full,
                    current_company=company.name,
                    is_active=True,
                    data_sources=["pe_ecosystem_seed"],
                )
                self.db.add(person)
                self.db.flush()

                ldr = PECompanyLeadership(
                    company_id=company.id,
                    person_id=person.id,
                    title=full,
                    role_category="C-Suite",
                    is_ceo=("CEO" in short),
                    is_cfo=("CFO" in short),
                    start_date=date(2026 - rng.randint(1, 8), rng.randint(1, 12), 1),
                    is_current=True,
                    appointed_by_pe=rng.random() < 0.3,
                    data_source=DATA_SOURCE,
                )
                self.db.add(ldr)
                rows.append(ldr)
        self.db.flush()
        return rows

    def _seed_deals(self, rng, companies, firms) -> list:
        rows = []
        for i in range(30):
            company = rng.choice(companies)
            firm = rng.choice(firms)
            deal_type = rng.choice(DEAL_TYPES)
            ann_date = date(2024, rng.randint(1, 12), rng.randint(1, 28))
            ev = float(getattr(company, "_base_rev", 50_000_000)) * rng.uniform(6, 14)
            deal = PEDeal(
                company_id=company.id,
                deal_name=f"{company.name} — {deal_type}",
                deal_type=deal_type,
                announced_date=ann_date,
                closed_date=ann_date + timedelta(days=rng.randint(30, 180)) if rng.random() > 0.3 else None,
                enterprise_value_usd=Decimal(str(round(ev, 2))),
                ev_ebitda_multiple=Decimal(str(round(rng.uniform(7, 16), 2))),
                buyer_name=firm.name if deal_type != "Exit" else rng.choice(["Strategic Corp", "Mega Holdings LLC", "IPO"]),
                status="Closed" if rng.random() > 0.3 else "Active",
                data_source=DATA_SOURCE,
            )
            self.db.add(deal)
            rows.append(deal)
        self.db.flush()
        return rows

    def _seed_market_signals(self, rng) -> list:
        rows = []
        for sector in SECTORS:
            momentum = rng.randint(30, 90)
            sig = PEMarketSignal(
                sector=sector,
                momentum_score=momentum,
                deal_count=rng.randint(5, 80),
                avg_multiple=Decimal(str(round(rng.uniform(7, 16), 2))),
                signal_type="bullish" if momentum > 65 else ("neutral" if momentum > 45 else "bearish"),
                deal_flow_change_pct=Decimal(str(round(rng.uniform(-15, 25), 2))),
                multiple_change_pct=Decimal(str(round(rng.uniform(-5, 10), 2))),
                batch_id="pe_ecosystem_seed",
                scanned_at=datetime.utcnow(),
            )
            self.db.add(sig)
            rows.append(sig)
        self.db.flush()
        return rows

    def _seed_cash_flows(self, rng, funds) -> list:
        rows = []
        for fund in funds:
            committed = float(fund.final_close_usd_millions or 500) * 1_000_000
            age = 2026 - (fund.vintage_year or 2020)
            # Capital calls in early years, distributions in later years
            for yr in range(min(age, 6)):
                call_amt = committed * rng.uniform(0.08, 0.2)
                rows.append(PECashFlow(
                    fund_id=fund.id,
                    flow_date=date(fund.vintage_year + yr, rng.randint(1, 6), 15),
                    amount=Decimal(str(round(-call_amt, 2))),
                    cash_flow_type="capital_call",
                    description=f"Capital Call #{yr + 1}",
                    data_source=DATA_SOURCE,
                ))
                if yr >= 3:
                    dist_amt = committed * rng.uniform(0.05, 0.15)
                    rows.append(PECashFlow(
                        fund_id=fund.id,
                        flow_date=date(fund.vintage_year + yr, rng.randint(7, 12), 15),
                        amount=Decimal(str(round(dist_amt, 2))),
                        cash_flow_type="distribution",
                        description=f"Distribution from exit",
                        data_source=DATA_SOURCE,
                    ))
                self.db.add_all(rows[-2:] if yr >= 3 else rows[-1:])
        self.db.flush()
        return rows

    def _seed_snapshots(self, rng, companies, firms) -> list:
        rows = []
        for company in companies:
            profile = getattr(company, "_profile", "stable")
            exit_score = {"exit_ready": 82, "growing": 65, "stable": 55, "stressed": 35, "early": 40}.get(profile, 50)
            snap = PEPortfolioSnapshot(
                company_id=company.id,
                firm_id=firms[0].id,
                snapshot_date=date(2026, 3, 31),
                exit_score=Decimal(str(exit_score + rng.randint(-5, 5))),
                exit_grade="A" if exit_score >= 80 else "B" if exit_score >= 65 else "C" if exit_score >= 50 else "D",
                revenue=Decimal(str(round(getattr(company, "_base_rev", 50_000_000) * (1.05 ** 3), 2))),
                ebitda_margin=Decimal(str(round(rng.uniform(10, 30), 2))),
                data_source=DATA_SOURCE,
            )
            self.db.add(snap)
            rows.append(snap)
        self.db.flush()
        return rows

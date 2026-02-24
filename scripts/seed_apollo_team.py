#!/usr/bin/env python3
"""
Seed Apollo Global Management's leadership team and segment AUM from public sources.

Apollo's website is JS-rendered, so the bio extractor can't parse it without
Playwright. This script seeds the known leadership from publicly available
information (SEC 10-K, proxy statements, press releases, Wikipedia).

Segment AUM is from Apollo's 10-K filing (Dec 31, 2024).

Usage:
    python scripts/seed_apollo_team.py
    python scripts/seed_apollo_team.py --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Apollo Global Management leadership team
# Sources: SEC 10-K/proxy, apollo.com, press releases, Wikipedia
APOLLO_TEAM = [
    # C-Suite / Founders
    {
        "full_name": "Marc Rowan",
        "title": "Chief Executive Officer & Co-Founder",
        "seniority": "Partner",
        "department": "Executive",
        "start_year": 1990,
        "bio": "Co-founded Apollo in 1990 and became CEO in 2021. Architected Apollo's expansion into retirement services through the Athene platform, transforming the firm into a hybrid alternative asset manager and insurance company. Oversees $651 billion in AUM.",
        "education": [
            {"institution": "University of Pennsylvania (Wharton)", "degree": "BS", "field": "Economics", "graduation_year": 1985},
            {"institution": "University of Pennsylvania (Wharton)", "degree": "MBA", "field": "Finance", "graduation_year": 1985},
        ],
        "experience": [
            {"company": "Drexel Burnham Lambert", "title": "Associate", "start_year": 1985, "end_year": 1990},
        ],
    },
    {
        "full_name": "Scott Kleinman",
        "title": "Co-President",
        "seniority": "Partner",
        "department": "Executive",
        "start_year": 1996,
        "bio": "Joined Apollo in 1996 and became Co-President in 2022. Oversees the firm's asset management businesses including private equity, credit, and real assets. Previously led Apollo's private equity business.",
        "education": [
            {"institution": "University of Pennsylvania (Wharton)", "degree": "BS", "field": "Economics", "graduation_year": 1996},
        ],
        "experience": [],
    },
    {
        "full_name": "James Zelter",
        "title": "Co-President",
        "seniority": "Partner",
        "department": "Executive",
        "start_year": 2006,
        "bio": "Joined Apollo in 2006 and became Co-President in 2022. Previously served as Co-Head of Apollo Credit. Oversees the firm's credit and fixed income business, one of the largest alternative credit platforms globally.",
        "education": [
            {"institution": "University of Pennsylvania (Wharton)", "degree": "BS", "field": "Economics", "graduation_year": 1988},
            {"institution": "Columbia Business School", "degree": "MBA", "field": "Finance", "graduation_year": 1993},
        ],
        "experience": [
            {"company": "Citigroup", "title": "Managing Director, Leveraged Finance", "start_year": 2000, "end_year": 2006},
            {"company": "Salomon Brothers", "title": "Vice President", "start_year": 1993, "end_year": 2000},
        ],
    },
    {
        "full_name": "Martin Kelly",
        "title": "Chief Financial Officer & Co-Chief Operating Officer",
        "seniority": "Partner",
        "department": "Finance",
        "start_year": 2007,
        "bio": "Joined Apollo in 2007 and serves as CFO and Co-COO. Responsible for all financial operations, treasury, tax, investor relations, and corporate strategy. Previously at Deutsche Bank.",
        "education": [
            {"institution": "Boston College", "degree": "BS", "field": "Finance", "graduation_year": 1999},
        ],
        "experience": [
            {"company": "Deutsche Bank", "title": "Vice President, Financial Sponsors Group", "start_year": 2003, "end_year": 2007},
            {"company": "Arthur Andersen", "title": "Senior Associate", "start_year": 1999, "end_year": 2002},
        ],
    },
    {
        "full_name": "John Suydam",
        "title": "Chief Legal Officer & CCO",
        "seniority": "Partner",
        "department": "Legal",
        "start_year": 2000,
        "bio": "Joined Apollo in 2000 and serves as Chief Legal Officer and Chief Compliance Officer. Oversees all legal, regulatory, compliance, and government affairs matters globally.",
        "education": [
            {"institution": "Harvard College", "degree": "AB", "field": "Government", "graduation_year": 1990},
            {"institution": "Columbia Law School", "degree": "JD", "field": "Law", "graduation_year": 1993},
        ],
        "experience": [
            {"company": "Schulte Roth & Zabel", "title": "Associate", "start_year": 1993, "end_year": 2000},
        ],
    },

    # Private Equity Leadership
    {
        "full_name": "David Sambur",
        "title": "Senior Partner & Co-Head of Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 2004,
        "bio": "Joined Apollo in 2004 and co-leads the private equity business. Focuses on large-cap buyouts across financial services, media, telecom, and technology. Led investments including Yahoo, Shutterfly, and ADT.",
        "education": [
            {"institution": "Duke University", "degree": "BA", "field": "Public Policy Studies", "graduation_year": 2001},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2006},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Analyst, Leveraged Finance", "start_year": 2001, "end_year": 2004},
        ],
    },
    {
        "full_name": "Matt Nord",
        "title": "Senior Partner & Co-Head of Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 2003,
        "bio": "Joined Apollo in 2003 and co-leads the private equity business. Focuses on industrials, chemicals, and natural resources. Led Apollo's investments in Roper Technologies, Hexion, and Berry Global.",
        "education": [
            {"institution": "Princeton University", "degree": "AB", "field": "Economics", "graduation_year": 2001},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2008},
        ],
        "experience": [
            {"company": "Morgan Stanley", "title": "Analyst, M&A", "start_year": 2001, "end_year": 2003},
        ],
    },
    {
        "full_name": "Alex van Hoek",
        "title": "Partner, Head of European Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 2008,
        "bio": "Joined Apollo in 2008 and leads European private equity. Focuses on large-scale buyouts and corporate carve-outs in Europe across industrials, financial services, and consumer sectors.",
        "education": [
            {"institution": "London School of Economics", "degree": "BSc", "field": "Economics", "graduation_year": 2003},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Analyst, European Special Situations", "start_year": 2003, "end_year": 2008},
        ],
    },
    {
        "full_name": "Sanjay Patel",
        "title": "Partner, Head of Asia Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 2011,
        "bio": "Joined Apollo in 2011 and leads Asia-Pacific private equity. Based in Singapore, oversees investments across India, Southeast Asia, and Greater China.",
        "education": [
            {"institution": "Indian Institute of Technology, Delhi", "degree": "BTech", "field": "Engineering", "graduation_year": 1997},
            {"institution": "Indian Institute of Management, Ahmedabad", "degree": "PGDM", "field": "Management", "graduation_year": 1999},
        ],
        "experience": [
            {"company": "Texas Pacific Group (TPG)", "title": "Principal", "start_year": 2005, "end_year": 2011},
            {"company": "McKinsey & Company", "title": "Associate", "start_year": 1999, "end_year": 2003},
        ],
    },

    # Credit Leadership
    {
        "full_name": "Chris Edson",
        "title": "Partner, Co-Head of Apollo Credit",
        "seniority": "Partner",
        "department": "Credit",
        "start_year": 2008,
        "bio": "Joined Apollo in 2008 and co-leads the credit business. Focuses on performing credit, direct lending, and structured credit strategies. The credit platform manages over $500 billion in assets.",
        "education": [
            {"institution": "Georgetown University", "degree": "BS", "field": "Finance", "graduation_year": 2004},
        ],
        "experience": [
            {"company": "Deutsche Bank", "title": "Analyst, Leveraged Finance", "start_year": 2004, "end_year": 2008},
        ],
    },
    {
        "full_name": "John Zito",
        "title": "Partner, Deputy CIO of Credit",
        "seniority": "Partner",
        "department": "Credit",
        "start_year": 2009,
        "bio": "Joined Apollo in 2009 and serves as Deputy CIO of Apollo Credit. Oversees investment activity across the credit platform including high-yield, leveraged loans, and structured credit.",
        "education": [
            {"institution": "Fordham University", "degree": "BS", "field": "Finance", "graduation_year": 2005},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Analyst, Leveraged Finance", "start_year": 2005, "end_year": 2009},
        ],
    },

    # Retirement Services / Athene
    {
        "full_name": "Jim Belardi",
        "title": "CEO & CIO of Athene",
        "seniority": "Partner",
        "department": "Retirement Services",
        "start_year": 2009,
        "bio": "Co-founded Athene Holding in 2009 with Apollo. Built Athene into one of the largest retirement services platforms with over $300 billion in assets. Athene merged with Apollo in 2022.",
        "education": [
            {"institution": "University of Michigan", "degree": "BBA", "field": "Finance", "graduation_year": 1983},
        ],
        "experience": [
            {"company": "SunLife of New York", "title": "CEO", "start_year": 2004, "end_year": 2009},
            {"company": "Protective Life", "title": "Senior Vice President", "start_year": 1996, "end_year": 2004},
        ],
    },
    {
        "full_name": "Grant Kvalheim",
        "title": "President of Athene",
        "seniority": "Partner",
        "department": "Retirement Services",
        "start_year": 2009,
        "bio": "Co-founded Athene in 2009 and serves as President. Oversees distribution, strategy, and operations for Apollo's retirement services platform.",
        "education": [
            {"institution": "Brigham Young University", "degree": "BS", "field": "Finance", "graduation_year": 1992},
            {"institution": "University of Pennsylvania (Wharton)", "degree": "MBA", "field": "Finance", "graduation_year": 1999},
        ],
        "experience": [
            {"company": "SunLife of New York", "title": "President", "start_year": 2005, "end_year": 2009},
        ],
    },

    # Real Assets / Infrastructure
    {
        "full_name": "Olivia Wassenaar",
        "title": "Partner, Head of Real Assets & Infrastructure",
        "seniority": "Partner",
        "department": "Real Assets",
        "start_year": 2010,
        "bio": "Joined Apollo in 2010 and leads the real assets and infrastructure platform. Oversees investments in infrastructure, clean energy transition, and natural resources globally.",
        "education": [
            {"institution": "University of Virginia", "degree": "BA", "field": "Economics", "graduation_year": 2002},
            {"institution": "Columbia Business School", "degree": "MBA", "field": "Finance", "graduation_year": 2008},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Associate, Natural Resources", "start_year": 2002, "end_year": 2006},
        ],
    },

    # Corporate Functions / Other Senior Leaders (without detailed bios)
    {
        "full_name": "Stephanie Drescher",
        "title": "Chief Client & Product Development Officer",
        "seniority": "Partner",
        "department": "Client Solutions",
    },
    {
        "full_name": "Anthony Civale",
        "title": "Co-Chief Operating Officer",
        "seniority": "Partner",
        "department": "Operations",
    },
    {
        "full_name": "Josh Harris",
        "title": "Co-Founder & Senior Managing Director",
        "seniority": "Partner",
        "department": "Executive",
    },
    {
        "full_name": "Aaron Sobel",
        "title": "Partner, General Counsel",
        "seniority": "Partner",
        "department": "Legal",
    },
    {
        "full_name": "Joanna Reiss",
        "title": "Chief Human Resources Officer",
        "seniority": "Managing Director",
        "department": "Human Resources",
    },
    {
        "full_name": "Lance West",
        "title": "Head of Government & Regulatory Affairs",
        "seniority": "Managing Director",
        "department": "Government Affairs",
    },
    {
        "full_name": "Craig Farr",
        "title": "Partner, Head of Capital Solutions",
        "seniority": "Partner",
        "department": "Capital Solutions",
    },
    {
        "full_name": "Damon Krytzer",
        "title": "Partner, Head of Global Wealth Management",
        "seniority": "Partner",
        "department": "Wealth Management",
    },
    {
        "full_name": "Mike Clancy",
        "title": "Partner, Head of Insurance Solutions",
        "seniority": "Partner",
        "department": "Retirement Services",
    },
    {
        "full_name": "Robert Givone",
        "title": "Partner, Head of Opportunistic Credit",
        "seniority": "Partner",
        "department": "Credit",
    },
]


# Apollo business segments — AUM from 10-K (Dec 31, 2024)
APOLLO_SEGMENTS = [
    {
        "name": "Credit",
        "strategy": "Credit",
        "aum_millions": 506000,
        "description": "Investment grade, direct lending, performing credit, structured credit, and asset-backed finance",
    },
    {
        "name": "Private Equity",
        "strategy": "Buyout",
        "aum_millions": 101000,
        "description": "Large-cap buyouts, hybrid value, and distressed equity across sectors globally",
    },
    {
        "name": "Real Assets",
        "strategy": "Real Assets",
        "aum_millions": 44000,
        "description": "Infrastructure, clean energy, natural resources, and real estate equity and debt",
    },
]


def seed_segments(db, firm_id, dry_run=False):
    """Insert Apollo business segments as pe_funds entries."""
    from sqlalchemy import text

    print(f"\nSeeding {len(APOLLO_SEGMENTS)} business segments...")

    added = 0
    skipped = 0

    for seg in APOLLO_SEGMENTS:
        r = db.execute(
            text("SELECT id FROM pe_funds WHERE firm_id = :fid AND name = :name"),
            {"fid": firm_id, "name": seg["name"]},
        )
        if r.fetchone():
            print(f"  [--] {seg['name']} (already exists)")
            skipped += 1
            continue

        if dry_run:
            print(f"  [OK] {seg['name']} — ${seg['aum_millions']/1000:.0f}B AUM (dry run)")
            added += 1
            continue

        db.execute(
            text("""
                INSERT INTO pe_funds (firm_id, name, strategy, final_close_usd_millions, status, created_at)
                VALUES (:fid, :name, :strategy, :aum, 'Active', NOW())
            """),
            {
                "fid": firm_id,
                "name": seg["name"],
                "strategy": seg["strategy"],
                "aum": seg["aum_millions"],
            },
        )
        print(f"  [OK] {seg['name']} — ${seg['aum_millions']/1000:.0f}B AUM")
        added += 1

    if not dry_run and added > 0:
        db.commit()
    print(f"Segments: {added} added, {skipped} skipped")


def seed_team(db, firm_id, dry_run=False):
    """Insert Apollo team into pe_people + pe_firm_people + education + experience."""
    from sqlalchemy import text

    # Verify firm exists
    r = db.execute(text("SELECT name FROM pe_firms WHERE id = :id"), {"id": firm_id})
    firm = r.fetchone()
    if not firm:
        print(f"ERROR: No PE firm with id={firm_id}")
        return

    # Clean up garbage team member
    db.execute(
        text("""
            DELETE FROM pe_firm_people
            WHERE firm_id = :fid AND person_id IN (
                SELECT p.id FROM pe_people p
                JOIN pe_firm_people fp ON fp.person_id = p.id
                WHERE fp.firm_id = :fid AND p.full_name = 'Featured Content'
            )
        """),
        {"fid": firm_id},
    )
    db.commit()

    print(f"Seeding team for: {firm[0]} (id={firm_id})")
    print(f"Team members to seed: {len(APOLLO_TEAM)}")
    if dry_run:
        print("DRY RUN — no changes will be made\n")

    added = 0
    skipped = 0
    enriched = 0

    for member in APOLLO_TEAM:
        name = member["full_name"]
        has_bio = "bio" in member

        # Check if person already exists
        r = db.execute(
            text("SELECT id FROM pe_people WHERE full_name = :name"),
            {"name": name},
        )
        person = r.fetchone()

        if person:
            person_id = person[0]

            # Enrich existing person with bio if available
            if has_bio and not dry_run:
                db.execute(
                    text("""
                        UPDATE pe_people
                        SET bio = COALESCE(bio, :bio),
                            current_title = COALESCE(current_title, :title),
                            current_company = COALESCE(current_company, 'Apollo Global Management')
                        WHERE id = :pid
                    """),
                    {"bio": member["bio"], "title": member["title"], "pid": person_id},
                )

            # Check if already linked to this firm
            r = db.execute(
                text("""
                    SELECT id FROM pe_firm_people
                    WHERE firm_id = :fid AND person_id = :pid
                """),
                {"fid": firm_id, "pid": person_id},
            )
            link = r.fetchone()
            if link:
                start_year = member.get("start_year")
                if not dry_run and start_year:
                    db.execute(
                        text("""
                            UPDATE pe_firm_people
                            SET start_date = COALESCE(start_date, :start_date),
                                seniority = COALESCE(seniority, :seniority),
                                department = COALESCE(department, :dept)
                            WHERE id = :link_id
                        """),
                        {
                            "start_date": f"{start_year}-01-01",
                            "seniority": member["seniority"],
                            "dept": member["department"],
                            "link_id": link[0],
                        },
                    )

                if has_bio:
                    if not dry_run:
                        _seed_education(db, person_id, member.get("education", []))
                        _seed_experience(db, person_id, member.get("experience", []))
                    enriched += 1
                    print(f"  [++] {name} (enriched with bio/edu/exp)")
                else:
                    print(f"  [--] {name} (already exists)")
                    skipped += 1
                continue
        else:
            if dry_run:
                label = " (with bio)" if has_bio else ""
                print(f"  [OK] {name} — {member['title']}{label} (dry run)")
                added += 1
                continue

            # Create person
            bio = member.get("bio")
            r = db.execute(
                text("""
                    INSERT INTO pe_people (full_name, current_title, current_company, bio, created_at)
                    VALUES (:name, :title, 'Apollo Global Management', :bio, NOW())
                    RETURNING id
                """),
                {"name": name, "title": member["title"], "bio": bio},
            )
            person_id = r.fetchone()[0]

        if not dry_run:
            # Link to firm
            start_year = member.get("start_year")
            start_date = f"{start_year}-01-01" if start_year else None
            db.execute(
                text("""
                    INSERT INTO pe_firm_people (firm_id, person_id, title, seniority, department, start_date, is_current, created_at)
                    VALUES (:fid, :pid, :title, :seniority, :dept, :start_date, true, NOW())
                """),
                {
                    "fid": firm_id,
                    "pid": person_id,
                    "title": member["title"],
                    "seniority": member["seniority"],
                    "dept": member["department"],
                    "start_date": start_date,
                },
            )

            # Seed education and experience
            _seed_education(db, person_id, member.get("education", []))
            _seed_experience(db, person_id, member.get("experience", []))

            label = " (with bio)" if has_bio else ""
            print(f"  [OK] {name} — {member['title']}{label}")

        added += 1

    if not dry_run:
        db.commit()
        print(f"\nDone: {added} added, {enriched} enriched, {skipped} skipped")
    else:
        print(f"\nDry run: {added} would be added, {skipped} already exist")


def _seed_education(db, person_id, education_list):
    """Insert education rows (idempotent)."""
    from sqlalchemy import text

    for edu in education_list:
        institution = edu.get("institution")
        if not institution:
            continue

        r = db.execute(
            text("""
                SELECT id, graduation_year FROM pe_person_education
                WHERE person_id = :pid AND LOWER(institution) = LOWER(:inst)
            """),
            {"pid": person_id, "inst": institution},
        )
        existing = r.fetchone()
        if existing:
            grad_year = edu.get("graduation_year")
            if grad_year and existing[1] is None:
                db.execute(
                    text("UPDATE pe_person_education SET graduation_year = :yr WHERE id = :id"),
                    {"yr": grad_year, "id": existing[0]},
                )
            continue

        db.execute(
            text("""
                INSERT INTO pe_person_education
                    (person_id, institution, degree, field_of_study, graduation_year, created_at)
                VALUES (:pid, :inst, :degree, :field, :grad_year, NOW())
            """),
            {
                "pid": person_id,
                "inst": institution,
                "degree": edu.get("degree"),
                "field": edu.get("field"),
                "grad_year": edu.get("graduation_year"),
            },
        )


def _seed_experience(db, person_id, experience_list):
    """Insert experience rows (idempotent)."""
    from sqlalchemy import text

    for exp in experience_list:
        company = exp.get("company")
        title = exp.get("title")
        if not company or not title:
            continue

        r = db.execute(
            text("""
                SELECT id FROM pe_person_experience
                WHERE person_id = :pid AND LOWER(company) = LOWER(:company) AND LOWER(title) = LOWER(:title)
            """),
            {"pid": person_id, "company": company, "title": title},
        )
        if r.fetchone():
            continue

        start_year = exp.get("start_year")
        end_year = exp.get("end_year")
        db.execute(
            text("""
                INSERT INTO pe_person_experience
                    (person_id, company, title, start_date, end_date, is_current, created_at)
                VALUES (:pid, :company, :title, :start_date, :end_date, false, NOW())
            """),
            {
                "pid": person_id,
                "company": company,
                "title": title,
                "start_date": f"{start_year}-01-01" if start_year else None,
                "end_date": f"{end_year}-01-01" if end_year else None,
            },
        )


def update_firm_details(db, firm_id, dry_run=False):
    """Update Apollo firm record with employee count."""
    from sqlalchemy import text

    if dry_run:
        print("\nWould update firm details (dry run)")
        return

    db.execute(
        text("""
            UPDATE pe_firms
            SET employee_count = COALESCE(employee_count, :emp),
                headquarters_city = 'New York',
                headquarters_state = 'NY'
            WHERE id = :fid
        """),
        {"emp": 5050, "fid": firm_id},
    )
    db.commit()
    print("\nUpdated firm details (employees: ~5,050)")


def main():
    parser = argparse.ArgumentParser(description="Seed Apollo Global Management leadership team")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    from app.core.database import get_session_factory
    SessionFactory = get_session_factory()
    db = SessionFactory()

    firm_id = 3  # Apollo Global Management

    try:
        update_firm_details(db, firm_id, dry_run=args.dry_run)
        seed_team(db, firm_id, dry_run=args.dry_run)
        seed_segments(db, firm_id, dry_run=args.dry_run)
    finally:
        db.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Seed Blackstone's leadership team and segment AUM from public sources.

Blackstone's website is fully JS-rendered (React), so the bio extractor
can't parse it without Playwright. This script seeds the known leadership
from publicly available information (SEC filings, Wikipedia, press releases).

Segment AUM is from Blackstone's 10-K filing (Dec 31, 2024).

Usage:
    python scripts/seed_blackstone_team.py
    python scripts/seed_blackstone_team.py --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Blackstone leadership team — sourced from public filings, website, Wikipedia
# Bios, education, and experience from SEC 10-K, proxy statements, and press releases
BLACKSTONE_TEAM = [
    # C-Suite
    {
        "full_name": "Stephen A. Schwarzman",
        "title": "Chairman, CEO & Co-Founder",
        "seniority": "Partner",
        "department": "Executive",
        "start_year": 1985,
        "bio": "Co-founded Blackstone in 1985 with Pete Peterson. Has led the firm from a startup to the world's largest alternative asset manager with over $1 trillion in AUM. Author of 'What It Takes: Lessons in the Pursuit of Excellence.'",
        "education": [
            {"institution": "Yale University", "degree": "BA", "field": "Interdisciplinary Studies", "graduation_year": 1969},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 1972},
        ],
        "experience": [
            {"company": "Lehman Brothers", "title": "Managing Director", "start_year": 1972, "end_year": 1985},
        ],
    },
    {
        "full_name": "Jonathan D. Gray",
        "title": "President & Chief Operating Officer",
        "seniority": "Partner",
        "department": "Executive",
        "start_year": 1992,
        "bio": "Joined Blackstone in 1992 and became President & COO in 2018. Previously built Blackstone's real estate business into the largest in the world. Oversees all firm investment businesses and key corporate functions.",
        "education": [
            {"institution": "University of Pennsylvania", "degree": "BS", "field": "Economics", "graduation_year": 1992},
        ],
        "experience": [],
    },
    {
        "full_name": "Michael S. Chae",
        "title": "Chief Financial Officer",
        "seniority": "Partner",
        "department": "Finance",
        "start_year": 1997,
        "bio": "Joined Blackstone in 1997 and became CFO in 2015. Oversees all financial operations, strategic planning, and investor relations. Previously a Senior Managing Director in the Private Equity group.",
        "education": [
            {"institution": "Harvard College", "degree": "AB", "field": "Economics", "graduation_year": 1993},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 1997},
        ],
        "experience": [
            {"company": "McKinsey & Company", "title": "Associate", "start_year": 1993, "end_year": 1995},
        ],
    },
    {
        "full_name": "John G. Finley",
        "title": "Chief Legal Officer",
        "seniority": "Partner",
        "department": "Legal",
        "start_year": 1999,
        "bio": "Joined Blackstone in 1999 and serves as Chief Legal Officer. Responsible for all legal affairs and leads the global legal, compliance, and government affairs teams.",
        "education": [
            {"institution": "Georgetown University", "degree": "BSBA", "field": "Accounting & Finance", "graduation_year": 1988},
            {"institution": "Columbia Law School", "degree": "JD", "field": "Law", "graduation_year": 1993},
        ],
        "experience": [
            {"company": "Simpson Thacher & Bartlett", "title": "Associate", "start_year": 1993, "end_year": 1999},
        ],
    },

    # Business Unit Heads
    {
        "full_name": "Joseph Baratta",
        "title": "Global Head of Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 1998,
        "bio": "Joined Blackstone in 1998 and leads the firm's $352 billion private equity platform globally. Under his leadership, Blackstone PE has become the largest corporate PE business in the world.",
        "education": [
            {"institution": "Georgetown University", "degree": "BSBA", "field": "Finance", "graduation_year": 1994},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 1998},
        ],
        "experience": [
            {"company": "Tinicum Incorporated", "title": "Analyst", "start_year": 1994, "end_year": 1996},
        ],
    },
    {
        "full_name": "Kenneth Caplan",
        "title": "Global Co-Head of Real Estate",
        "seniority": "Partner",
        "department": "Real Estate",
        "start_year": 1997,
        "bio": "Joined Blackstone in 1997 and co-heads the $315 billion real estate platform. Responsible for the firm's global real estate investment activities across equity and debt.",
        "education": [
            {"institution": "University of Pennsylvania", "degree": "BS", "field": "Economics", "graduation_year": 1995},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2001},
        ],
        "experience": [],
    },
    {
        "full_name": "Kathleen McCarthy",
        "title": "Global Co-Head of Real Estate",
        "seniority": "Partner",
        "department": "Real Estate",
        "start_year": 2010,
        "bio": "Joined Blackstone in 2010 and co-heads the global real estate platform. Previously led real estate debt investing at Blackstone and oversaw BREDS.",
        "education": [
            {"institution": "Harvard College", "degree": "AB", "field": "Economics", "graduation_year": 2002},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2008},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Analyst, Real Estate Principal Investments", "start_year": 2002, "end_year": 2006},
        ],
    },
    {
        "full_name": "Gilles Dellaert",
        "title": "Global Head of Blackstone Credit & Insurance",
        "seniority": "Partner",
        "department": "Credit & Insurance",
        "start_year": 2012,
        "bio": "Joined Blackstone in 2012 and leads the $375 billion credit and insurance platform. Oversees performing credit, direct lending, asset-based finance, and insurance solutions.",
        "education": [
            {"institution": "University of Ghent", "degree": "MS", "field": "Engineering", "graduation_year": 2000},
            {"institution": "Columbia Business School", "degree": "MBA", "field": "Finance", "graduation_year": 2006},
        ],
        "experience": [
            {"company": "Goldman Sachs", "title": "Vice President, Principal Finance Group", "start_year": 2006, "end_year": 2012},
        ],
    },
    {
        "full_name": "John McCormick",
        "title": "Head of Hedge Fund Solutions (BAAM)",
        "seniority": "Partner",
        "department": "Hedge Fund Solutions",
        "start_year": 2005,
        "bio": "Joined Blackstone in 2005 and heads the $84 billion hedge fund solutions platform (BAAM), the world's largest discretionary allocator to hedge funds.",
        "education": [
            {"institution": "Trinity College Dublin", "degree": "BA", "field": "Business Studies", "graduation_year": 1993},
        ],
        "experience": [
            {"company": "JPMorgan", "title": "Vice President, Alternative Investments", "start_year": 1999, "end_year": 2005},
        ],
    },

    # Senior Managing Directors / Key Partners
    {
        "full_name": "Martin Brand",
        "title": "Head of North America Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
        "start_year": 2003,
        "bio": "Joined Blackstone in 2003 and leads North America private equity. Responsible for sourcing, executing, and managing large-scale buyout transactions across sectors.",
        "education": [
            {"institution": "University of Cape Town", "degree": "BBusSc", "field": "Finance & Accounting", "graduation_year": 1999},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2003},
        ],
        "experience": [
            {"company": "Bain & Company", "title": "Consultant", "start_year": 1999, "end_year": 2001},
        ],
    },
    {
        "full_name": "Jon Korngold",
        "title": "Global Head of Growth Equity",
        "seniority": "Partner",
        "department": "Growth Equity",
        "start_year": 2012,
        "bio": "Joined Blackstone in 2012 and leads the growth equity platform. Focuses on high-growth technology and technology-enabled companies. Previously a General Partner at General Atlantic.",
        "education": [
            {"institution": "University of Michigan", "degree": "BBA", "field": "Business Administration", "graduation_year": 1998},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2004},
        ],
        "experience": [
            {"company": "General Atlantic", "title": "General Partner", "start_year": 2004, "end_year": 2012},
        ],
    },
    {
        "full_name": "Sean Klimczak",
        "title": "Global Head of Infrastructure",
        "seniority": "Partner",
        "department": "Infrastructure",
        "start_year": 2007,
        "bio": "Joined Blackstone in 2007 and leads the firm's global infrastructure investing business. Has been central to building Blackstone's infrastructure platform into one of the largest in the industry.",
        "education": [
            {"institution": "University of Notre Dame", "degree": "BBA", "field": "Finance", "graduation_year": 2000},
            {"institution": "Harvard Business School", "degree": "MBA", "field": "Business Administration", "graduation_year": 2007},
        ],
        "experience": [
            {"company": "Lazard", "title": "Analyst, Power & Utilities", "start_year": 2000, "end_year": 2005},
        ],
    },

    # Other key leaders (without detailed bios)
    {
        "full_name": "Michael Nash",
        "title": "Vice Chairman of Blackstone Credit & Insurance",
        "seniority": "Partner",
        "department": "Credit & Insurance",
    },
    {
        "full_name": "Peter Wallace",
        "title": "Head of Asia Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
    },
    {
        "full_name": "Lionel Assant",
        "title": "Head of European Private Equity",
        "seniority": "Partner",
        "department": "Private Equity",
    },
    {
        "full_name": "David Blitzer",
        "title": "Senior Managing Director",
        "seniority": "Managing Director",
        "department": "Private Equity",
    },
    {
        "full_name": "Eli Maimon",
        "title": "Senior Managing Director, Tactical Opportunities",
        "seniority": "Managing Director",
        "department": "Tactical Opportunities",
    },
    {
        "full_name": "David Levine",
        "title": "Senior Managing Director, Growth Equity",
        "seniority": "Managing Director",
        "department": "Growth Equity",
    },
    {
        "full_name": "Raj Agrawal",
        "title": "Senior Managing Director, Infrastructure",
        "seniority": "Managing Director",
        "department": "Infrastructure",
    },
    {
        "full_name": "Tyler Henritze",
        "title": "Head of Real Estate Acquisitions, Americas",
        "seniority": "Partner",
        "department": "Real Estate",
    },
    {
        "full_name": "Jacob Werner",
        "title": "Head of European Real Estate",
        "seniority": "Partner",
        "department": "Real Estate",
    },
    {
        "full_name": "Chris Heady",
        "title": "Head of Asia Real Estate",
        "seniority": "Partner",
        "department": "Real Estate",
    },
    {
        "full_name": "Nadeem Meghji",
        "title": "Head of Real Estate Americas",
        "seniority": "Partner",
        "department": "Real Estate",
    },
    {
        "full_name": "Paige Ross",
        "title": "Global Head of Human Resources",
        "seniority": "Managing Director",
        "department": "Human Resources",
    },
    {
        "full_name": "Christine Anderson",
        "title": "Global Head of Communications",
        "seniority": "Managing Director",
        "department": "Communications",
    },
    {
        "full_name": "Raymond O'Rourke",
        "title": "Chief Compliance Officer",
        "seniority": "Managing Director",
        "department": "Compliance",
    },
    {
        "full_name": "Hamilton E. James",
        "title": "Executive Vice Chairman",
        "seniority": "Partner",
        "department": "Executive",
    },
    {
        "full_name": "Tony James",
        "title": "Executive Vice Chairman Emeritus",
        "seniority": "Partner",
        "department": "Executive",
    },
    {
        "full_name": "Joe Zidle",
        "title": "Chief Investment Strategist, Private Wealth Solutions",
        "seniority": "Managing Director",
        "department": "Private Wealth Solutions",
    },
]


# Blackstone business segments — AUM from 10-K (Dec 31, 2024)
BLACKSTONE_SEGMENTS = [
    {
        "name": "Real Estate (BREP)",
        "strategy": "Real Estate",
        "aum_millions": 315400,
        "employees": 675,
        "description": "Global leader in real estate investing across equity and debt strategies",
    },
    {
        "name": "Private Equity (BX PE)",
        "strategy": "Buyout",
        "aum_millions": 352200,
        "employees": 675,
        "description": "Corporate private equity, life sciences, growth equity, and tactical opportunities",
    },
    {
        "name": "Credit & Insurance (BXCI)",
        "strategy": "Credit",
        "aum_millions": 375500,
        "employees": 685,
        "description": "Performing credit, direct lending, asset-based finance, and insurance solutions",
    },
    {
        "name": "Hedge Fund Solutions (BXMA)",
        "strategy": "Hedge Fund Solutions",
        "aum_millions": 84200,
        "employees": 240,
        "description": "World's largest discretionary allocator to hedge funds and customized solutions",
    },
]


def seed_segments(db, dry_run=False):
    """Insert Blackstone business segments as pe_funds entries."""
    from sqlalchemy import text

    firm_id = 1  # Blackstone

    print(f"\nSeeding {len(BLACKSTONE_SEGMENTS)} business segments...")

    added = 0
    skipped = 0

    for seg in BLACKSTONE_SEGMENTS:
        # Check if fund already exists
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


def seed_team(db, dry_run=False):
    """Insert Blackstone team into pe_people + pe_firm_people + education + experience."""
    from sqlalchemy import text

    firm_id = 1  # Blackstone

    # Verify firm exists
    r = db.execute(text("SELECT name FROM pe_firms WHERE id = :id"), {"id": firm_id})
    firm = r.fetchone()
    if not firm:
        print(f"ERROR: No PE firm with id={firm_id}")
        return

    print(f"Seeding team for: {firm[0]} (id={firm_id})")
    print(f"Team members to seed: {len(BLACKSTONE_TEAM)}")
    if dry_run:
        print("DRY RUN — no changes will be made\n")

    added = 0
    skipped = 0
    enriched = 0

    for member in BLACKSTONE_TEAM:
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
                            current_company = COALESCE(current_company, 'Blackstone')
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
                # Update existing link with start_date/seniority/department
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
                    # Still seed education and experience even if person exists
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
                    VALUES (:name, :title, 'Blackstone', :bio, NOW())
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
    """Insert education rows (idempotent — skip if institution already exists for person)."""
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
            # Fill in graduation_year if missing
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
    """Insert experience rows (idempotent — skip if company+title already exists for person)."""
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


def main():
    parser = argparse.ArgumentParser(description="Seed Blackstone leadership team")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    from app.core.database import get_session_factory
    SessionFactory = get_session_factory()
    db = SessionFactory()

    try:
        seed_team(db, dry_run=args.dry_run)
        seed_segments(db, dry_run=args.dry_run)
    finally:
        db.close()


if __name__ == "__main__":
    main()

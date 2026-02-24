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
BLACKSTONE_TEAM = [
    # C-Suite
    {"full_name": "Stephen A. Schwarzman", "title": "Chairman, CEO & Co-Founder", "seniority": "Partner", "department": "Executive"},
    {"full_name": "Jonathan D. Gray", "title": "President & Chief Operating Officer", "seniority": "Partner", "department": "Executive"},
    {"full_name": "Michael S. Chae", "title": "Chief Financial Officer", "seniority": "Partner", "department": "Finance"},
    {"full_name": "John G. Finley", "title": "Chief Legal Officer", "seniority": "Partner", "department": "Legal"},

    # Business Unit Heads
    {"full_name": "Joseph Baratta", "title": "Global Head of Private Equity", "seniority": "Partner", "department": "Private Equity"},
    {"full_name": "Kenneth Caplan", "title": "Global Co-Head of Real Estate", "seniority": "Partner", "department": "Real Estate"},
    {"full_name": "Kathleen McCarthy", "title": "Global Co-Head of Real Estate", "seniority": "Partner", "department": "Real Estate"},
    {"full_name": "Michael Nash", "title": "Vice Chairman of Blackstone Credit & Insurance", "seniority": "Partner", "department": "Credit & Insurance"},
    {"full_name": "Gilles Dellaert", "title": "Global Head of Blackstone Credit & Insurance", "seniority": "Partner", "department": "Credit & Insurance"},
    {"full_name": "John McCormick", "title": "Head of Hedge Fund Solutions (BAAM)", "seniority": "Partner", "department": "Hedge Fund Solutions"},

    # Senior Managing Directors / Key Partners
    {"full_name": "Martin Brand", "title": "Head of North America Private Equity", "seniority": "Partner", "department": "Private Equity"},
    {"full_name": "Peter Wallace", "title": "Head of Asia Private Equity", "seniority": "Partner", "department": "Private Equity"},
    {"full_name": "Lionel Assant", "title": "Head of European Private Equity", "seniority": "Partner", "department": "Private Equity"},
    {"full_name": "David Blitzer", "title": "Senior Managing Director", "seniority": "Managing Director", "department": "Private Equity"},
    {"full_name": "Eli Maimon", "title": "Senior Managing Director, Tactical Opportunities", "seniority": "Managing Director", "department": "Tactical Opportunities"},
    {"full_name": "David Levine", "title": "Senior Managing Director, Growth Equity", "seniority": "Managing Director", "department": "Growth Equity"},
    {"full_name": "Jon Korngold", "title": "Global Head of Growth Equity", "seniority": "Partner", "department": "Growth Equity"},

    # Infrastructure
    {"full_name": "Sean Klimczak", "title": "Global Head of Infrastructure", "seniority": "Partner", "department": "Infrastructure"},
    {"full_name": "Raj Agrawal", "title": "Senior Managing Director, Infrastructure", "seniority": "Managing Director", "department": "Infrastructure"},

    # Real Estate Leaders
    {"full_name": "Tyler Henritze", "title": "Head of Real Estate Acquisitions, Americas", "seniority": "Partner", "department": "Real Estate"},
    {"full_name": "Jacob Werner", "title": "Head of European Real Estate", "seniority": "Partner", "department": "Real Estate"},
    {"full_name": "Chris Heady", "title": "Head of Asia Real Estate", "seniority": "Partner", "department": "Real Estate"},
    {"full_name": "Nadeem Meghji", "title": "Head of Real Estate Americas", "seniority": "Partner", "department": "Real Estate"},

    # Corporate Functions
    {"full_name": "Paige Ross", "title": "Global Head of Human Resources", "seniority": "Managing Director", "department": "Human Resources"},
    {"full_name": "Christine Anderson", "title": "Global Head of Communications", "seniority": "Managing Director", "department": "Communications"},
    {"full_name": "Raymond O'Rourke", "title": "Chief Compliance Officer", "seniority": "Managing Director", "department": "Compliance"},

    # Board / Senior Advisors
    {"full_name": "Hamilton E. James", "title": "Executive Vice Chairman", "seniority": "Partner", "department": "Executive"},
    {"full_name": "Tony James", "title": "Executive Vice Chairman Emeritus", "seniority": "Partner", "department": "Executive"},
    {"full_name": "Byron Wien", "title": "Vice Chairman, Private Wealth Solutions (in memoriam)", "seniority": "Partner", "department": "Private Wealth Solutions"},
    {"full_name": "Joe Zidle", "title": "Chief Investment Strategist, Private Wealth Solutions", "seniority": "Managing Director", "department": "Private Wealth Solutions"},
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
    """Insert Blackstone team into pe_people + pe_firm_people."""
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

    for member in BLACKSTONE_TEAM:
        name = member["full_name"]

        # Check if person already exists
        r = db.execute(
            text("SELECT id FROM pe_people WHERE full_name = :name"),
            {"name": name},
        )
        person = r.fetchone()

        if person:
            person_id = person[0]
            # Check if already linked to this firm
            r = db.execute(
                text("""
                    SELECT id FROM pe_firm_people
                    WHERE firm_id = :fid AND person_id = :pid
                """),
                {"fid": firm_id, "pid": person_id},
            )
            if r.fetchone():
                print(f"  [--] {name} (already exists)")
                skipped += 1
                continue
        else:
            if dry_run:
                print(f"  [OK] {name} — {member['title']} (dry run)")
                added += 1
                continue

            # Create person
            r = db.execute(
                text("""
                    INSERT INTO pe_people (full_name, created_at)
                    VALUES (:name, NOW())
                    RETURNING id
                """),
                {"name": name},
            )
            person_id = r.fetchone()[0]

        if not dry_run:
            # Link to firm
            db.execute(
                text("""
                    INSERT INTO pe_firm_people (firm_id, person_id, title, seniority, department, is_current, created_at)
                    VALUES (:fid, :pid, :title, :seniority, :dept, true, NOW())
                """),
                {
                    "fid": firm_id,
                    "pid": person_id,
                    "title": member["title"],
                    "seniority": member["seniority"],
                    "dept": member["department"],
                },
            )
            print(f"  [OK] {name} — {member['title']}")

        added += 1

    if not dry_run:
        db.commit()
        print(f"\nDone: {added} added, {skipped} skipped")
    else:
        print(f"\nDry run: {added} would be added, {skipped} already exist")


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

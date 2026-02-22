"""Upsert First Citizens officers extracted from SEC DEF 14A proxy into the database."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.database import get_engine, get_session_factory
from app.core.people_models import Person, CompanyPerson

COMPANY_ID = 159

# Officers extracted from DEF 14A proxy (March 2025)
officers = [
    {"name": "Frank B. Holding, Jr.", "title": "Chairman and Chief Executive Officer", "department": "Corporate", "is_board": True, "is_exec": True, "start_year": 2009},
    {"name": "Craig L. Nix", "title": "Chief Financial Officer", "department": "Finance", "is_board": False, "is_exec": True, "start_year": None},
    {"name": "Hope Holding Bryant", "title": "Vice Chairwoman", "department": "General Bank", "is_board": True, "is_exec": True, "start_year": None},
    {"name": "Peter M. Bristow", "title": "President", "department": "Corporate", "is_board": True, "is_exec": True, "age": 59, "start_year": None},
    {"name": "Lorie K. Rupp", "title": "Chief Risk Officer", "department": "Risk Management", "is_board": False, "is_exec": True, "start_year": None},
    {"name": "Matthew G.T. Martin", "title": "Corporate Secretary and General Counsel", "department": "Legal", "is_board": False, "is_exec": True, "start_year": None},
    {"name": "Ellen R. Alemany", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False, "age": 69, "bio": "Retired; Former Chairwoman and CEO of CIT Group."},
    {"name": "Victor E. Bell III", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False, "age": 68, "bio": "Chairman and President of Marjan, Ltd."},
    {"name": "H. Lee Durham, Jr.", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False, "start_year": None},
    {"name": "David G. Leitch", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False, "bio": "Retired legal counsel; former Global General Counsel, Bank of America Corporation."},
    {"name": "Robert E. Mason IV", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False, "age": 66},
    {"name": "Robert T. Newcomb", "title": "Lead Independent Director", "department": "Corporate", "is_board": True, "is_exec": False},
    {"name": "Dr. Eugene Flood, Jr.", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False},
    {"name": "Robert R. Hoppe", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False},
    {"name": "R. Mattox Snow III", "title": "Director", "department": "Corporate", "is_board": True, "is_exec": False},
]

SessionFactory = get_session_factory()

created = 0
updated = 0

with SessionFactory() as session:
    for off in officers:
        name = off["name"]
        # Check if person exists
        person = session.query(Person).filter(Person.full_name == name).first()

        if not person:
            person = Person(full_name=name, bio=off.get("bio"))
            session.add(person)
            session.flush()
            created += 1
            print(f"  CREATED: {name} (id={person.id})")
        else:
            # Update bio if we have one and they don't
            if off.get("bio") and not person.bio:
                person.bio = off["bio"]
            updated += 1
            print(f"  EXISTS:  {name} (id={person.id})")

        # Check/update company_people link
        cp = session.query(CompanyPerson).filter(
            CompanyPerson.person_id == person.id,
            CompanyPerson.company_id == COMPANY_ID,
        ).first()

        if not cp:
            cp = CompanyPerson(
                person_id=person.id,
                company_id=COMPANY_ID,
                title=off["title"],
                department=off.get("department"),
                is_board_member=off.get("is_board", False),
                source="sec_def14a",
            )
            session.add(cp)
            print(f"    + Linked to company as {off['title']}")
        else:
            # Update title if different
            if cp.title != off["title"]:
                print(f"    ~ Updated title: {cp.title} -> {off['title']}")
                cp.title = off["title"]
            if off.get("department") and cp.department != off.get("department"):
                cp.department = off["department"]

    session.commit()

print(f"\nDone: {created} created, {updated} updated")

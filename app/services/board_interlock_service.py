"""
Board Interlock Service.

Computes pairwise co-director relationships from BoardSeat data.
"""

import logging
from typing import List, Dict
from itertools import combinations
from sqlalchemy.orm import Session

from app.core.people_models import Person

logger = logging.getLogger(__name__)


class BoardInterlockService:

    def compute_interlocks_for_company(self, company_name: str, db: Session) -> int:
        """
        For all current board members of company_name, find their other board seats
        and create BoardInterlock records for each pair.
        Returns count of interlocks created/updated.
        """
        from app.core.people_models import BoardSeat, BoardInterlock
        seats = (
            db.query(BoardSeat)
            .filter(BoardSeat.company_name == company_name, BoardSeat.is_current == True)
            .all()
        )
        if len(seats) < 2:
            return 0

        person_ids = [s.person_id for s in seats]
        count = 0

        for pid_a, pid_b in combinations(person_ids, 2):
            seats_a = {s.company_name for s in
                       db.query(BoardSeat).filter(BoardSeat.person_id == pid_a, BoardSeat.is_current == True).all()}
            seats_b = {s.company_name for s in
                       db.query(BoardSeat).filter(BoardSeat.person_id == pid_b, BoardSeat.is_current == True).all()}
            shared = seats_a & seats_b

            for shared_co in shared:
                existing = (
                    db.query(BoardInterlock)
                    .filter(
                        BoardInterlock.person_id_a == min(pid_a, pid_b),
                        BoardInterlock.person_id_b == max(pid_a, pid_b),
                        BoardInterlock.shared_company == shared_co,
                    )
                    .first()
                )
                if not existing:
                    db.add(BoardInterlock(
                        person_id_a=min(pid_a, pid_b),
                        person_id_b=max(pid_a, pid_b),
                        shared_company=shared_co,
                        is_current=True,
                    ))
                    count += 1

        db.commit()
        return count

    def get_network_graph(self, company_id: int, db: Session) -> dict:
        """
        Return nodes + edges for the board network centered on a company.
        """
        from app.core.people_models import BoardSeat, BoardInterlock
        from sqlalchemy import or_

        board_pids = [
            r[0] for r in
            db.query(BoardSeat.person_id)
            .filter(BoardSeat.company_id == company_id, BoardSeat.is_current == True)
            .all()
        ]
        if not board_pids:
            return {"nodes": [], "edges": [], "stats": {"total_nodes": 0, "total_edges": 0}}

        interlocks = (
            db.query(BoardInterlock)
            .filter(
                or_(
                    BoardInterlock.person_id_a.in_(board_pids),
                    BoardInterlock.person_id_b.in_(board_pids),
                ),
                BoardInterlock.is_current == True,
            )
            .all()
        )

        all_pids = set(board_pids)
        for il in interlocks:
            all_pids.add(il.person_id_a)
            all_pids.add(il.person_id_b)

        people = {p.id: p for p in db.query(Person).filter(Person.id.in_(all_pids)).all()}
        nodes = [
            {"id": pid, "name": people[pid].full_name if pid in people else f"Person {pid}",
             "is_center_board": pid in board_pids}
            for pid in all_pids
        ]
        edges = [
            {"source": il.person_id_a, "target": il.person_id_b,
             "shared_company": il.shared_company}
            for il in interlocks
        ]
        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {"total_nodes": len(nodes), "total_edges": len(edges)},
        }

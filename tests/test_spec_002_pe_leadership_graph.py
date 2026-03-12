"""
Tests for SPEC 002 — PE Leadership Graph
Covers: leadership seed data, leadership graph endpoint, data integrity.
"""
import pytest
from unittest.mock import MagicMock

from app.sources.pe.demo_seeder import (
    COMPANY_EXECUTIVES,
    COMPANY_LEADERSHIP_MAP,
    BOARD_SEATS,
    PORTFOLIO_COMPANIES,
    PEOPLE,
    PERSON_FIRM_MAP,
    _get_person_firm,
)


# =============================================================================
# Test Leadership Seed Data Integrity
# =============================================================================


class TestLeadershipSeedData:
    """Tests for the PE leadership seed data."""

    def test_executive_count(self):
        """T1: At least 48 executives defined (2 per company * 24 companies)."""
        assert len(COMPANY_EXECUTIVES) >= 48

    def test_every_company_has_leadership(self):
        """T2: Every portfolio company has at least 2 leadership records."""
        all_company_names = set()
        for firm_name, companies in PORTFOLIO_COMPANIES.items():
            for co in companies:
                all_company_names.add(co["name"])

        for co_name in all_company_names:
            assert co_name in COMPANY_LEADERSHIP_MAP, (
                f"Missing leadership map for {co_name}"
            )
            leaders = COMPANY_LEADERSHIP_MAP[co_name]
            assert len(leaders) >= 2, (
                f"{co_name} should have at least 2 leaders, got {len(leaders)}"
            )

    def test_every_company_has_ceo_and_cfo(self):
        """T3: Every company has both a CEO and CFO."""
        for co_name, leaders in COMPANY_LEADERSHIP_MAP.items():
            has_ceo = any(l[3] for l in leaders)  # is_ceo flag
            has_cfo = any(l[4] for l in leaders)  # is_cfo flag
            assert has_ceo, f"{co_name} missing CEO"
            assert has_cfo, f"{co_name} missing CFO"

    def test_executives_reference_valid_people(self):
        """T4: All COMPANY_LEADERSHIP_MAP person names exist in COMPANY_EXECUTIVES."""
        exec_names = {e["full_name"] for e in COMPANY_EXECUTIVES}
        for co_name, leaders in COMPANY_LEADERSHIP_MAP.items():
            for (person_name, *_rest) in leaders:
                assert person_name in exec_names, (
                    f"Leadership map references unknown person: {person_name}"
                )

    def test_board_seats_reference_valid_people_and_companies(self):
        """T5: Board seats reference people in PERSON_FIRM_MAP and companies in PORTFOLIO_COMPANIES."""
        pe_people_names = set(PERSON_FIRM_MAP.keys())
        all_company_names = set()
        for firm_name, companies in PORTFOLIO_COMPANIES.items():
            for co in companies:
                all_company_names.add(co["name"])

        for (person_name, co_name, title, is_chair) in BOARD_SEATS:
            assert person_name in pe_people_names, (
                f"Board seat references unknown PE person: {person_name}"
            )
            assert co_name in all_company_names, (
                f"Board seat references unknown company: {co_name}"
            )

    def test_board_seat_count(self):
        """T6: At least 20 board seats across the 3 firms."""
        assert len(BOARD_SEATS) >= 20

    def test_executive_data_fields(self):
        """T7: Each executive has required fields."""
        for exec_data in COMPANY_EXECUTIVES:
            assert exec_data["full_name"]
            assert exec_data["first_name"]
            assert exec_data["last_name"]
            assert exec_data["current_title"]
            assert exec_data["current_company"]
            assert exec_data["city"]
            assert exec_data["state"]
            assert "bio" in exec_data

    def test_leadership_role_categories(self):
        """T8: All leadership records have valid role categories."""
        valid_categories = {"C-Suite", "Board", "VP", "Director"}
        for co_name, leaders in COMPANY_LEADERSHIP_MAP.items():
            for (person_name, title, role_cat, *_rest) in leaders:
                assert role_cat in valid_categories, (
                    f"Invalid role_category '{role_cat}' for {person_name} at {co_name}"
                )

    def test_get_person_firm_helper(self):
        """T9: _get_person_firm returns correct firm for known people."""
        assert _get_person_firm("James Harrington") == "Summit Ridge Partners"
        assert _get_person_firm("Elena Vasquez") == "Cascade Growth Equity"
        assert _get_person_firm("William Blackwell") == "Ironforge Industrial Capital"
        assert _get_person_firm("Unknown Person") is None

    def test_no_duplicate_executives(self):
        """T10: No duplicate executive names."""
        names = [e["full_name"] for e in COMPANY_EXECUTIVES]
        assert len(names) == len(set(names)), "Duplicate executive names found"

    def test_board_seats_have_chairs(self):
        """T11: At least one board chair per firm's portfolio."""
        chairs = [b for b in BOARD_SEATS if b[3] is True]
        assert len(chairs) >= 3, "Should have at least 3 board chairs (1 per firm)"

    def test_pe_appointed_leaders_exist(self):
        """T12: Some executives are PE-appointed."""
        pe_appointed = 0
        for co_name, leaders in COMPANY_LEADERSHIP_MAP.items():
            for (_name, _title, _cat, _ceo, _cfo, _board, appointed, _affil) in leaders:
                if appointed:
                    pe_appointed += 1
        assert pe_appointed >= 10, (
            f"Expected at least 10 PE-appointed leaders, got {pe_appointed}"
        )


# =============================================================================
# Test Leadership Graph Response Structure
# =============================================================================


class TestLeadershipGraphEndpoint:
    """Tests for the leadership graph API response model."""

    def test_graph_response_model(self):
        """T13: GraphNodeResponse and GraphLinkResponse models work correctly."""
        from app.api.v1.pe_benchmarks import (
            GraphNodeResponse,
            GraphLinkResponse,
            LeadershipGraphResponse,
        )

        node = GraphNodeResponse(
            id="firm_1", name="Test Firm", type="firm",
        )
        assert node.id == "firm_1"
        assert node.type == "firm"
        assert node.title is None

        link = GraphLinkResponse(
            source="firm_1", target="person_1", type="employment",
        )
        assert link.source == "firm_1"
        assert link.type == "employment"

        graph = LeadershipGraphResponse(
            firm_id=1, firm_name="Test Firm",
            nodes=[node], links=[link],
        )
        assert graph.firm_id == 1
        assert len(graph.nodes) == 1
        assert len(graph.links) == 1

    def test_graph_node_types(self):
        """T14: All 4 node types are valid."""
        from app.api.v1.pe_benchmarks import GraphNodeResponse

        for node_type in ["firm", "pe_person", "company", "executive"]:
            node = GraphNodeResponse(
                id=f"test_{node_type}", name="Test", type=node_type,
            )
            assert node.type == node_type

    def test_graph_link_types(self):
        """T15: All 3 link types are valid."""
        from app.api.v1.pe_benchmarks import GraphLinkResponse

        for link_type in ["employment", "board_seat", "management"]:
            link = GraphLinkResponse(
                source="a", target="b", type=link_type,
            )
            assert link.type == link_type

    def test_total_seed_counts(self):
        """T16: Verify total data volume is sufficient for demo."""
        # PE firm people (existing)
        assert len(PEOPLE) >= 20

        # Company executives (new)
        assert len(COMPANY_EXECUTIVES) >= 48

        # All portfolio companies covered
        total_companies = sum(len(cos) for cos in PORTFOLIO_COMPANIES.values())
        assert total_companies == 24

        # Leadership records cover all companies
        assert len(COMPANY_LEADERSHIP_MAP) == 24

        # Board seats create cross-links
        assert len(BOARD_SEATS) >= 20

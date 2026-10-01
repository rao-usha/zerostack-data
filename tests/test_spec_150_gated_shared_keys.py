"""
Tests for SPEC 150 — gated shared keys: an EIN or CRD alone never joins two EDGAR filers.

Pure tests drive `resolve_core.plan()` with filer profiles; the PG test needs TEST_PG_URL
pointing at a DISPOSABLE database.
"""

import importlib.util
import os
import random
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

ACTIVE = ("2024-01-15", "2026-09-20")      # (first, last) of a filer still filing


def _rec(record_key, **kw):
    from app.entities import norm
    from app.entities.resolve_core import _rec as base

    if kw.get("legal_name") and "name_norm" not in kw:
        kw["name_norm"] = norm.norm(kw["legal_name"])
    return base(record_key, **kw)


def _prof(name, first=ACTIVE[0], last=ACTIVE[1], **kw):
    p = {"name": name, "former_names": [], "sic": None, "first_filed": first,
         "last_filed": last, "latest_form": None, "tickers": None, "insider_owner": False,
         "insider_issuer": False, "owner_filings": 0, "formd_filings": 0,
         "formd_pooled": False, "f13_filings": 0, "k10_filings": 0}
    p.update(kw)
    return p


def _filer(cik, name, ein=None, crd=None, twin=True):
    """An EDGAR record for `cik` (+ a Form D twin so a lone CIK still materializes)."""
    pad = str(cik).zfill(10)
    out = [_rec(f"edgar:{pad}", cik=pad, ein=ein, legal_name=name)]
    if crd:
        out.append(_rec(f"f13:{pad}", cik=pad, crd=crd, legal_name=name))
    elif twin:
        out.append(_rec(f"formd:{pad}", cik=pad, legal_name=name))
    return out


def _plan(records, profiles, bridge=(), vetoes=None):
    from app.entities.resolve_core import plan

    return plan(records, list(bridge), vetoes or {}, profiles=profiles)


def _comp_of(p, record_key):
    for i, c in enumerate(p["components"]):
        if record_key in c["members"]:
            return i
    return None


def _together(p, a, b):
    ca, cb = _comp_of(p, a), _comp_of(p, b)
    return ca is not None and ca == cb


def _gate_rules(p, idx):
    return [j["rule"] for j in (p["components"][idx].get("gate") or {}).get("joined", [])]


def _gate_refusals(p, idx):
    return [r["reason"] for r in (p["components"][idx].get("gate") or {}).get("refused", [])]


E1 = "261640968"
E2 = "043460239"


# ---------------------------------------------------------------------------
# T1 name normalization
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEdgarNameNorm:
    def test_edgar_name_norm(self):
        from app.entities import gate

        assert gate.enorm("FOCUS FINANCIAL NETWORK INC /ADV") == gate.enorm("Focus Financial Network Inc")
        assert gate.enorm("Rothschild Investment LLC /IL") == gate.enorm("Rothschild Investment LLC")
        assert gate.enorm("ACME CORP /NY/") == gate.enorm("Acme Corp")
        assert gate.enorm("Kagin's Digital, Inc.") == gate.enorm("Kagins Digital LLC")
        assert gate.legal_form("WILD TREE VENTURES, L.P.") == "lp"
        assert gate.legal_form("Wild Tree Ventures Limited Partnership") == "lp"
        assert gate.legal_form("Wild Tree Ventures") is None
        assert gate.legal_form("Teton Advisors, L.L.C.") == "llc"
        # designators are kept: Fund I and Fund II are two names
        assert gate.enorm("Donum Fund I LP") != gate.enorm("Donum Fund II LP")
        # roman numerals equal digits in the equality forms
        assert gate.name_keys(gate.enorm("Donum Charitable Lending Fund I LP"), True) & \
            gate.name_keys(gate.enorm("Donum Charitable Lending Fund 1 LLC"), True)
        # the sorted token set is NOT an equality form for vehicles
        assert not (gate.name_keys("series a nukudo", True) & gate.name_keys("nukudo series a", True))
        assert gate.name_keys("capital e2", False) & gate.name_keys("e2 capital", False)


# ---------------------------------------------------------------------------
# kept: /ADV duplicate, rename, legal-form conversion
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestKept:
    def test_adv_duplicate_kept(self):
        """T2"""
        recs = _filer(13, "FOCUS FINANCIAL NETWORK INC /ADV", ein=E1) + \
            _filer(14, "Focus Financial Network Inc", ein=E1)
        prof = {"13": _prof("FOCUS FINANCIAL NETWORK INC /ADV"), "14": _prof("Focus Financial Network Inc")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1
        assert _gate_rules(p, 0) == ["R1_name_equal"]
        assert p["components"][0]["keys"].count(("ein", E1)) == 1

    def test_rename_kept(self):
        """T3: a dormant CIK and its successor whose former name is the old name"""
        recs = _filer(10, "HoyleCohen, LLC", ein=E1) + _filer(19, "INKWELL CAPITAL LLC", ein=E1)
        prof = {"10": _prof("HoyleCohen, LLC", first="2019-03-01", last="2024-11-15"),
                "19": _prof("INKWELL CAPITAL LLC", first="2025-02-01", last="2026-09-01",
                            former_names=["HoyleCohen, LLC"])}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1
        assert _gate_rules(p, 0) == ["R2_former_name_succession"]

    def test_form_conversion_kept_and_side_by_side_split(self):
        """T4"""
        recs = _filer(20, "KMX Technologies LLC", ein=E1) + _filer(21, "KMX Technologies, Inc.", ein=E1)
        prof = {"20": _prof("KMX Technologies LLC", first="2021-01-01", last="2023-09-01"),
                "21": _prof("KMX Technologies, Inc.", first="2023-11-01", last="2026-08-01")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1 and _gate_rules(p, 0) == ["R1c_form_conversion"]

        recs = _filer(24, "Gates Capital Management Inc", ein=E2) + \
            _filer(25, "Gates Capital Management LP", ein=E2)
        prof = {"24": _prof("Gates Capital Management Inc", first="2015-01-01"),
                "25": _prof("Gates Capital Management LP", first="2015-02-01")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert not _together(p, "edgar:0000000024", "edgar:0000000025")
        assert "V2_form_conflict" in _gate_refusals(p, 0)


# ---------------------------------------------------------------------------
# split: subsidiary, insider, separate accounts, ESOP
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSplit:
    def test_subsidiary_split(self):
        """T5: parent + subsidiary on one EIN, both filing; a concurrent former-name match
        (a holding-company reorg) is not a succession either"""
        recs = _filer(30, "Park-Ohio Holdings Corp", ein=E1) + _filer(31, "Park-Ohio Industries Inc", ein=E1)
        prof = {"30": _prof("Park-Ohio Holdings Corp", sic="3460", k10_filings=20),
                "31": _prof("Park-Ohio Industries Inc", sic="3460", k10_filings=20)}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert "no_corroboration" in _gate_refusals(p, 0)

        recs = _filer(32, "NEWCO HOLDINGS INC", ein=E2) + _filer(35, "NEWCO OPERATING LLC", ein=E2)
        prof = {"32": _prof("NEWCO HOLDINGS INC", sic="2800", k10_filings=3),
                "35": _prof("NEWCO OPERATING LLC", sic="2800", k10_filings=9,
                            former_names=["NEWCO HOLDINGS INC"])}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert "R2_concurrent" in _gate_refusals(p, 0)

    def test_insider_split(self):
        """T6: a reporting person filed under the company's EIN never joins it (V1)"""
        recs = _filer(40, "Burnham & Co LLC", ein=E1) + _filer(41, "TRASK ADAM ROLAND", ein=E1)
        prof = {"40": _prof("Burnham & Co LLC", f13_filings=12),
                "41": _prof("TRASK ADAM ROLAND", insider_owner=True, owner_filings=4)}
        p = _plan(recs, prof)
        assert not _together(p, "edgar:0000000040", "edgar:0000000041")
        assert "V1_person" in _gate_refusals(p, _comp_of(p, "edgar:0000000041"))
        # ... and the same via a shared CRD on the 13F cover page
        recs = _filer(42, "Avenue Capital Management II, L.P.", crd="556") + \
            _filer(43, "LASRY MARC", crd="556")
        prof = {"42": _prof("Avenue Capital Management II, L.P.", f13_filings=40),
                "43": _prof("LASRY MARC", f13_filings=40, insider_owner=True)}
        p = _plan(recs, prof)
        assert not _together(p, "edgar:0000000042", "edgar:0000000043")

    def test_separate_account_split(self):
        """T7: an insurer's separate accounts share its EIN; none joins (V3), and the EIN is
        owned by the one operating piece"""
        recs = (_filer(50, "Zurich American Life Insurance Co", ein=E1)
                + _filer(51, "ZALICO Variable Annuity Separate Account", ein=E1)
                + _filer(52, "ZALICO Variable Life Separate Account", ein=E1))
        prof = {"50": _prof("Zurich American Life Insurance Co", sic="6311"),
                "51": _prof("ZALICO Variable Annuity Separate Account"),
                "52": _prof("ZALICO Variable Life Separate Account")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 3
        owner = _comp_of(p, "edgar:0000000050")
        for i, c in enumerate(p["components"]):
            assert (("ein", E1) in c["keys"]) == (i == owner)
        acct = p["components"][_comp_of(p, "edgar:0000000051")]
        assert ("ein", E1) in acct["withheld"]
        assert "V3_vehicle" in _gate_refusals(p, _comp_of(p, "edgar:0000000051"))

    def test_esop_split(self):
        """T8: a company and its ESOP share an EIN; the Form 5500 sponsor joins the company"""
        recs = (_filer(60, "Acme Widgets Corp", ein=E1)
                + _filer(61, "Acme Widgets Corp Employee Stock Ownership Plan", ein=E1)
                + [_rec(f"dol5500:{E1}", ein=E1, legal_name="ACME WIDGETS CORP")])
        prof = {"60": _prof("Acme Widgets Corp", sic="3560", k10_filings=5),
                "61": _prof("Acme Widgets Corp Employee Stock Ownership Plan")}
        p = _plan(recs, prof)
        assert not _together(p, "edgar:0000000060", "edgar:0000000061")
        assert _together(p, "edgar:0000000060", f"dol5500:{E1}")
        company = p["components"][_comp_of(p, "edgar:0000000060")]
        assert ("ein", E1) in company["keys"]
        assert ("ein", E1) not in p["components"][_comp_of(p, "edgar:0000000061")]["keys"]


# ---------------------------------------------------------------------------
# shared CRD (13F cover page and bridge edges)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSharedCrd:
    def test_shared_crd_rules(self):
        """T9"""
        # R3: one adviser, a strategy-labelled 13F CIK
        recs = _filer(70, "Alpha Beta Capital LP", crd="701") + \
            _filer(71, "Alpha Beta Capital Long Short LLC", crd="701")
        prof = {"70": _prof("Alpha Beta Capital LP", f13_filings=10),
                "71": _prof("Alpha Beta Capital Long Short LLC", f13_filings=10)}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1 and _gate_rules(p, 0) == ["R3_crd_name_prefix"]
        # ...but a structural extra token ("GP") is not a strategy label
        recs = _filer(72, "Trian Fund Management LP", crd="702") + \
            _filer(73, "Trian Fund Management GP LLC", crd="702")
        prof = {"72": _prof("Trian Fund Management LP", f13_filings=10),
                "73": _prof("Trian Fund Management GP LLC", f13_filings=10)}
        assert len(_plan(recs, prof)["components"]) == 2
        # R4: the registered adviser's old CIK went dormant, a new one took over
        recs = _filer(74, "Zega Financial LLC", crd="703") + _filer(75, "ZEGA Investments LLC", crd="703")
        prof = {"74": _prof("Zega Financial LLC", first="2018-01-01", last="2025-01-10"),
                "75": _prof("ZEGA Investments LLC", first="2025-03-01", last="2026-09-10")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1 and _gate_rules(p, 0) == ["R4_crd_succession"]
        # distinct advisers filing under one cover-page CRD
        recs = _filer(76, "Peak Capital Management LLC", crd="704") + \
            _filer(79, "Shepherd Kaplan LLC", crd="704") + \
            [_rec("adv:704", crd="704", legal_name="PEAK CAPITAL MANAGEMENT, LLC")]
        prof = {"76": _prof("Peak Capital Management LLC", f13_filings=5),
                "79": _prof("Shepherd Kaplan LLC", f13_filings=5)}
        p = _plan(recs, prof)
        assert not _together(p, "edgar:0000000076", "edgar:0000000079")
        assert _together(p, "edgar:0000000076", "adv:704")       # ADV attaches by name
        assert ("crd", "704") in p["components"][_comp_of(p, "edgar:0000000076")]["keys"]
        assert ("crd", "704") not in p["components"][_comp_of(p, "edgar:0000000079")]["keys"]

    def test_bridge_edge_gated(self):
        """T9b: a cover_page bridge edge between two unrelated filers joins nothing"""
        recs = _filer(80, "Waterfront Capital Partners LLC") + \
            _filer(81, "Sunesis Capital LLC", crd="801")
        prof = {"80": _prof("Waterfront Capital Partners LLC", f13_filings=3),
                "81": _prof("Sunesis Capital LLC", f13_filings=3)}
        bridge = [("80", "801", "cover_page_crd", "coverpage")]
        p = _plan(recs, prof, bridge)
        assert not _together(p, "edgar:0000000080", "edgar:0000000081")
        # and a corroborated one still joins, with the crosswalk tier
        recs = _filer(82, "Shelton Capital Management") + \
            _filer(83, "SHELTON CAPITAL MANAGEMENT /ADV", crd="802")
        prof = {"82": _prof("Shelton Capital Management"), "83": _prof("SHELTON CAPITAL MANAGEMENT /ADV")}
        p = _plan(recs, prof, [("82", "802", "cover_page_crd", "coverpage")])
        assert len(p["components"]) == 1 and p["components"][0]["tier"] == "crosswalk"


# ---------------------------------------------------------------------------
# ambiguous: split + flagged, never merged
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAmbiguous:
    def _flags(self, p):
        return sorted({a["flag"] for c in p["components"]
                       for a in (c.get("gate") or {}).get("ambiguous", [])})

    def test_ambiguous_flagged_not_merged(self):
        """T10"""
        # A3: equal base name, conflicting legal forms, a short overlap
        recs = _filer(90, "Kagins Digital LLC", ein=E1) + _filer(91, "Kagin's Digital, Inc.", ein=E1)
        prof = {"90": _prof("Kagins Digital LLC", first="2024-01-01", last="2026-09-01"),
                "91": _prof("Kagin's Digital, Inc.", first="2026-06-01", last="2026-09-15")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2 and self._flags(p) == ["A3_form_conflict_short_overlap"]
        assert p["metrics"]["gate"]["ambiguous_by_flag"] == {"A3_form_conflict_short_overlap": 1}
        # A1: EIN only, sequential, unrelated names, not both Form-D-only
        recs = _filer(92, "RP Management, LLC", ein=E2) + _filer(93, "Royalty Pharma Manager, LLC", ein=E2)
        prof = {"92": _prof("RP Management, LLC", first="2019-01-01", last="2025-08-29"),
                "93": _prof("Royalty Pharma Manager, LLC", first="2025-09-02", last="2026-09-01")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2 and self._flags(p) == ["A1_ein_sequential_unnamed"]
        # A4: EIN and CRD both shared, names share no token
        recs = [_rec("edgar:0000000094", cik="94", ein=E1, crd="904", legal_name="Ceredex Value Advisors LLC"),
                _rec("formd:0000000094", cik="94", legal_name="Ceredex Value Advisors LLC"),
                _rec("edgar:0000000095", cik="95", ein=E1, crd="904", legal_name="Silvant Capital Management LLC"),
                _rec("formd:0000000095", cik="95", legal_name="Silvant Capital Management LLC")]
        prof = {"94": _prof("Ceredex Value Advisors LLC"), "95": _prof("Silvant Capital Management LLC")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2 and self._flags(p) == ["A4_ein_and_crd_unrelated_names"]
        # A5: former names overlap while both file, neither an operating issuer
        recs = _filer(96, "Mesirow Financial Investment Management Inc", ein=E2) + \
            _filer(97, "Mesirow Financial Investment Management Inc - Fixed Income", ein=E2)
        prof = {"96": _prof("Mesirow Financial Investment Management Inc", f13_filings=30),
                "97": _prof("Mesirow Financial Investment Management Inc - Fixed Income", f13_filings=30,
                            former_names=["Mesirow Financial Investment Management Inc"])}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2 and self._flags(p) == ["A5_former_name_concurrent_nonissuer"]


# ---------------------------------------------------------------------------
# attaching records that carry no CIK
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAttach:
    def test_attach_no_cik_records(self):
        """T11"""
        # only class: a Form 5500 sponsor and its single EDGAR filer still join (SPEC_147)
        recs = _filer(100, "Bolt Labs Inc", ein=E1, twin=False) + \
            [_rec(f"dol5500:{E1}", ein=E1, legal_name="BOLT LABS INC")]
        p = _plan(recs, {"100": _prof("Bolt Labs Inc")})
        assert _together(p, "edgar:0000000100", f"dol5500:{E1}")
        # single operating class: the record's name matches nobody
        recs = (_filer(101, "Acme Industrial Corp", ein=E2)
                + _filer(102, "Acme Fund I LP", ein=E2)
                + [_rec(f"dol5500:{E2}", ein=E2, legal_name="ACME RETIREMENT TRUSTEES")])
        prof = {"101": _prof("Acme Industrial Corp", sic="3500"), "102": _prof("Acme Fund I LP")}
        p = _plan(recs, prof)
        assert _together(p, "edgar:0000000101", f"dol5500:{E2}")
        how = [a["how"] for c in p["components"] for a in (c.get("gate") or {}).get("attached", [])]
        assert how == ["single_operating_class"]
        # own entity: two operating classes, the ADV + IAPD records name neither
        recs = (_filer(103, "Nalanda India Equity Fund Ltd", crd="1103")
                + _filer(104, "Nalanda India Fund Ltd", crd="1103")
                + [_rec("adv:1103", crd="1103", legal_name="NALANDA CAPITAL PTE LTD"),
                   _rec("iapd:1103", crd="1103", legal_name="NALANDA CAPITAL PTE LTD")])
        prof = {"103": _prof("Nalanda India Equity Fund Ltd", f13_filings=8),
                "104": _prof("Nalanda India Fund Ltd", f13_filings=8)}
        p = _plan(recs, prof)
        own = _comp_of(p, "adv:1103")
        assert own is not None and sorted(p["components"][own]["members"]) == ["adv:1103", "iapd:1103"]
        assert ("crd", "1103") in p["components"][own]["keys"]   # the anchor owns the CRD


# ---------------------------------------------------------------------------
# contested keys
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestContested:
    def test_contested_key_owner(self):
        """T12: sibling vehicles -> nobody owns the EIN; keys never on two components;
        the canonical column is NULL with a note"""
        from app.entities.resolve_core import canonical_row

        recs = _filer(110, "Nukudo Series A LLC", ein=E1) + _filer(112, "Nukudo Series B LLC", ein=E1)
        prof = {"110": _prof("Nukudo Series A LLC"), "112": _prof("Nukudo Series B LLC")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert all(("ein", E1) not in c["keys"] and ("ein", E1) in c["withheld"] for c in p["components"])
        assert p["metrics"]["gate"]["contested_keys"] == {"ein": 1}
        assert p["metrics"]["gate"]["contested_no_owner"] == {"ein": 1}
        by_key = {r["record_key"]: r for r in recs}
        c = p["components"][0]
        cols, conflicts = canonical_row(c["members"], by_key, c["withheld"])
        assert cols["ein"] is None
        assert conflicts["contested"] == [{"type": "ein", "value": E1}]
        # keys are unique across components on every corpus in this file
        seen = {}
        for i, comp in enumerate(p["components"]):
            for k in comp["keys"]:
                assert seen.setdefault(k, i) == i


# ---------------------------------------------------------------------------
# main piece keeps the id
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMainPiece:
    def test_main_piece_keeps_id(self):
        """T13: the CRD-owning adviser keeps the old id over a larger vehicle piece"""
        from app.entities.resolve_core import assign_ids

        recs = ([_rec("edgar:0000000120", cik="120", ein=E1, legal_name="Windward Management LP"),
                 _rec("f13:0000000120", cik="120", crd="1200", legal_name="Windward Management LP"),
                 _rec("adv:1200", crd="1200", legal_name="WINDWARD MANAGEMENT LP")]
                + [_rec(f"{s}:0000000121", cik="121", ein=E1 if s == "edgar" else None,
                        legal_name="Windward Partners Fund LP") for s in ("edgar", "formd", "x1", "x2")])
        prof = {"120": _prof("Windward Management LP", f13_filings=4),
                "121": _prof("Windward Partners Fund LP", formd_pooled=True, formd_filings=6)}
        p = _plan(recs, prof)
        comps = p["components"]
        assert len(comps) == 2
        prior = {r["record_key"]: 5 for r in recs}
        ids = assign_ids(comps, prior, {})
        adviser = _comp_of(p, "edgar:0000000120")
        assert ids["assigned"] == {adviser: 5}
        assert ids["new_entities"] == [1 - adviser]
        assert ids["splits"] == [{"component_first_member": comps[1 - adviser]["members"][0],
                                  "lost_entity_ids": [5]}]
        # rank: CRD owner first; operating before vehicle; active before dormant; oldest first
        assert comps[adviser]["main_rank"] < comps[1 - adviser]["main_rank"]


# ---------------------------------------------------------------------------
# never a new merge; unchanged where nothing is gated
# ---------------------------------------------------------------------------


def _reference_components(records, bridge):
    """The pre-SPEC_150 rule: every strong key and every live bridge edge unions."""
    from app.entities.resolve_core import _extract_keys

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    live = set()
    for r in records:
        for k in _extract_keys(r):
            live.add(k)
            union("r:" + r["record_key"], f"k:{k[0]}:{k[1]}")
    for cik, crd, _t, _m in bridge:
        if ("cik", cik) in live and ("crd", crd) in live:
            union(f"k:cik:{cik}", f"k:crd:{crd}")
    comps = {}
    for r in records:
        if _extract_keys(r):
            comps.setdefault(find("r:" + r["record_key"]), []).append(r["record_key"])
    return sorted(sorted(m) for m in comps.values() if len(m) >= 2)


NAMES = ["Acme Capital LLC", "ACME CAPITAL LLC /ADV", "Acme Capital Fund I LP", "Bolt Labs Inc",
         "Bolt Labs Holdings Inc", "Cobalt Partners LP", "Cobalt Partners Ltd", "SMITH JOHN",
         "Delta Separate Account A", "Echo Advisors LLC", "Echo Advisors Long Short LLC"]


@pytest.mark.unit
class TestNoNewMerge:
    def test_never_a_new_merge(self):
        """T14: on random corpora every new component sits inside one old component"""
        rng = random.Random(150)
        for _trial in range(60):
            recs, prof = [], {}
            for i in range(rng.randint(5, 30)):
                cik = str(rng.randint(7001, 7012)) if rng.random() < 0.7 else None
                ein = rng.choice([E1, E2, "910470860", None, None])
                crd = rng.choice(["1101", "1201", None, None, None])
                name = rng.choice(NAMES)
                recs.append(_rec(f"s{i % 4}:{i}", cik=cik, ein=ein, crd=crd, legal_name=name))
                if cik:
                    first = date(2015 + rng.randint(0, 10), rng.randint(1, 12), 1)
                    last = date(min(2026, first.year + rng.randint(0, 6)), rng.randint(1, 9), 1)
                    prof.setdefault(cik, _prof(name, first=first.isoformat(), last=max(first, last).isoformat(),
                                               insider_owner=name == "SMITH JOHN",
                                               former_names=[rng.choice(NAMES)] if rng.random() < 0.3 else []))
            bridge = [(str(rng.randint(7001, 7012)), rng.choice(["1101", "1201"]), "cover_page_crd", "x")
                      for _ in range(rng.randint(0, 3))]
            old = _reference_components(recs, bridge)
            new = _plan(recs, prof, bridge)["components"]
            where = {rk: i for i, m in enumerate(old) for rk in m}
            for c in new:
                assert len({where.get(rk) for rk in c["members"]}) == 1, (c["members"], old)
                assert None not in {where.get(rk) for rk in c["members"]}

    def test_ungated_unchanged(self):
        """T15: no gated key spans two CIKs -> exactly the old components and tiers"""
        recs = [_rec("dol5500:1", ein=E1, legal_name="Acme Dental LLC"),
                _rec("dol5500:2", ein=E1, legal_name="ACME DENTAL"),
                _rec("edgar:0000000042", cik="0000000042", ein=E2, legal_name="Unrelated Co"),
                _rec("formd:42", cik="42", legal_name="Unrelated Co"),
                _rec("adv:104720", crd="104720", legal_name="SHELTON CAPITAL MANAGEMENT"),
                _rec("iapd:104720", crd="104720", legal_name="SHELTON CAPITAL MANAGEMENT"),
                _rec("edgar:0001002784", cik="0001002784", legal_name="SHELTON CAPITAL MANAGEMENT")]
        bridge = [("1002784", "104720", "cover_page_crd", "coverpage")]
        p = _plan(recs, {}, bridge)
        assert [c["members"] for c in p["components"]] == _reference_components(recs, bridge)
        tiers = {c["members"][0]: c["tier"] for c in p["components"]}
        assert tiers == {"adv:104720": "crosswalk", "dol5500:1": "exact_key",
                         "edgar:0000000042": "exact_key"}
        assert all(not c.get("gate") for c in p["components"] if c["tier"] == "exact_key")

    def test_profiles_only_for_shared_keys(self):
        """T16"""
        from app.entities.resolve_core import gated_ciks

        recs = (_filer(211, "A Inc", ein=E1) + _filer(212, "B Inc", ein=E1) + _filer(213, "C Inc", ein=E2)
                + _filer(214, "D LLC", crd="4401") + _filer(215, "E LLC") + [_rec("adv:4401", crd="4401")])
        bridge = [("215", "4401", "cover_page_crd", "x"), ("213", "9901", "cover_page_crd", "x")]
        assert gated_ciks(recs, bridge, {}) == {"211", "212", "214", "215"}


# ---------------------------------------------------------------------------
# Postgres: the write path
# ---------------------------------------------------------------------------


def _apply_migration(conn, name: str, attr: str):
    from sqlalchemy import text

    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for stmt in getattr(mod, attr):
        conn.execute(text(stmt))


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        for t in ("sec_filers", "sec_filer_former_names", "sec_13f_filings"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        _apply_migration(conn, "0008_entity_master", "CORE_DDL")
        _apply_migration(conn, "0016_entity_weak_match", "UPGRADE_SQL")
        # a deliberately thin sec_filers: missing profile columns must not break the load
        conn.execute(text("""CREATE TABLE sec_filers (cik TEXT, name TEXT, ein TEXT, sic TEXT,
            latest_filing_date DATE, earliest_recent_filing_date DATE)"""))
        conn.execute(text("CREATE TABLE sec_filer_former_names (cik TEXT, name TEXT, from_date DATE, to_date DATE)"))
        conn.execute(text("CREATE TABLE sec_13f_filings (cik TEXT, crd_number TEXT)"))
    yield engine
    engine.dispose()


@pg
def test_gated_split_writes_provenance_pg(pg_engine):
    """T17"""
    import json

    from sqlalchemy import text

    from app.entities import resolve

    rows = [  # record_key, source, cik, ein, crd, name
        ("edgar:0000000201", "edgar", "0000000201", E1, None, "Zurich American Life Insurance Co"),
        ("formd:0000000201", "formd", "0000000201", None, None, "Zurich American Life Insurance Co"),
        ("edgar:0000000202", "edgar", "0000000202", E1, None, "ZALICO Variable Annuity Separate Account"),
        ("formd:0000000202", "formd", "0000000202", None, None, "ZALICO Variable Annuity Separate Account"),
        ("x:0000000202", "x", "0000000202", None, None, "ZALICO Variable Annuity Separate Account"),
        ("edgar:0000000203", "edgar", "0000000203", E2, None, "Sunshine Biopharma Inc"),
        ("edgar:0000000204", "edgar", "0000000204", E2, None, "Grom Social Enterprises Inc"),
    ]
    filers = [("0000000201", "Zurich American Life Insurance Co", E1, "6311"),
              ("0000000202", "ZALICO Variable Annuity Separate Account", E1, None),
              ("0000000203", "Sunshine Biopharma Inc", E2, "2834"),
              ("0000000204", "Grom Social Enterprises Inc", E2, "7370")]
    with pg_engine.begin() as conn:
        for rk, src, cik, ein, crd, name in rows:
            conn.execute(text("""INSERT INTO core.source_record (record_key, source, native_id,
                legal_name, name_norm, name_norm_version, ein, cik, crd) VALUES
                (:rk, :src, :nid, :n, lower(:n), 'v1', :ein, :cik, :crd)"""),
                {"rk": rk, "src": src, "nid": rk.split(":")[1], "n": name, "ein": ein, "cik": cik, "crd": crd})
        for cik, name, ein, sic in filers:
            conn.execute(text("""INSERT INTO sec_filers VALUES (:c, :n, :e, :s, '2026-09-20', '2024-01-02')"""),
                         {"c": cik, "n": name, "e": ein, "s": sic})
        # prior state: the old rules had fused them -- 77 (insurer + account), 78 (two issuers)
        conn.execute(text("SELECT setval('core.entity_entity_id_seq', 500)"))
        for eid, members in ((77, [r[0] for r in rows[:5]]), (78, [r[0] for r in rows[5:]])):
            conn.execute(text("""INSERT INTO core.entity (entity_id, canonical_name, member_count,
                strong_key_count, match_tier, field_conflicts, last_members, resolver_version, updated_at)
                VALUES (:e, 'fused', :n, 1, 'exact_key', '{}', CAST(:m AS JSONB), 'er_v1', NOW())"""),
                {"e": eid, "n": len(members), "m": json.dumps(members)})
            for rk in members:
                conn.execute(text("""INSERT INTO core.membership (record_key, entity_id, match_tier,
                    match_method, features, updated_at) VALUES (:rk, :e, 'exact_key', 'strong_key', '{}', NOW())"""),
                    {"rk": rk, "e": eid})

    with pg_engine.begin() as conn:
        dry = resolve.resolve(conn, dry_run=True)
    assert dry["gate"]["refused_by_reason.V3_vehicle"] == 1
    assert dry["gate"]["refused_by_reason.no_corroboration"] == 1
    with pg_engine.begin() as conn:
        m = resolve.resolve(conn)
    assert m["entities_split"] == 1
    with pg_engine.connect() as conn:
        ent = {r["entity_id"]: r for r in conn.execute(text(
            "SELECT entity_id, cik, ein, dissolved_at, field_conflicts FROM core.entity")).mappings()}
        mem = dict(conn.execute(text("SELECT record_key, entity_id FROM core.membership")).fetchall())
        ids = conn.execute(text("SELECT id_type, id_value, entity_id FROM core.identifier")).fetchall()
    # the insurer (the operating piece) keeps 77 and owns the EIN
    assert mem["edgar:0000000201"] == 77 and ent[77]["cik"] == "201" and ent[77]["ein"] == E1
    acct = mem["edgar:0000000202"]
    assert acct != 77 and acct > 500
    assert ent[acct]["ein"] is None and ent[acct]["cik"] == "202"
    fc = ent[acct]["field_conflicts"]
    assert fc["split_from"] == [77]
    assert fc["contested"] == [{"type": "ein", "value": E1}]
    assert fc["gate"]["refused"][0]["reason"] == "V3_vehicle"
    assert ("ein", E1, 77) in ids and not any(t == "ein" and e == acct for t, _v, e in ids)
    # 78: two unrelated issuers on one EIN -> both single records -> 78 dissolved, with the reason
    assert "edgar:0000000203" not in mem and ent[78]["dissolved_at"] is not None
    assert ent[78]["field_conflicts"]["dissolved_by_gate"]["refused"][0]["reason"] == "no_corroboration"
    # idempotent
    with pg_engine.begin() as conn:
        again = resolve.resolve(conn)
    assert again["entities_written"] == 0 and again["entities_updated"] == 0
    assert again["memberships_written"] == 0 and again["memberships_updated"] == 0


# ---------------------------------------------------------------------------
# review fixes (2026-10-01 review of the dry run)
# ---------------------------------------------------------------------------


def _flags_all(p):
    return sorted({a["flag"] for c in p["components"]
                   for a in (c.get("gate") or {}).get("ambiguous", [])})


def _fn(name, frm, to):
    return {"name": name, "from_date": frm, "to_date": to}


@pytest.mark.unit
class TestReviewFixes:
    def test_despac_namesake_split(self):
        """T18: a SPAC renamed INTO a private company's name while that company was
        filing is two legal persons (VEEA / Plum, Hadron / GigCapital7): split, flagged"""
        recs = _filer(130, "VEEA INC.", ein=E1) + _filer(131, "Veea Inc.", ein=E1)
        prof = {"130": _prof("VEEA INC.", first="2021-02-01", last="2026-09-16", sic="7373",
                             tickers=["VEEA", "VEEAW"], latest_form="8-K",
                             former_names=[_fn("Plum Acquisition Corp. I", "2021-02-01", "2024-09-18")]),
                "131": _prof("Veea Inc.", first="2023-09-13", last="2023-10-12", latest_form="D",
                             formd_filings=2)}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert "V4_name_adopted_concurrent" in _gate_refusals(p, 0)
        assert _flags_all(p) == ["A6_name_adopted_concurrent"]
        assert p["metrics"]["gate"]["refused_by_reason"] == {"V4_name_adopted_concurrent": 1}

    def test_name_adopted_controls_still_kept(self):
        """T18b: renamed together (both carry the old name), renamed long before the other
        CIK appeared, a plain /ADV duplicate: still R1"""
        # both CIKs carry the same former name: one firm renamed both accounts
        recs = _filer(132, "OSAIC WEALTH, INC.", ein=E1) + _filer(133, "OSAIC WEALTH, INC.", ein=E1)
        prof = {"132": _prof("OSAIC WEALTH, INC.", first="2011-02-23", last="2024-08-26",
                             former_names=[_fn("ROYAL ALLIANCE ASSOCIATES INC", "2011-02-23", "2023-08-01")]),
                "133": _prof("OSAIC WEALTH, INC.", first="2002-03-01", last="2026-08-24",
                             former_names=[_fn("ROYAL ALLIANCE ASSOCIATES, INC.", "2002-03-01", "2023-08-01")])}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1 and _gate_rules(p, 0) == ["R1_name_equal"]
        # renamed in 1998; the second CIK first filed in 2025
        recs = _filer(134, "UNIVERSITY BANCORP INC /DE/", ein=E2) + _filer(135, "University Bancorp, Inc.", ein=E2)
        prof = {"134": _prof("UNIVERSITY BANCORP INC /DE/", first="1995-05-15", last="2024-10-28", sic="6022",
                             tickers=["UNIB"], former_names=[_fn("NEWBERRY BANCORP INC", "1995-05-15", "1998-01-01")]),
                "135": _prof("University Bancorp, Inc.", first="2025-12-01", last="2026-07-15", formd_filings=1,
                             latest_form="D")}
        p = _plan(recs, prof)
        assert len(p["components"]) == 1 and _gate_rules(p, 0) == ["R1_name_equal"]

    def test_sequential_is_data_relative(self):
        """T19: the plan reads no run date, so the same data plans the same on any day. Two
        one-filing 13F CIKs three days apart on one CRD (N10 WEALTH / N10 ASSETS) are never a
        succession while the data stays the same -- not even when other filers' newer filings
        move the data's own date on; a real succession (Zega) is one throughout."""
        import inspect

        from app.entities.resolve_core import plan

        assert "asof" not in inspect.signature(plan).parameters
        recs = (_filer(140, "N10 WEALTH, LLC", ein="422835283", crd="342185")
                + _filer(141, "N10 ASSETS, LLC", ein="422780528", crd="342185")
                + _filer(142, "Zega Financial LLC", crd="7400") + _filer(143, "ZEGA Investments LLC", crd="7400"))
        prof = {"140": _prof("N10 WEALTH, LLC", first="2026-08-14", last="2026-08-14", f13_filings=1),
                "141": _prof("N10 ASSETS, LLC", first="2026-08-17", last="2026-08-17", f13_filings=1),
                "142": _prof("Zega Financial LLC", first="2018-01-01", last="2025-01-10", f13_filings=9),
                "143": _prof("ZEGA Investments LLC", first="2025-03-01", last="2026-09-10", f13_filings=6)}
        later = dict(prof, **{"999": _prof("Unrelated Filer LLC", first="2020-01-01", last="2027-06-01")})
        outs = [plan(list(recs), [], {}, profiles=prof), plan(list(recs), [], {}, profiles=prof),
                plan(list(recs), [], {}, profiles=later)]
        for o in outs:
            assert not _together(o, "edgar:0000000140", "edgar:0000000141")
            assert _together(o, "edgar:0000000142", "edgar:0000000143")
        assert outs[0]["components"] == outs[1]["components"] and outs[0]["metrics"] == outs[1]["metrics"]
        assert [c.get("gate") for c in outs[2]["components"]] == [c.get("gate") for c in outs[0]["components"]]
        assert outs[2]["metrics"]["gate"]["refused_by_reason"] == outs[0]["metrics"]["gate"]["refused_by_reason"]

    def test_heavy_filer_first_date(self):
        """T20: EDGAR's recent window holds 1,000 filings, so its earliest date is not the
        first filing of a heavy filer. The earliest former-name date is used when there is
        one; without it the first filing is unknown, never 'recent'."""
        from app.entities import gate

        f = gate.features("1094831", _prof(
            "BGC Group, Inc.", first="2011-06-29", last="2026-09-28", recent_filing_count=1000,
            former_names=[_fn("BGC Partners, Inc.", "2008-04-04", "2023-07-03"),
                          _fn("ESPEED INC", "1999-11-16", "2008-04-04")]))
        assert f["first"] == date(1999, 11, 16)
        h = gate.features("777", _prof("Heavy Filer Inc", first="2024-06-01", last="2026-09-28",
                                       recent_filing_count=1000))
        assert h["first"] is None and h["first_floor"] == date(2024, 6, 1)
        # a heavy filer can never be the NEW side of a succession
        old = gate.features("778", _prof("Old Filer Inc", first="2010-01-01", last="2024-03-01"))
        assert gate.sequential(old, h) is None
        # an unknown (truncated) first filing ranks as the oldest for the main piece
        recs = _filer(150, "Heavy Filer Inc", ein=E1) + _filer(151, "Heavy Filer Leasing Inc", ein=E1)
        p = _plan(recs, {"150": _prof("Heavy Filer Inc", first="2024-06-01", recent_filing_count=1000, sic="6200"),
                         "151": _prof("Heavy Filer Leasing Inc", first="2015-01-01", sic="6200")})
        assert len(p["components"]) == 2
        heavy, other = _comp_of(p, "edgar:0000000150"), _comp_of(p, "edgar:0000000151")
        assert p["components"][heavy]["main_rank"] < p["components"][other]["main_rank"]

    def test_ein_and_crd_shared_flagged(self):
        """T21: an EIN AND a CRD shared by two advisers with related names is the strongest
        same-firm hint the rules split on: flagged A2 (Orion USA LP / Orion LP)"""
        recs = [_rec("edgar:0000000160", cik="160", ein=E1, crd="161327",
                     legal_name="ORION RESOURCE PARTNERS (USA) LP"),
                _rec("formd:0000000160", cik="160", legal_name="ORION RESOURCE PARTNERS (USA) LP"),
                _rec("edgar:0000000161", cik="161", ein=E1, crd="161327", legal_name="Orion Resource Partners LP"),
                _rec("formd:0000000161", cik="161", legal_name="Orion Resource Partners LP")]
        prof = {"160": _prof("ORION RESOURCE PARTNERS (USA) LP", first="2017-08-10", f13_filings=13),
                "161": _prof("Orion Resource Partners LP", f13_filings=14)}
        p = _plan(recs, prof)
        assert len(p["components"]) == 2
        assert _flags_all(p) == ["A2_ein_and_crd_related_names"]

    def test_crd_hint_flags_never_joins(self):
        """T21b: the real Orion pair -- the second CIK names the CRD only through a 13F
        bridge edge the resolver does not union on (the CRD maps to two CIKs). That edge is
        a review HINT: it flags the EIN-sharing pair A2 and never joins anything."""
        from app.entities.resolve_core import plan

        recs = [_rec("edgar:0000000170", cik="170", ein=E1, legal_name="ORION RESOURCE PARTNERS (USA) LP"),
                _rec("f13:0000000170", cik="170", crd="161327", legal_name="ORION RESOURCE PARTNERS (USA) LP"),
                _rec("edgar:0000000171", cik="171", ein=E1, legal_name="Orion Resource Partners LP"),
                _rec("f13:0000000171", cik="171", legal_name="Orion Resource Partners LP")]
        prof = {"170": _prof("ORION RESOURCE PARTNERS (USA) LP", first="2017-08-10", f13_filings=13),
                "171": _prof("Orion Resource Partners LP", f13_filings=14)}
        bare = plan(list(recs), [], {}, profiles=prof)
        assert len(bare["components"]) == 2 and _flags_all(bare) == []
        hinted = plan(list(recs), [], {}, profiles=prof, crd_hints=[("0000000171", "161327")])
        assert [c["members"] for c in hinted["components"]] == [c["members"] for c in bare["components"]]
        assert _flags_all(hinted) == ["A2_ein_and_crd_related_names"]

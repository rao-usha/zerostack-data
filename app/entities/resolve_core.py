"""
Identifier-only entity resolution — the pure core (PLAN_083 / SPEC_116).

Ported verbatim from wildcard-workbench `ingest/resolve.py` (the I/O layer is
rewritten for SQLAlchemy in `app/entities/resolve.py`). Everything here is a
pure function: no DB, no clock, no randomness, so `_selftest()` proves the
rules offline and the port is checkable against the original.

WHAT IT DOES: two records are the same entity only when they share a STRONG
identifier (EIN, CIK, CRD, LEI, UEI, or a state-scoped registry id), or when a
curated CIK<->CRD crosswalk edge joins them. Names, addresses, phones and
domains never merge anything — that is what produced "Investor Relations" as a
person linked to four firms in the old nexdata resolver.

Values are canonicalized before comparison, so EDGAR's zero-padded CIK
("0001002784") and the bare integer (1002784) are ONE key, and placeholder
values made of one repeated digit are refused outright.
"""

import re

from app.entities import norm


RESOLVER_VERSION = "er_v1"

# One identifier value asserted by more than this many records is a placeholder
# or a parse artifact. MEASURED on the 2026-09-05 corpus: the busiest real EIN
# is asserted by 25 records (one Form 5500 sponsor filing 25 plans), so this cap
# sits ~20x above anything legitimate seen so far. If it ever fires it is
# reported with the offending values rather than quietly widened.
KEY_FANOUT_CAP = 500

# A resolved component this large is a runaway merge, not a company. It is not
# materialized; it is reported. Same reasoning, one level up.
COMPONENT_RECORD_CAP = 1000

# Refusal COUNTS are always exact; the accompanying example lists are capped so
# one pathological key cannot write a megabyte of jsonb into the run ledger.
# The count always rides alongside, so the cap can never be misread as the real
# number (ingest/cik_crd_bridge.py:EVIDENCE_LIST_CAP, same rule).
DETAIL_CAP = 25

# Identifier canonicalizers. Each returns the canonical string or None; None
# means "this record does not usefully assert this identifier" and NOTHING is
# invented in its place. state_entity_id is handled separately because it is
# only unique within its issuing state.
_DIGITS = re.compile(r"\D")
_ALNUM = re.compile(r"[^A-Z0-9]")


def _degenerate(value):
    """True for a value made of one repeated character ('000000000',
    '999999999', 'XXXXXXXX'). These are placeholders every registry collects
    and they would fuse everything that carries them."""
    return len(set(value)) <= 1


def _ein(raw):
    v = norm.clean_ein(raw)          # the harness's own cleaner, incl. the
    return v if v and not _degenerate(v) else None   # '000000000' sentinel


def _numeric_id(raw, max_len):
    """CIK and CRD: digits, leading zeros stripped, so the zero-padded EDGAR
    form ('0001002784') and the bare integer form (1002784) are ONE key."""
    digits = _DIGITS.sub("", str(raw or ""))
    if not digits or len(digits) > max_len:
        return None
    canon = digits.lstrip("0")
    return canon if canon and not _degenerate(canon) else None


def _cik(raw):
    return _numeric_id(raw, 10)


def _crd(raw):
    return _numeric_id(raw, 10)


def _lei(raw):
    v = _ALNUM.sub("", str(raw or "").upper())
    return v if len(v) == 20 and not _degenerate(v) else None


def _uei(raw):
    v = _ALNUM.sub("", str(raw or "").upper())
    return v if len(v) == 12 and not _degenerate(v) else None


# (key_type, column, canonicalizer). Order is fixed so evidence lists are
# deterministic across runs.
STRONG_KEYS = (
    ("ein", "ein", _ein),
    ("cik", "cik", _cik),
    ("crd", "crd", _crd),
    ("lei", "lei", _lei),
    ("uei", "uei", _uei),
)

# The crosswalk. Only the identifier-to-identifier tiers; see the refusal
# section of the module docstring for why 'name_state' is not here.
BRIDGE_TIERS_ACCEPTED = ("cover_page_crd", "other_manager_crd")

XWALK_KEY_TYPE = "xwalk:cik_crd"


# ---------------------------------------------------------------------------
# union-find
# ---------------------------------------------------------------------------

class _UF:
    """Union-find with path compression. Node labels are strings."""

    def __init__(self):
        self.parent = {}

    def add(self, x):
        self.parent.setdefault(x, x)

    def find(self, x):
        p = self.parent
        root = x
        while p[root] != root:
            root = p[root]
        while p[x] != root:
            p[x], x = root, p[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Smaller label wins so component roots are deterministic across
            # runs regardless of insertion order.
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra
        return ra


def _rec_node(record_key):
    return "rec:" + record_key


def _key_node(key_type, key_value):
    return f"key:{key_type}:{key_value}"


# ---------------------------------------------------------------------------
# the resolve itself -- pure, given the loaded inputs
# ---------------------------------------------------------------------------

def _extract_keys(rec):
    """-> [(key_type, key_value)] for one record, in STRONG_KEYS order."""
    out = []
    for key_type, col, canon in STRONG_KEYS:
        val = canon(rec.get(col))
        if val:
            out.append((key_type, val))
    # state_entity_id is unique only within its issuing state, and
    # er_source_record does not carry the issuing state separately. The record's
    # own state is the only available scope, so a record with no state cannot
    # use this key at all -- refused, not guessed into a national namespace.
    sei = rec.get("state_entity_id")
    st = rec.get("state")
    if sei and st:
        val = _ALNUM.sub("", str(sei).upper())
        if val and len(val) >= 3 and not _degenerate(val):
            out.append(("sei", f"{st}:{val}"))
    return out


def plan(records, bridge_edges, vetoes):
    """Compute the resolution. PURE: no DB, no clock, no randomness.

    -> dict with 'components' (materializable, >=2 records), 'record_keys'
    (record_key -> [(type, value)]), 'refusals' and 'metrics'.
    """
    refusals = {"key_veto_record": 0, "key_veto_global": 0,
                "key_fanout_over_cap": 0, "xwalk_veto": 0,
                "component_over_cap": 0}
    refused_detail = {"fanout": [], "oversize_components": []}

    # 1. record -> keys, honouring vetoes
    rec_keys = {}
    key_to_recs = {}
    key_assertions_by_type = {}
    for rec in records:
        rk = rec["record_key"]
        kept = []
        for key_type, val in _extract_keys(rec):
            if (key_type, val, "*") in vetoes:
                refusals["key_veto_global"] += 1
                continue
            if (key_type, val, rk) in vetoes:
                refusals["key_veto_record"] += 1
                continue
            kept.append((key_type, val))
            key_to_recs.setdefault((key_type, val), []).append(rk)
            key_assertions_by_type[key_type] = (
                key_assertions_by_type.get(key_type, 0) + 1)
        rec_keys[rk] = kept

    # 2. fanout cap -- a key asserted by too many records is a placeholder
    over_cap = {k for k, v in key_to_recs.items() if len(v) > KEY_FANOUT_CAP}
    for k in sorted(over_cap):
        refusals["key_fanout_over_cap"] += 1
        if len(refused_detail["fanout"]) < DETAIL_CAP:
            refused_detail["fanout"].append(
                {"key_type": k[0], "key_value": k[1],
                 "records": len(key_to_recs[k])})
    if over_cap:
        for rk, kept in rec_keys.items():
            rec_keys[rk] = [k for k in kept if k not in over_cap]

    # 3. two graphs: A is exact_key edges only, B adds the crosswalk. The
    #    difference between a record's A-component and its B-component is
    #    exactly what makes its membership 'crosswalk' rather than 'exact_key'.
    uf_a, uf_b = _UF(), _UF()
    for rk, kept in rec_keys.items():
        node = _rec_node(rk)
        uf_a.add(node)
        uf_b.add(node)
        for key_type, val in kept:
            kn = _key_node(key_type, val)
            uf_a.add(kn)
            uf_b.add(kn)
            uf_a.union(node, kn)
            uf_b.union(node, kn)

    live_keys = {k for k, v in key_to_recs.items() if k not in over_cap}
    xwalk_used = []
    for cik, crd, tier, matched_on in bridge_edges:
        pair = f"{cik}:{crd}"
        if (XWALK_KEY_TYPE, pair, "*") in vetoes:
            refusals["xwalk_veto"] += 1
            continue
        # An edge between two identifiers nobody in the corpus asserts joins
        # nothing. Counted separately so "the bridge has 2,271 usable rows" is
        # never confused with "2,271 of them did anything here".
        if ("cik", cik) not in live_keys or ("crd", crd) not in live_keys:
            continue
        a, b = _key_node("cik", cik), _key_node("crd", crd)
        uf_b.add(a)
        uf_b.add(b)
        uf_b.union(a, b)
        xwalk_used.append({"cik": cik, "crd": crd, "bridge_tier": tier,
                           "matched_on": matched_on})

    # 4. components of B that contain at least two RECORDS
    comps = {}
    for rk in rec_keys:
        if not rec_keys[rk]:
            continue                     # no strong key: not in any component
        comps.setdefault(uf_b.find(_rec_node(rk)), []).append(rk)

    materializable, singleton_keyed = [], 0
    for root in sorted(comps):
        members = sorted(comps[root])
        if len(members) < 2:
            singleton_keyed += 1
            continue
        if len(members) > COMPONENT_RECORD_CAP:
            refusals["component_over_cap"] += 1
            if len(refused_detail["oversize_components"]) < DETAIL_CAP:
                refused_detail["oversize_components"].append(
                    {"root": root, "records": len(members),
                     "sample_members": members[:10]})
            continue
        # tier per record: exact_key when the crosswalk added nothing to this
        # record's cluster, crosswalk when its co-membership depends on a
        # bridge edge.
        a_roots = {uf_a.find(_rec_node(rk)) for rk in members}
        tier = "exact_key" if len(a_roots) == 1 else "crosswalk"
        keys_in_comp = sorted({kv for rk in members for kv in rec_keys[rk]})
        via = []
        if tier == "crosswalk":
            comp_ciks = {v for t, v in keys_in_comp if t == "cik"}
            comp_crds = {v for t, v in keys_in_comp if t == "crd"}
            via = [e for e in xwalk_used
                   if e["cik"] in comp_ciks and e["crd"] in comp_crds]
        materializable.append({
            "root": root, "members": members, "tier": tier,
            "keys": keys_in_comp, "via": via,
        })

    no_key = sum(1 for rk in rec_keys if not rec_keys[rk])
    metrics = {
        "corpus_records": len(records),
        "records_with_strong_key": len(records) - no_key,
        "records_no_strong_key": no_key,
        "key_assertions_by_type": key_assertions_by_type,
        "distinct_key_values": len(key_to_recs),
        "xwalk_edges_applied": len(xwalk_used),
        "components_materialized": len(materializable),
        "components_exact_key": sum(1 for c in materializable
                                    if c["tier"] == "exact_key"),
        "components_crosswalk": sum(1 for c in materializable
                                    if c["tier"] == "crosswalk"),
        "singleton_keyed": singleton_keyed,
        "members_total": sum(len(c["members"]) for c in materializable),
        # Computed HERE, not in the write path, so --dry-run reports the same
        # per-tier numbers a real run does. A dry run that quietly printed
        # zeros for the tier it is supposed to be measuring would be worse than
        # no dry run at all.
        "members_by_tier": {
            "exact_key": sum(len(c["members"]) for c in materializable
                             if c["tier"] == "exact_key"),
            "crosswalk": sum(len(c["members"]) for c in materializable
                             if c["tier"] == "crosswalk"),
        },
    }
    return {"components": materializable, "record_keys": rec_keys,
            "refusals": refusals, "refused_detail": refused_detail,
            "metrics": metrics, "xwalk_used": xwalk_used}


# ---------------------------------------------------------------------------
# canonical row derivation
# ---------------------------------------------------------------------------

def _passthrough(v):
    return str(v) if v not in (None, "") else None


# (er_entity column, er_source_record column, canonicalizer). The identifier
# columns are compared in their CANONICAL form, never raw: EDGAR writes CIK
# zero-padded to ten ('0001002784') and every other source writes it bare, so
# comparing raw strings would report a conflict where the two sources agree
# perfectly. The canonical (unpadded) form is also what destructive.py's purge
# passes when it deletes er_entity by cik / crd taken from a composite company
# id.
_AGREEMENT_COLS = (("ein", "ein", _ein), ("cik", "cik", _cik),
                   ("crd", "crd", _crd), ("lei", "lei", _lei),
                   ("uei", "uei", _uei),
                   ("state_entity_id", "state_entity_id", _passthrough),
                   ("canonical_state", "state", _passthrough),
                   ("canonical_zip5", "zip5", _passthrough),
                   ("canonical_domain", "domain", _passthrough))


def canonical_row(members, by_key):
    """-> (columns dict, field_conflicts dict) for one component.

    AGREEMENT ONLY on every column except canonical_name. See the module
    docstring: two members asserting different values leaves the column NULL
    and writes both values into field_conflicts.
    """
    cols, conflicts = {}, {}
    for out_col, src_col, canon in _AGREEMENT_COLS:
        vals = sorted({v for v in (canon(by_key[rk].get(src_col))
                                   for rk in members) if v})
        if len(vals) == 1:
            cols[out_col] = vals[0]
        else:
            cols[out_col] = None
            if len(vals) > 1:
                conflicts[out_col] = vals[:12]
                if len(vals) > 12:
                    conflicts[out_col + "_count"] = len(vals)

    # canonical_name: modal name_norm, ties on the smallest record_key.
    counts = {}
    for rk in members:
        nn = by_key[rk].get("name_norm")
        if nn:
            counts.setdefault(nn, []).append(rk)
    if counts:
        winner = min(counts, key=lambda nn: (-len(counts[nn]),
                                             min(counts[nn])))
        cols["canonical_name"] = by_key[min(counts[winner])]["legal_name"]
        conflicts["name_norm_distinct"] = len(counts)
    else:
        cols["canonical_name"] = None
        conflicts["name_norm_distinct"] = 0
    return cols, conflicts


# ---------------------------------------------------------------------------
# apply -- the only part that writes
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# entity id assignment -- PURE, so the four id rules are provable offline
# ---------------------------------------------------------------------------

def assign_ids(comps, prior, dissolved_claim=None):
    """Decide which entity_id each component gets -> the four rules, testable.

    Pure: no DB, no clock, no randomness. Inputs are the components from
    plan(), the LIVE membership map (record_key -> entity_id) and the
    dissolved-entity fallback (record_key -> entity_id of an entity that last
    held that record). Output:

        {"assigned":  {component index: existing entity_id}   (reuse/merge)
         "new_entities": [component index, ...]                (needs a new id)
         "merges": [(absorbed_id, survivor_id, cause_dict), ...]
         "splits": [{"component_first_member", "lost_entity_ids"}, ...]
         "reclaimed": int}

    This was factored out of resolve() because the real corpus cannot produce a
    merge or a split -- 0 entities carry more than one key TYPE across enough
    members, so no observation and no veto fractures a component into two
    surviving fragments. Measured: workbench.er_entity_merge holds 0 rows and
    all 13 er_resolve_run ledger rows report entities_merged = entities_split
    = 0. Rules that only a hypothetical corpus exercises are rules nobody has
    checked, and 'ent:<id>' flows into graph_edges, notes, labels and events,
    where a dangling reference is silent. _selftest drives this function
    directly instead of waiting for data that may never arrive.
    """
    dissolved_claim = dissolved_claim or {}
    claims, reclaimed = [], 0
    for comp in comps:
        ids = sorted({prior[rk] for rk in comp["members"] if rk in prior})
        if not ids:
            # No LIVE membership claims this component. Before issuing a new
            # id, check whether a dissolved entity last held these records --
            # the veto/unveto round trip lands here, and reviving the old id is
            # what makes the undo of the undo a real restore rather than a
            # lookalike with a different 'ent:' key. Live claims always win; the
            # fallback is only consulted when there are none.
            ids = sorted({dissolved_claim[rk] for rk in comp["members"]
                          if rk in dissolved_claim})
            if ids:
                reclaimed += 1
        claims.append(ids)

    # A single existing entity claimed by several components is a SPLIT: the
    # largest fragment keeps the id, ties break on the smallest member
    # record_key (comp["members"] is sorted, so members[0] IS that key), and
    # the component index is the last tiebreak so the result cannot depend on
    # dict or row order.
    keeper = {}
    for idx, ids in enumerate(claims):
        for eid in ids:
            cand = (-len(comps[idx]["members"]), comps[idx]["members"][0], idx)
            if keeper.get(eid) is None or cand < keeper[eid]:
                keeper[eid] = cand
    splits = []
    for idx, ids in enumerate(claims):
        claims[idx] = [eid for eid in ids if keeper[eid][2] == idx]
        if len(ids) != len(claims[idx]):
            splits.append({"component_first_member": comps[idx]["members"][0],
                           "lost_entity_ids": [e for e in ids
                                               if e not in claims[idx]]})

    new_entities = [i for i, ids in enumerate(claims) if not ids]
    merges = []          # (absorbed_id, survivor_id, cause)
    assigned = {}        # component index -> entity_id
    for idx, ids in enumerate(claims):
        if not ids:
            continue
        survivor = ids[0]        # LOWEST id survives: the oldest identity wins
        assigned[idx] = survivor
        for absorbed in ids[1:]:
            merges.append((absorbed, survivor, {
                "reason": "components joined by a later observation",
                "resolver_version": RESOLVER_VERSION,
                "tier": comps[idx]["tier"],
                "keys": [{"type": t, "value": v}
                         for t, v in comps[idx]["keys"]][:20],
                "members": comps[idx]["members"][:20],
                "member_count": len(comps[idx]["members"]),
            }))
    return {"assigned": assigned, "new_entities": new_entities,
            "merges": merges, "splits": splits, "reclaimed": reclaimed}



# ---------------------------------------------------------------------------
# vetoes -- the undo surface
# ---------------------------------------------------------------------------

VETO_KEY_TYPES = tuple(k for k, _c, _f in STRONG_KEYS) + ("sei", XWALK_KEY_TYPE)


def parse_veto_target(spec):
    """'ein=123456789@dol5500:99' -> ('ein', '123456789', 'dol5500:99').
    'ein=123456789' -> ('ein', '123456789', '*'). Raises ValueError, loudly."""
    key_type, sep, rest = spec.partition("=")
    if not sep or not rest:
        raise ValueError(f"veto target must look like TYPE=VALUE[@record_key], "
                         f"got {spec!r}")
    key_type = key_type.strip()
    if key_type not in VETO_KEY_TYPES:
        raise ValueError(f"unknown key type {key_type!r} -- known: "
                         f"{', '.join(VETO_KEY_TYPES)}")
    value, at, record_key = rest.partition("@")
    return key_type, value.strip(), (record_key.strip() if at else "*")



# ---------------------------------------------------------------------------
# self-test -- `python -m ingest.resolve` (NOT `python ingest/resolve.py`:
# this module imports its sibling ingest.norm, so it needs the package on the
# path). DB-free, house convention
# (ingest/norm.py). plan() is a pure function, so the tiers, the refusals and
# the veto undo are all provable offline with zero spend and zero writes.
# ---------------------------------------------------------------------------

def _rec(record_key, **kw):
    row = {"record_key": record_key, "source": record_key.split(":")[0],
           "native_id": record_key.split(":")[-1], "legal_name": None,
           "name_norm": None, "name_norm_version": norm.NAME_NORM_VERSION,
           "ein": None, "cik": None, "crd": None, "lei": None, "uei": None,
           "state_entity_id": None, "state": None, "zip5": None,
           "domain": None}
    row.update(kw)
    return row


def _selftest():
    fails = []

    def check(label, got, want):
        ok = got == want
        print(("  ok   " if ok else "  FAIL ") + f"{label}: {got!r}")
        if not ok:
            fails.append(f"{label}: got {got!r}, want {want!r}")

    print("exact_key tier")
    recs = [
        _rec("dol5500:1", ein="123456789", legal_name="Acme Dental LLC",
             name_norm="acme dental", state="CA"),
        _rec("dol5500:2", ein="12-3456789", legal_name="ACME DENTAL, L.L.C.",
             name_norm="acme dental", state="CA"),
        _rec("edgar:0000000042", cik="0000000042", legal_name="Unrelated Co",
             name_norm="unrelated"),
    ]
    r = plan(recs, [], {})
    check("one component from two records sharing an EIN",
          [c["members"] for c in r["components"]],
          [["dol5500:1", "dol5500:2"]])
    check("its tier is exact_key", r["components"][0]["tier"], "exact_key")
    check("the lone CIK record is a keyed singleton, not an entity",
          r["metrics"]["singleton_keyed"], 1)
    check("a hyphenated EIN and a bare one are ONE key",
          r["metrics"]["distinct_key_values"], 2)

    print("\nzero-padding is not a difference")
    r = plan([_rec("edgar:0001002784", cik="0001002784"),
              _rec("b2b:1002784", cik="1002784")], [], {})
    check("padded and bare CIK resolve together",
          [c["members"] for c in r["components"]],
          [["b2b:1002784", "edgar:0001002784"]])

    print("\ncrosswalk tier")
    recs = [_rec("edgar:0001002784", cik="0001002784",
                 legal_name="SHELTON CAPITAL MANAGEMENT",
                 name_norm="shelton capital management"),
            _rec("fin:104720", crd="104720",
                 legal_name="SHELTON CAPITAL MANAGEMENT",
                 name_norm="shelton capital management")]
    bridge = [("1002784", "104720", "cover_page_crd", "coverpage")]
    r = plan(recs, bridge, {})
    check("the bridge joins a CIK record to a CRD record",
          [c["members"] for c in r["components"]],
          [["edgar:0001002784", "fin:104720"]])
    check("and the tier says so", r["components"][0]["tier"], "crosswalk")
    check("with the bridge row carried as evidence",
          r["components"][0]["via"],
          [{"cik": "1002784", "crd": "104720",
            "bridge_tier": "cover_page_crd", "matched_on": "coverpage"}])
    check("no bridge, no match",
          len(plan(recs, [], {})["components"]), 0)

    print("\nan edge nobody asserts joins nothing")
    r = plan(recs, bridge + [("999999", "888888", "cover_page_crd", "x")], {})
    check("only the edge with both endpoints in the corpus is applied",
          r["metrics"]["xwalk_edges_applied"], 1)

    print("\nthe UNDO: a veto splits what it joined")
    recs = [_rec("dol5500:1", ein="123456789"),
            _rec("dol5500:2", ein="123456789"),
            _rec("edgar:9", ein="123456789")]
    check("all three resolve together with no veto",
          len(plan(recs, [], {})["components"][0]["members"]), 3)
    surgical = plan(recs, [], {("ein", "123456789", "edgar:9"): "wrong row"})
    check("a record-scoped veto splits out exactly that record",
          [c["members"] for c in surgical["components"]],
          [["dol5500:1", "dol5500:2"]])
    check("and it is counted as a refusal, not dropped silently",
          surgical["refusals"]["key_veto_record"], 1)
    global_veto = plan(recs, [], {("ein", "123456789", "*"): "junk EIN"})
    check("a global veto dissolves the component entirely",
          global_veto["components"], [])
    check("leaving the records in the no_strong_key remainder",
          global_veto["metrics"]["records_no_strong_key"], 3)
    check("undoing the veto restores the merge byte for byte",
          plan(recs, [], {}) == plan(recs, [], {}), True)
    check("and the restored component is the original",
          [c["members"] for c in plan(recs, [], {})["components"]],
          [["dol5500:1", "dol5500:2", "edgar:9"]])

    print("\nrefusals")
    check("the all-zero EIN sentinel is never a key",
          plan([_rec("a:1", ein="000000000"), _rec("a:2", ein="000000000")],
               [], {})["components"], [])
    check("a repeated-digit CIK is never a key",
          plan([_rec("a:1", cik="9999999999"), _rec("a:2", cik="9999999999")],
               [], {})["components"], [])
    over = [_rec(f"a:{i}", ein="123456789") for i in range(KEY_FANOUT_CAP + 2)]
    r = plan(over, [], {})
    check("a key over the fanout cap is refused",
          r["refusals"]["key_fanout_over_cap"], 1)
    check("and refusing it materializes nothing", r["components"], [])
    check("state_entity_id with no state is refused, not nationalised",
          plan([_rec("a:1", state_entity_id="C1234567"),
                _rec("a:2", state_entity_id="C1234567")],
               [], {})["components"], [])
    check("the same ids WITH a state resolve",
          [c["members"] for c in plan(
              [_rec("a:1", state_entity_id="C1234567", state="CA"),
               _rec("a:2", state_entity_id="c-1234567", state="CA")],
              [], {})["components"]], [["a:1", "a:2"]])
    check("but not across two states",
          plan([_rec("a:1", state_entity_id="C1234567", state="CA"),
                _rec("a:2", state_entity_id="C1234567", state="NY")],
               [], {})["components"], [])

    print("\nnames and addresses never merge anything")
    check("identical normalized names do not resolve together",
          plan([_rec("a:1", legal_name="LaserAway", name_norm="laseraway",
                     domain="laseraway.com", state="CA", zip5="90210"),
                _rec("a:2", legal_name="LASERAWAY INC", name_norm="laseraway",
                     domain="laseraway.com", state="CA", zip5="90210")],
               [], {})["components"], [])

    print("\ndeterminism")
    recs = [_rec("dol5500:1", ein="123456789"), _rec("dol5500:2", ein="123456789"),
            _rec("edgar:3", cik="0000000042"), _rec("fin:4", crd="42")]
    bridge = [("42", "42", "cover_page_crd", "coverpage")]
    a = plan(recs, bridge, {})
    b = plan(list(reversed(recs)), bridge, {})
    check("input order does not change the components",
          [c["members"] for c in a["components"]],
          [c["members"] for c in b["components"]])

    print("\ncanonical row")
    by_key = {r["record_key"]: r for r in [
        _rec("dol5500:1", ein="123456789", cik="0000000042", state="CA",
             legal_name="Acme Dental LLC", name_norm="acme dental"),
        _rec("dol5500:2", ein="12-3456789", cik="42", state="NY",
             legal_name="ACME DENTAL", name_norm="acme dental"),
        _rec("dol5500:3", ein="123456789", state="CA",
             legal_name="Acme Dental Group", name_norm="acme dental group")]}
    cols, conflicts = canonical_row(sorted(by_key), by_key)
    check("an agreed EIN is promoted", cols["ein"], "123456789")
    check("padded and bare CIK count as agreement", cols["cik"], "42")
    check("a disagreed state is left NULL, never picked",
          cols["canonical_state"], None)
    check("and the competing values are recorded",
          conflicts["canonical_state"], ["CA", "NY"])
    check("canonical_name is the modal spelling",
          cols["canonical_name"], "Acme Dental LLC")
    check("with the variant count alongside it",
          conflicts["name_norm_distinct"], 2)

    print("\nveto target parsing")
    check("global form", parse_veto_target("ein=123456789"),
          ("ein", "123456789", "*"))
    check("record-scoped form", parse_veto_target("ein=123456789@dol5500:99"),
          ("ein", "123456789", "dol5500:99"))
    check("crosswalk form", parse_veto_target("xwalk:cik_crd=1002784:104720"),
          ("xwalk:cik_crd", "1002784:104720", "*"))
    try:
        parse_veto_target("nope=1")
        check("an unknown key type is refused", "no error", "ValueError")
    except ValueError:
        check("an unknown key type is refused", "ValueError", "ValueError")


    # -----------------------------------------------------------------------
    # the four entity-id rules. NONE of these has ever fired on the real
    # corpus (er_entity_merge: 0 rows; all 13 er_resolve_run ledger rows report
    # entities_merged = entities_split = 0), and 'ent:<id>' is a foreign key
    # the rest of the app dereferences. So they are proven here, DB-free,
    # against the pure function rather than against data that has not arrived.
    # -----------------------------------------------------------------------
    print("\nentity id assignment -- rule 1: a new component gets a new id")
    comps2 = [{"members": ["a:1", "a:2"], "tier": "exact_key",
               "keys": [("ein", "111111111")]}]
    r = assign_ids(comps2, {}, {})
    check("no prior membership -> needs a new id", r["new_entities"], [0])
    check("...and nothing is reused", r["assigned"], {})
    check("...no merge", r["merges"], [])
    check("...no split", r["splits"], [])

    print("\nrule 2: one component onto one entity REUSES that id")
    r = assign_ids(comps2, {"a:1": 7, "a:2": 7}, {})
    check("the id is reused", r["assigned"], {0: 7})
    check("...not issued anew", r["new_entities"], [])
    check("a partly-known component still reuses",
          assign_ids(comps2, {"a:1": 7}, {})["assigned"], {0: 7})

    print("\nrule 2b: a dissolved entity's id is RECLAIMED, not reissued")
    r = assign_ids(comps2, {}, {"a:1": 43, "a:2": 43})
    check("the dissolved id comes back", r["assigned"], {0: 43})
    check("...counted as a reclaim", r["reclaimed"], 1)
    check("a LIVE claim beats the dissolved fallback",
          assign_ids(comps2, {"a:1": 9}, {"a:1": 43})["assigned"], {0: 9})
    check("...and is not counted as a reclaim",
          assign_ids(comps2, {"a:1": 9}, {"a:1": 43})["reclaimed"], 0)

    print("\nrule 3: MERGE -- one component onto two entities, lowest survives")
    r = assign_ids(comps2, {"a:1": 12, "a:2": 4}, {})
    check("the LOWEST (oldest) id survives", r["assigned"], {0: 4})
    check("the other is absorbed, forwarded to the survivor",
          [(a, s) for a, s, _c in r["merges"]], [(12, 4)])
    check("...with a cause that names the tier",
          r["merges"][0][2]["tier"], "exact_key")
    check("...and the members that joined them",
          r["merges"][0][2]["members"], ["a:1", "a:2"])
    check("a merge is not a split", r["splits"], [])
    check("id order in the prior map cannot change the survivor",
          assign_ids(comps2, {"a:1": 4, "a:2": 12}, {})["assigned"], {0: 4})
    r3 = assign_ids([{"members": ["a:1", "a:2", "a:3"], "tier": "exact_key",
                      "keys": []}], {"a:1": 30, "a:2": 8, "a:3": 19}, {})
    check("three entities collapse to the lowest", r3["assigned"], {0: 8})
    check("...both others forwarded",
          sorted((a, s) for a, s, _c in r3["merges"]), [(19, 8), (30, 8)])

    print("\nrule 4: SPLIT -- one entity claimed by two components")
    split_comps = [{"members": ["a:1"], "tier": "exact_key", "keys": []},
                   {"members": ["a:2", "a:3", "a:4"], "tier": "exact_key",
                    "keys": []}]
    r = assign_ids(split_comps, {"a:1": 5, "a:2": 5, "a:3": 5, "a:4": 5}, {})
    check("the LARGEST fragment keeps the id", r["assigned"], {1: 5})
    check("...the smaller fragment becomes a new entity",
          r["new_entities"], [0])
    check("...the loss is reported, not silent",
          r["splits"], [{"component_first_member": "a:1",
                         "lost_entity_ids": [5]}])
    check("a split is not a merge", r["splits"] and not r["merges"], True)
    tie = [{"members": ["b:9", "b:9z"], "tier": "exact_key", "keys": []},
           {"members": ["a:1", "a:2"], "tier": "exact_key", "keys": []}]
    r = assign_ids(tie, {"b:9": 5, "b:9z": 5, "a:1": 5, "a:2": 5}, {})
    check("equal fragments tie-break on the SMALLEST member record_key",
          r["assigned"], {1: 5})
    check("...independent of component order",
          assign_ids(list(reversed(tie)),
                     {"b:9": 5, "b:9z": 5, "a:1": 5, "a:2": 5},
                     {})["assigned"], {0: 5})

    print("\nid assignment: invariants that must hold on every shape")
    mixed = [{"members": ["a:1", "a:2"], "tier": "exact_key", "keys": []},
             {"members": ["b:1"], "tier": "exact_key", "keys": []},
             {"members": ["c:1", "c:2", "c:3"], "tier": "crosswalk",
              "keys": []}]
    prior_mixed = {"a:1": 5, "a:2": 11, "b:1": 5, "c:1": 2}
    r = assign_ids(mixed, prior_mixed, {})
    check("every component gets exactly one outcome",
          sorted(list(r["assigned"]) + r["new_entities"]), [0, 1, 2])
    check("no id is handed to two components",
          len(set(r["assigned"].values())), len(r["assigned"]))
    check("a merge and a split can coexist in one run",
          (bool(r["merges"]), bool(r["splits"])), (True, True))
    check("assignment is deterministic under input reordering",
          assign_ids(mixed, dict(reversed(list(prior_mixed.items()))), {}),
          r)
    check("an absorbed id is never also an assigned id",
          set(a for a, _s, _c in r["merges"]) & set(r["assigned"].values()),
          set())
    check("empty corpus is not an error",
          assign_ids([], {}, {}),
          {"assigned": {}, "new_entities": [], "merges": [], "splits": [],
           "reclaimed": 0})
    print("\n" + (f"{len(fails)} FAILURE(S)" if fails else "all checks passed"))
    for f in fails:
        print("  " + f)
    return 1 if fails else 0

if __name__ == "__main__":
    import sys

    sys.exit(_selftest())

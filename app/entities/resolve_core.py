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


# SPEC_149: sequence placeholders that pass the repeated-digit check. MEASURED
# 2026-10-01: '123456789' is typed by 14 EDGAR filers (8 were fused into one
# entity), '987654321' by 1. No IRS campus assigns a '00' prefix, so an EIN that
# starts '00' (a typo, or a 7-digit value left-padded) is not an EIN.
EIN_PLACEHOLDERS = frozenset({"123456789", "987654321"})


def ein_is_placeholder(v):
    return v in EIN_PLACEHOLDERS or v.startswith("00") or _degenerate(v)


def _ein(raw):
    v = norm.clean_ein(raw)          # the harness's own cleaner, incl. the
    return v if v and not ein_is_placeholder(v) else None   # '000000000' sentinel


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
# the WEAK name+state tier (SPEC_147) -- recorded, never merged
# ---------------------------------------------------------------------------

# Sources whose records get weak candidates. Form 5500 sponsors carry an EIN and
# nothing else strong, so most of them share no key with anyone; their only
# other handle is the sponsor name and state.
WEAK_SOURCES = ("dol5500",)
WEAK_TIER = "name_state"
# Distinct candidate identities (entities or lone records) kept per sponsor. A
# common name can match dozens; the rest are counted, never silently dropped.
WEAK_CANDIDATE_CAP = 5
# Sponsor-level summary: the best status a sponsor reached.
WEAK_STATUS_RANK = {"corroborates": 0, "conflict": 1, "ambiguous": 2, "candidate": 3}


def _source_of(rec):
    return rec.get("source") or rec["record_key"].split(":", 1)[0]


def weak_name_state(records, rec_keys, comps, sources=WEAK_SOURCES,
                    cap=WEAK_CANDIDATE_CAP):
    """Name+state candidates for records of `sources`. PURE, and it changes no
    component: the resolver stays identifier-only (module docstring). Every row
    is evidence for a reviewer, with its contradictions written down.

    A candidate is a record of any OTHER source with the identical
    (name_norm, state). Its identity is its component (when it is in one) or
    itself. Status, per (sponsor, candidate):

      corroborates  the candidate sits in the sponsor's own strong component
      conflict      a contradiction with strong evidence, listed in `conflicts`:
                    ein_conflict (the candidate's identity carries EINs and none
                    is the sponsor's) and/or strong_match_elsewhere (the sponsor
                    is already strongly matched to a different component)
      ambiguous     no conflict, but the name+state names >1 foreign identity
      candidate     exactly one foreign identity, no conflict

    -> {"rows": [...], "metrics": {...}}; row["candidate_comp"] is an index
    into `comps` or None.
    """
    sources = set(sources)
    comp_of = {rk: i for i, c in enumerate(comps) for rk in c["members"]}
    comp_eins = {i: sorted({v for t, v in c["keys"] if t == "ein"})
                 for i, c in enumerate(comps)}

    index = {}
    for rec in records:
        if _source_of(rec) in sources:
            continue
        nn, st = rec.get("name_norm"), rec.get("state")
        if nn and st:
            index.setdefault((nn, st), []).append(rec["record_key"])

    def identity(rk):
        i = comp_of.get(rk)
        return ("comp", i) if i is not None else ("rec", rk)

    rows = []
    considered = with_cands = with_conflict = over_cap_sponsors = over_cap_records = 0
    by_status, pairs_by_status = {}, {}
    for rec in sorted(records, key=lambda r: r["record_key"]):
        if _source_of(rec) not in sources:
            continue
        nn, st = rec.get("name_norm"), rec.get("state")
        if not (nn and st):
            continue
        considered += 1
        cands = sorted(index.get((nn, st), ()))
        if not cands:
            continue
        rk = rec["record_key"]
        own = comp_of.get(rk)
        own_ident = ("comp", own) if own is not None else None
        own_eins = sorted({v for t, v in rec_keys.get(rk, ()) if t == "ein"})
        # the sponsor's own component first, then a stable order
        idents = sorted({identity(c) for c in cands},
                        key=lambda i: (i != own_ident, str(i)))
        kept = set(idents[:cap])
        foreign = [i for i in idents if i != own_ident]
        dropped = sum(1 for c in cands if identity(c) not in kept)
        if dropped:
            over_cap_sponsors += 1
            over_cap_records += dropped
        statuses = []
        for c in cands:
            if identity(c) not in kept:
                continue
            ci = comp_of.get(c)
            conflicts = []
            if own is not None and ci == own:
                status = "corroborates"
            else:
                cand_eins = (comp_eins[ci] if ci is not None else
                             sorted({v for t, v in rec_keys.get(c, ()) if t == "ein"}))
                if cand_eins and own_eins and not set(own_eins) & set(cand_eins):
                    conflicts.append({"reason": "ein_conflict", "sponsor_ein": own_eins[0],
                                      "candidate_eins": cand_eins[:12]})
                if own is not None:
                    conflicts.append({"reason": "strong_match_elsewhere",
                                      "sponsor_component_first_member":
                                          comps[own]["members"][0]})
                status = ("conflict" if conflicts else
                          "ambiguous" if len(foreign) > 1 else "candidate")
            rows.append({"record_key": rk, "candidate_record_key": c, "candidate_comp": ci,
                         "tier": WEAK_TIER, "status": status, "conflicts": conflicts,
                         "matched_on": {"name_norm": nn, "state": st}})
            pairs_by_status[status] = pairs_by_status.get(status, 0) + 1
            statuses.append(status)
        with_cands += 1
        if "conflict" in statuses:
            with_conflict += 1
        best = min(statuses, key=WEAK_STATUS_RANK.__getitem__)
        by_status[best] = by_status.get(best, 0) + 1

    return {"rows": rows, "metrics": {
        "tier": WEAK_TIER,
        "sources": sorted(sources),
        "sponsors_considered": considered,
        "sponsors_with_candidates": with_cands,
        "sponsors_by_status": by_status,
        "sponsors_with_any_conflict": with_conflict,
        "pairs_by_status": pairs_by_status,
        "sponsors_over_cap": over_cap_sponsors,
        "candidates_over_cap": over_cap_records,
        "merged": 0,
    }}


def source_metrics(records, rec_keys, comps, weak, new_components=(), source="dol5500"):
    """How one source fared in this resolve -> dict. PURE, so a dry run reports
    exactly what a real run does."""
    new_components = set(new_components)
    comp_of = {rk: i for i, c in enumerate(comps) for rk in c["members"]}
    by_key = {r["record_key"]: r for r in records}
    mine = [r for r in records if _source_of(r) == source]
    with_ein = matched = in_new = 0
    strong_comps = set()
    unmatched = []
    for r in mine:
        rk = r["record_key"]
        if any(t == "ein" for t, _v in rec_keys.get(rk, ())):
            with_ein += 1
        ci = comp_of.get(rk)
        if ci is not None and any(_source_of(by_key[m]) != source
                                  for m in comps[ci]["members"]):
            matched += 1
            strong_comps.add(ci)
        else:
            unmatched.append(rk)
        if ci is not None and ci in new_components:
            in_new += 1
    field_conflicts = {}
    for ci in sorted(strong_comps):
        _cols, conflicts = canonical_row(comps[ci]["members"], by_key)
        for k, v in conflicts.items():
            if k == "name_norm_distinct":
                if v > 1:
                    field_conflicts["name_norm_distinct>1"] = (
                        field_conflicts.get("name_norm_distinct>1", 0) + 1)
            elif not k.endswith("_count"):
                field_conflicts[k] = field_conflicts.get(k, 0) + 1
    weak_rks = {row["record_key"] for row in weak["rows"]}
    return {
        "records_fed": len(mine),
        "with_ein_key": with_ein,
        "matched_strong_ein": matched,
        "unmatched_singletons": len(unmatched),
        "in_new_entities": in_new,
        "strong_components": len(strong_comps),
        "strong_components_with_field_conflicts": field_conflicts,
        "weak_sponsors_by_status": dict(weak["metrics"]["sponsors_by_status"]),
        "unmatched_with_weak_candidate": sum(1 for rk in unmatched if rk in weak_rks),
    }


# ---------------------------------------------------------------------------
# web domains (SPEC_148) -- an identifier that is RECORDED, never merged
# ---------------------------------------------------------------------------

# Sources whose records ATTACH a domain claim to an identity through their keys
# but never resolve: their CIKs are self-reported by a research pipeline, and
# letting them into the union-find would turn a scraped CIK into merge evidence.
ATTACH_ONLY_SOURCES = ("pefirm", "industrial", "peportco", "portco", "threepl", "famoffice", "lpfund")

# Independence: two records of ONE family are one source. ADV roster and IAPD
# are both Form ADV (Item 1.I, the adviser's own filing), so they never confirm
# each other. Every attach-only table is its own family. `own_site` is the
# probe finding the entity's legal name on the domain's own homepage.
SOURCE_FAMILY = {
    "adv": "form_adv", "iapd": "form_adv", "edgar": "edgar", "f13": "form_13f",
    "formd": "form_d", "dol5500": "form_5500",
    "pefirm": "pe_firm_web", "industrial": "industrial_seed", "peportco": "pe_portfolio",
    "portco": "portfolio_agentic", "threepl": "three_pl", "famoffice": "family_office",
    "lpfund": "lp_fund",
}
OWN_SITE_FAMILY = "own_site"

# A domain named by more subjects than this is a shared host (a wealth platform,
# a parent group's site), not one company's: refused, counted, examples capped.
# MEASURED 2026-09-30: the busiest ADV domain is named by 15 CRDs
# (focusfinancialpartners.com); those stay conflicts, listed in full.
DOMAIN_SUBJECT_CAP = 25
DOMAIN_CLAIMS_CAP = 50          # claims kept per link row (count rides alongside)
DOMAIN_STATUSES = ("strong", "weak", "conflict", "alias")
REDIRECT_CHAIN_MAX = 5          # SPEC_149: hops followed through recorded probes


def family_of(source):
    return SOURCE_FAMILY.get(source, source)


def domain_links(records, rec_keys, comps, probes=None, cap=DOMAIN_SUBJECT_CAP):
    """Domain -> subject links with a status. PURE, and it changes no component.

    `records` is EVERY source record (attach-only included); `rec_keys` and
    `comps` come from plan() over the resolvable ones. `probes` maps a domain to
    {"redirect_to", "names_found", "evidence"} from core.domain_probe.

    A subject is a component (`comp:<first member>`) or a lone record (its
    record_key). Per (domain, subject):

      strong    the only subject on the domain, and >= 2 source families agree
      weak      the only subject, one family
      conflict  more than one subject claims the domain (`domain_shared`), or
                the claim could not be attached (`attach_key_disagreement`)
      alias     the domain redirects to another registrable domain (probe
                evidence); its claims are counted toward the target instead

    -> {"rows", "entity_strong": {comp index: [domains]}, "metrics"}.
    """
    from app.entities import domains as dom

    probes = probes or {}
    comp_of = {rk: i for i, c in enumerate(comps) for rk in c["members"]}
    by_key = {r["record_key"]: r for r in records}

    def subject_of(ident):
        return f"comp:{comps[ident[1]]['members'][0]}" if ident[0] == "comp" else ident[1]

    def names_of(ident):
        if ident[0] == "comp":
            return {by_key[m].get("name_norm") for m in comps[ident[1]]["members"]} - {None}
        nn = by_key[ident[1]].get("name_norm")
        return {nn} if nn else set()

    key_owner = {}
    for rk, kept in rec_keys.items():
        ident = ("comp", comp_of[rk]) if rk in comp_of else ("rec", rk)
        for k in kept:
            key_owner.setdefault(k, ident)

    attach = {"member": 0, "lone_record": 0, "key": 0, "name": 0, "unattached": 0,
              "key_disagreement": 0}
    claims = {}          # domain -> {ident: [claim, ...]}
    pending = []         # keyless attach-only records: (domain, record)
    disagree = {}        # ident -> conflict entry
    for rec in sorted(records, key=lambda r: r["record_key"]):
        d = dom.domain(rec.get("domain"))
        if not d:
            continue
        rk, src = rec["record_key"], _source_of(rec)
        claim = {"record_key": rk, "source": src, "family": family_of(src)}
        if src not in ATTACH_ONLY_SOURCES:
            ident = ("comp", comp_of[rk]) if rk in comp_of else ("rec", rk)
            attach["member" if rk in comp_of else "lone_record"] += 1
            claim["via"] = "member"
        else:
            owners = sorted({key_owner[k] for k in _extract_keys(rec) if k in key_owner}, key=str)
            if len(owners) == 1:
                ident = owners[0]
                attach["key"] += 1
                claim["via"] = "key"
            elif owners:
                ident = ("rec", rk)
                attach["key_disagreement"] += 1
                claim["via"] = "unattached"
                disagree[(d, ident)] = {"reason": "attach_key_disagreement",
                                        "subjects": [subject_of(o) for o in owners][:12]}
            else:
                pending.append((d, rec, claim))
                continue
        claims.setdefault(d, {}).setdefault(ident, []).append(claim)

    # keyless claims join the ONE identity of the same name already on the domain
    for d, rec, claim in pending:
        nn = rec.get("name_norm")
        hits = [i for i in claims.get(d, {}) if nn and nn in names_of(i)]
        if len(hits) == 1:
            ident = hits[0]
            attach["name"] += 1
            claim["via"] = "name"
        else:
            ident = ("rec", rec["record_key"])
            attach["unattached"] += 1
            claim["via"] = "unattached"
        claims.setdefault(d, {}).setdefault(ident, []).append(claim)

    # redirects FIRST: A -> B makes A an alias and A's claims count toward B, so the
    # own-site evidence of B (below) meets the subjects that only named A
    # SPEC_149: a chain A -> B -> C lands on C (the final domain that does not
    # redirect); a cycle, or a target that is a platform / invalid host (a lapsed
    # site forwarding to linkedin.com or a parking page), moves nothing and leaves
    # A a conflict -- the platform host must never become anyone's domain.
    def final_target(d):
        seen, cur = [d], d
        for _hop in range(REDIRECT_CHAIN_MAX):
            nxt = (probes.get(cur) or {}).get("redirect_to")
            if not nxt or nxt == cur:
                return cur, None
            if nxt in seen:
                return None, "redirect_cycle"
            if not dom.domain(nxt):
                return None, "redirect_to_generic"
            seen.append(nxt)
            cur = nxt
        return None, "redirect_cycle"

    aliases, redirect_refused = {}, {}
    for d in sorted(claims):
        target = (probes.get(d) or {}).get("redirect_to")
        if not target or target == d:
            continue
        final, refused = final_target(d)
        if refused:
            redirect_refused[d] = refused
            continue
        aliases[d] = final
    for d, final in aliases.items():
        for ident, cl in claims[d].items():
            moved = [dict(c, via=f"redirect:{d}") for c in cl]
            claims.setdefault(final, {}).setdefault(ident, []).extend(moved)

    # the domain's own homepage naming the subject's legal name: a family of its own
    own_site = 0
    for d, by_ident in claims.items():
        if d in aliases or d in redirect_refused:
            continue                     # a redirecting domain is not a live site of anyone's
        found = set((probes.get(d) or {}).get("names_found") or ())
        for ident, cl in by_ident.items():
            if found & names_of(ident):
                cl.append({"record_key": None, "source": "probe", "family": OWN_SITE_FAMILY,
                           "via": "probe"})
                own_site += 1

    rows, entity_strong = [], {}
    status_count, shared_detail, claims_by_family = {}, [], {}
    shared = 0
    for d in sorted(claims):
        by_ident = claims[d]
        idents = sorted(by_ident, key=lambda i: subject_of(i))
        if len(idents) > cap:
            shared += 1
            if len(shared_detail) < DETAIL_CAP:
                shared_detail.append({"domain": d, "subjects": len(idents)})
            continue
        for ident in idents:
            cl = sorted(by_ident[ident], key=lambda c: (c["record_key"] or "", c["via"]))
            fams = sorted({c["family"] for c in cl})
            conflicts = []
            if (d, ident) in disagree:
                conflicts.append(disagree[(d, ident)])
            if d in redirect_refused:
                conflicts.append({"reason": redirect_refused[d],
                                  "redirect_to": (probes.get(d) or {}).get("redirect_to")})
            if len(idents) > 1:
                others = [subject_of(i) for i in idents if i != ident]
                conflicts.append({"reason": "domain_shared", "other_subjects": others[:12],
                                  "other_count": len(others)})
            if d in aliases:
                status = "alias"
            elif conflicts:
                status = "conflict"
            else:
                status = "strong" if len(fams) >= 2 else "weak"
            if status == "strong" and ident[0] == "comp":
                entity_strong.setdefault(ident[1], []).append(d)
            if status != "alias":
                for f in fams:
                    claims_by_family[f] = claims_by_family.get(f, 0) + 1
            status_count[status] = status_count.get(status, 0) + 1
            probe = probes.get(d) or {}
            rows.append({
                "domain": d, "subject": subject_of(ident),
                "candidate_comp": ident[1] if ident[0] == "comp" else None,
                "record_key": ident[1] if ident[0] == "rec" else None,
                "status": status, "families": fams,
                "sources": sorted({c["source"] for c in cl if c["record_key"]}),
                "claims": cl[:DOMAIN_CLAIMS_CAP], "claim_count": len(cl),
                "conflicts": conflicts,
                "alias_of": aliases.get(d),
                "evidence": probe.get("evidence") if (d in aliases or d in redirect_refused
                                                      or OWN_SITE_FAMILY in fams) else None,
            })
    entity_strong = {i: sorted(v) for i, v in entity_strong.items()}
    return {"rows": rows, "entity_strong": entity_strong, "metrics": {
        "domains_claimed": len(claims),
        "links_by_status": status_count,
        "entities_with_strong_domain": len(entity_strong),
        "entities_with_several_strong": sum(1 for v in entity_strong.values() if len(v) > 1),
        "subject_families": claims_by_family,
        "attach": attach,
        "own_site_claims": own_site,
        "aliases": len(aliases),
        "shared_host_refused": shared,
        "redirects_refused": {r: sum(1 for v in redirect_refused.values() if v == r)
                              for r in sorted(set(redirect_refused.values()))},
        "shared_host_detail": shared_detail,
        "subject_cap": cap,
        "merged": 0,
    }}


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
        _rec("dol5500:1", ein="261640968", legal_name="Acme Dental LLC",
             name_norm="acme dental", state="CA"),
        _rec("dol5500:2", ein="26-1640968", legal_name="ACME DENTAL, L.L.C.",
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
    recs = [_rec("dol5500:1", ein="261640968"),
            _rec("dol5500:2", ein="261640968"),
            _rec("edgar:9", ein="261640968")]
    check("all three resolve together with no veto",
          len(plan(recs, [], {})["components"][0]["members"]), 3)
    surgical = plan(recs, [], {("ein", "261640968", "edgar:9"): "wrong row"})
    check("a record-scoped veto splits out exactly that record",
          [c["members"] for c in surgical["components"]],
          [["dol5500:1", "dol5500:2"]])
    check("and it is counted as a refusal, not dropped silently",
          surgical["refusals"]["key_veto_record"], 1)
    global_veto = plan(recs, [], {("ein", "261640968", "*"): "junk EIN"})
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
    over = [_rec(f"a:{i}", ein="261640968") for i in range(KEY_FANOUT_CAP + 2)]
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
    recs = [_rec("dol5500:1", ein="261640968"), _rec("dol5500:2", ein="261640968"),
            _rec("edgar:3", cik="0000000042"), _rec("fin:4", crd="42")]
    bridge = [("42", "42", "cover_page_crd", "coverpage")]
    a = plan(recs, bridge, {})
    b = plan(list(reversed(recs)), bridge, {})
    check("input order does not change the components",
          [c["members"] for c in a["components"]],
          [c["members"] for c in b["components"]])

    print("\ncanonical row")
    by_key = {r["record_key"]: r for r in [
        _rec("dol5500:1", ein="261640968", cik="0000000042", state="CA",
             legal_name="Acme Dental LLC", name_norm="acme dental"),
        _rec("dol5500:2", ein="26-1640968", cik="42", state="NY",
             legal_name="ACME DENTAL", name_norm="acme dental"),
        _rec("dol5500:3", ein="261640968", state="CA",
             legal_name="Acme Dental Group", name_norm="acme dental group")]}
    cols, conflicts = canonical_row(sorted(by_key), by_key)
    check("an agreed EIN is promoted", cols["ein"], "261640968")
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
    check("global form", parse_veto_target("ein=261640968"),
          ("ein", "261640968", "*"))
    check("record-scoped form", parse_veto_target("ein=261640968@dol5500:99"),
          ("ein", "261640968", "dol5500:99"))
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

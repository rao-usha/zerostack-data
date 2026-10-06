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

SPEC_150 (gated shared keys): an EIN or a CRD -- and a bridge edge -- shared by
two different CIKs is "related", not "the same legal person" (a parent and its
subsidiary, an issuer and its insider, an insurer and its separate accounts). It
joins them only when `gate.corroborate` holds for the pair (names equal up to
EDGAR tags, a rename or legal-form conversion after the old CIK went dormant, a
strategy-labelled or successor 13F CIK of one adviser). Names still never merge
anything on their own: they only corroborate a shared strong key.

SPEC_154: LEI and UEI are gated the same way (a shared LEI on two CIKs is a parent's
LEI typed on a subsidiary as often as a duplicate filer account). GLEIF and USAspending
records carry no CIK: they cluster on their LEI / UEI and attach by rule 5.

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


def _lei_checksum_ok(v):
    """ISO 17442 / ISO 7064 MOD 97-10: letters A..Z read as 10..35, the number mod 97 is 1."""
    return int("".join(str(int(c, 36)) for c in v)) % 97 == 1


def _lei(raw):
    # SPEC_154 review: the check digits are enforced. MEASURED 2026-10-03, 16 sec_filers values
    # of 20 alphanumerics fail them (a trust's name, registry numbers, typos of real LEIs).
    v = _ALNUM.sub("", str(raw or "").upper())
    return v if len(v) == 20 and not _degenerate(v) and _lei_checksum_ok(v) else None


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


# SPEC_150: these join two CIK groups only when corroborated. SPEC_154 adds LEI and UEI: MEASURED
# 2026-10-03, 6 LEIs are typed on more than one sec_filers CIK (a parent's LEI on a subsidiary or
# fund filer), so an LEI alone says "related" exactly as a shared EIN does. Records without a CIK
# (GLEIF, USAspending) still cluster on them freely and attach by rule 5.
GATED_KEY_TYPES = ("ein", "crd", "lei", "uei")
# SPEC_154 review: a no-CIK cluster whose ONLY link to the CIK classes is one of these keys
# attaches only when its name matches a CIK of the class (current or former EDGAR name). The
# EDGAR side of an LEI is self-reported: MEASURED 2026-10-03, of 168 GLEIF records attached by
# LEI alone, 6 named a different legal person (22C Capital LLC typed Bloomberg Finance L.P.'s
# LEI; managers' LEIs typed on their funds and SPVs). Unmatched, the record stays its own
# piece and, as the anchor, owns the LEI (rule 6), which is withheld from the filer.
NAME_GATED_ATTACH_TYPES = ("lei", "uei")


def _first_steps(records, bridge_edges, vetoes):
    """Steps 1-2 of plan(): record keys (vetoes, fanout cap) and the live bridge edges."""
    refusals = {"key_veto_record": 0, "key_veto_global": 0,
                "key_fanout_over_cap": 0, "xwalk_veto": 0,
                "component_over_cap": 0}
    refused_detail = {"fanout": [], "oversize_components": []}
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

    # fanout cap -- a key asserted by too many records is a placeholder
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

    xwalk_used = []
    for cik, crd, tier, matched_on in bridge_edges:
        pair = f"{cik}:{crd}"
        if (XWALK_KEY_TYPE, pair, "*") in vetoes:
            refusals["xwalk_veto"] += 1
            continue
        # An edge between two identifiers nobody in the corpus asserts joins
        # nothing. Counted separately so "the bridge has 2,271 usable rows" is
        # never confused with "2,271 of them did anything here".
        if (("cik", cik) not in key_to_recs or ("crd", crd) not in key_to_recs
                or ("cik", cik) in over_cap or ("crd", crd) in over_cap):
            continue
        xwalk_used.append({"cik": cik, "crd": crd, "bridge_tier": tier,
                           "matched_on": matched_on})
    return (rec_keys, key_to_recs, key_assertions_by_type, refusals,
            refused_detail, xwalk_used, over_cap)


def _groups(rec_keys, xwalk_used):
    """SPEC_150 structure: ungated groups, their CIKs and gated keys, the no-CIK
    anchor clusters, and which CIK groups hold each gated key.

    An UNGATED group is the records joined by CIK / state id (SPEC_154: LEI and UEI
    are gated like EIN and CRD). A group with a CIK is a "CIK group"; the rest are anchors (ADV, IAPD,
    Form 5500, ...). Anchors sharing an EIN / CRD cluster freely: there is no CIK
    between them to protect.
    """
    uf = _UF()
    for rk, kept in rec_keys.items():
        if not kept:
            continue
        node = _rec_node(rk)
        uf.add(node)
        for key_type, val in kept:
            if key_type in GATED_KEY_TYPES:
                continue
            kn = _key_node(key_type, val)
            uf.add(kn)
            uf.union(node, kn)
    members, gciks, gkeys = {}, {}, {}
    for rk in sorted(rec_keys):
        kept = rec_keys[rk]
        if not kept:
            continue
        g = uf.find(_rec_node(rk))
        members.setdefault(g, []).append(rk)
        for key_type, val in kept:
            if key_type == "cik":
                gciks.setdefault(g, set()).add(val)
            elif key_type in GATED_KEY_TYPES:
                gkeys.setdefault(g, {})[(key_type, val)] = "record"
    cik_group = {c: g for g, cs in gciks.items() for c in cs}
    for e in xwalk_used:
        g = cik_group.get(e["cik"])
        if g is not None:
            gkeys.setdefault(g, {}).setdefault(("crd", e["crd"]), "xwalk")

    # anchors: no-CIK groups, clustered on their (record-asserted) gated keys
    auf = _UF()
    anchors = sorted(g for g in members if g not in gciks)
    for g in anchors:
        auf.add("g:" + g)
        for k in gkeys.get(g, {}):
            kn = _key_node(*k)
            auf.add(kn)
            auf.union("g:" + g, kn)
    clusters = {}
    for g in anchors:
        clusters.setdefault(auf.find("g:" + g), []).append(g)
    clusters = sorted(sorted(v) for v in clusters.values())

    key_groups = {}                        # gated key -> CIK groups holding it
    for g in sorted(gciks):
        for k in gkeys.get(g, {}):
            key_groups.setdefault(k, []).append(g)
    return {"members": members, "gciks": gciks, "gkeys": gkeys, "cik_group": cik_group,
            "clusters": clusters, "key_groups": key_groups}


def _cluster_candidates(G, cluster):
    cands = set()
    for g in cluster:
        for k in G["gkeys"].get(g, {}):
            cands.update(G["key_groups"].get(k, ()))
    return cands


def _squash(name):
    """A name with spacing and punctuation gone ('TheGoodEarCompany, Inc.' == 'THE GOOD EAR
    COMPANY, INC.'): the name-gated attach accepts it as equal (SPEC_154 review)."""
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _cluster_name_gated(G, cluster):
    """True when every key linking the cluster to a CIK group is an LEI / UEI (SPEC_154)."""
    link = {k[0] for g in cluster for k in G["gkeys"].get(g, {}) if G["key_groups"].get(k)}
    return bool(link) and link <= set(NAME_GATED_ATTACH_TYPES)


def gated_ciks(records, bridge_edges, vetoes):
    """The CIKs whose filer profile the gate needs (SPEC_150): every CIK that shares
    an EIN / CRD / bridge CRD with a different CIK group, or is one of several
    groups a no-CIK record could attach to. PURE; resolve.py loads exactly these."""
    steps = _first_steps(records, bridge_edges, vetoes)
    G = _groups(steps[0], steps[5])
    out = set()
    for groups in G["key_groups"].values():
        if len(groups) > 1:
            for g in groups:
                out |= G["gciks"][g]
    for cluster in G["clusters"]:
        cands = _cluster_candidates(G, cluster)
        if len(cands) > 1 or (cands and _cluster_name_gated(G, cluster)):
            for g in cands:
                out |= G["gciks"][g]
    return out


def _cap_list(block, name, items):
    """Sorted, capped evidence list with its exact count alongside when capped."""
    items = sorted(items, key=lambda d: sorted((k, str(v)) for k, v in d.items()))
    if items:
        block[name] = items[:DETAIL_CAP]
        if len(items) > DETAIL_CAP:
            block[name + "_count"] = len(items)


def plan(records, bridge_edges, vetoes, profiles=None, crd_hints=()):
    """Compute the resolution. PURE: no DB, no clock, no randomness.

    SPEC_150: CIK / state ids union records as always. An EIN, a CRD, an LEI or a UEI
    (SPEC_154) (and a CIK<->CRD bridge edge) joins two CIK groups only when `gate.corroborate`
    says the pair is one legal person; records with no CIK attach to one class by
    rule 5; an EIN / CRD left on several pieces gets one owner. `profiles` maps a
    canonical CIK to its EDGAR filer profile; without one the gate sees only record
    names and never calls two CIKs sequential. No run date: every gate rule is
    data-relative, and "active" (main piece) is measured against the newest filing
    the profiles show, so the same data plans the same on any day. `crd_hints`
    ((cik, crd) pairs, e.g. 13F bridge edges on a CRD that maps to several CIKs) are
    REVIEW-ONLY: they let a split EIN-sharing pair carry the A2 / A4 flag and never
    union anything.

    -> dict with 'components' (materializable, >=2 records), 'record_keys'
    (record_key -> [(type, value)]), 'refusals', 'metrics', and the gate's
    'key_owner' (contested key -> owning identity), 'gate_refused_by_cik' and
    'gate_decisions'.
    """
    from app.entities import gate

    profiles = profiles or {}
    (rec_keys, key_to_recs, key_assertions_by_type, refusals,
     refused_detail, xwalk_used, over_cap) = _first_steps(records, bridge_edges, vetoes)
    G = _groups(rec_keys, xwalk_used)
    members, gciks, gkeys, cik_group = G["members"], G["gciks"], G["gkeys"], G["cik_group"]
    by_key = {r["record_key"]: r for r in records}

    # gate features, cached; a CIK with no profile falls back to its EDGAR record's name
    fallback = {}
    for g in gciks:
        for rk in members[g]:
            r = by_key[rk]
            if not r.get("legal_name"):
                continue
            pri = (0 if (r.get("source") or rk.split(":", 1)[0]) == "edgar" else 1, rk)
            for t, v in rec_keys[rk]:
                if t == "cik" and (v not in fallback or pri < fallback[v][0]):
                    fallback[v] = (pri, r["legal_name"])
    feat_cache = {}
    data_asof = max((d for d in (gate._d(p.get("last_filed")) for p in profiles.values()) if d),
                    default=None)

    def F(cik):
        f = feat_cache.get(cik)
        if f is None:
            f = feat_cache[cik] = gate.features(
                cik, profiles.get(cik), (fallback.get(cik) or (None, None))[1])
        return f

    # uf_a: the record graph without bridge edges (decides the tier);
    # ucb: the group graph with everything (decides the pieces)
    uf_a = _UF()
    ucb = _UF()
    for g, rks in members.items():
        ucb.add(g)
        for rk in rks:
            uf_a.add(_rec_node(rk))
            uf_a.union(_rec_node(rks[0]), _rec_node(rk))

    def head(g):
        return _rec_node(members[g][0])

    for cluster in G["clusters"]:          # anchors sharing a key are one identity
        for g in cluster[1:]:
            ucb.union(cluster[0], g)
            uf_a.union(head(cluster[0]), head(g))

    # gated CIK pairs: every two CIKs of different groups sharing an EIN / CRD
    cik_keys = {c: gkeys.get(g, {}) for g, cs in gciks.items() for c in cs}
    pairs = set()
    for groups in G["key_groups"].values():
        if len(groups) < 2:
            continue
        ks = sorted(c for g in groups for c in gciks[g])
        for i, a in enumerate(ks):
            for b in ks[i + 1:]:
                if cik_group[a] != cik_group[b]:
                    pairs.add((a, b))
    hint_crds = {}                         # cik -> CRDs it names (records, xwalk, hints)
    for c, ks in cik_keys.items():
        hint_crds[c] = {v for t, v in ks if t == "crd"}
    for hc, hr in crd_hints or ():
        hc, hr = _cik(hc), _crd(hr)
        if hc and hr:
            hint_crds.setdefault(hc, set()).add(hr)
    decisions = []
    for a, b in sorted(pairs):
        shared = sorted(set(cik_keys[a]) & set(cik_keys[b]))
        share_crd = any(t == "crd" for t, _v in shared)
        share_ein = any(t == "ein" for t, _v in shared)
        rule, reason = gate.corroborate(F(a), F(b), share_crd)
        decisions.append({"a": a, "b": b, "rule": rule, "reason": reason,
                          "keys": [f"{t}:{v}" for t, v in shared],
                          "share_crd": share_crd, "share_ein": share_ein,
                          "crd_hint": bool(hint_crds.get(a, set()) & hint_crds.get(b, set()))})
        if rule:
            ga, gb = cik_group[a], cik_group[b]
            ucb.union(ga, gb)
            if any(cik_keys[a][k] == "record" and cik_keys[b][k] == "record" for k in shared):
                uf_a.union(head(ga), head(gb))

    # rule 5: no-CIK clusters attach to one class (decided against the CIK classes
    # as the pairs left them, before any attachment)
    class_ciks = {}
    for g, cs in gciks.items():
        class_ciks.setdefault(ucb.find(g), set()).update(cs)
    attach_ev, attach_how, attaches = [], {}, []
    for cluster in G["clusters"]:
        cands = sorted({ucb.find(g) for g in _cluster_candidates(G, cluster)})
        if not cands:
            continue
        rn, forms = set(), set()
        for g in cluster:
            for rk in members[g]:
                rn |= gate.record_name_keys(by_key[rk].get("legal_name"))
                forms.add(gate.legal_form(by_key[rk].get("legal_name")))
        cc = {c: sorted(class_ciks[c]) for c in cands}
        if len(cands) == 1 and _cluster_name_gated(G, cluster):
            squashed = {_squash(by_key[rk].get("legal_name")) for g in cluster for rk in members[g]}
            squashed.discard("")
            if any(rn & F(x)["allkeys"] or _squash(F(x)["name"]) in squashed
                   for x in cc[cands[0]]):
                to, how = cands[0], "only_class"
            else:
                to, how = None, "own_entity_name_mismatch"
                attach_ev.append({"record_key": members[cluster[0]][0], "how": how,
                                  "candidates": 1})
        elif len(cands) == 1:
            to, how = cands[0], "only_class"
        else:
            named = [c for c in cands if any(rn & F(x)["allkeys"] for x in cc[c])]
            named_f = [c for c in named if any(F(x)["form"] in forms for x in cc[c])]
            ops = [c for c in cands if any(gate.operating(F(x)) for x in cc[c])]
            if len(named) == 1:
                to, how = named[0], "name_match"
            elif len(named_f) == 1:
                to, how = named_f[0], "name_and_form_match"
            elif len(ops) == 1:
                to, how = ops[0], "single_operating_class"
            else:
                to, how = None, "own_entity"
            attach_ev.append({"record_key": members[cluster[0]][0], "how": how,
                              "candidates": len(cands)})
        attach_how[how] = attach_how.get(how, 0) + 1
        if to is not None:
            attaches.append((cluster, to))
    for cluster, to in attaches:
        cluster_keys = {k for g in cluster for k in gkeys.get(g, {})}
        via_group = [g for g in sorted(gciks) if ucb.find(g) == ucb.find(to)
                     and any(gkeys.get(g, {}).get(k) == "record" for k in cluster_keys)]
        ucb.union(to, cluster[0])
        if via_group:
            uf_a.union(head(via_group[0]), head(cluster[0]))

    # pieces: every final identity, single-record ones included (key ownership)
    pieces = {}
    for g in sorted(members):
        pieces.setdefault(ucb.find(g), []).extend(members[g])
    for p in pieces:
        pieces[p].sort()
    piece_of_rec = {rk: p for p, rks in pieces.items() for rk in rks}
    piece_ciks = {}
    for g, cs in gciks.items():
        piece_ciks.setdefault(ucb.find(g), set()).update(cs)

    # rule 6: an EIN / CRD on two or more pieces has ONE owner (core.identifier PK)
    contested = {}
    for k, rks in key_to_recs.items():
        if k[0] not in GATED_KEY_TYPES or len(rks) < 2 or k in over_cap:
            continue
        holders = sorted({piece_of_rec[rk] for rk in rks if rk in piece_of_rec})
        if len(holders) < 2:
            continue
        anchor = sorted({piece_of_rec[rk] for rk in rks if rk in piece_of_rec
                         and not any(t == "cik" for t, _v in rec_keys[rk])})
        if len(anchor) == 1:
            owner = anchor[0]
        else:
            ops = [p for p in holders
                   if any(gate.operating(F(c)) for c in sorted(piece_ciks.get(p, ())))]
            owner = ops[0] if len(ops) == 1 else None
        contested[k] = owner

    # gate evidence
    refused_by_cik, joined_by_cik, amb_by_cik = {}, {}, {}
    amb_count, refused_count, joined_count = {}, {}, {}
    for d in decisions:
        if d["rule"]:
            joined_count[d["rule"]] = joined_count.get(d["rule"], 0) + 1
            joined_by_cik.setdefault(d["a"], []).append(
                {"a": d["a"], "b": d["b"], "rule": d["rule"], "keys": d["keys"]})
            continue
        refused_count[d["reason"]] = refused_count.get(d["reason"], 0) + 1
        if ucb.find(cik_group[d["a"]]) == ucb.find(cik_group[d["b"]]):
            continue                       # joined through another pair after all
        for own, other in ((d["a"], d["b"]), (d["b"], d["a"])):
            refused_by_cik.setdefault(own, []).append(
                {"cik": own, "other_cik": other, "reason": d["reason"], "keys": d["keys"]})
        flag = gate.ambiguity(F(d["a"]), F(d["b"]), d["share_crd"], d["share_ein"],
                             d["crd_hint"])
        if flag:
            amb_count[flag] = amb_count.get(flag, 0) + 1
            for own, other in ((d["a"], d["b"]), (d["b"], d["a"])):
                amb_by_cik.setdefault(own, []).append(
                    {"cik": own, "other_cik": other, "flag": flag})

    # materialize
    materializable, singleton_keyed, piece_index = [], 0, {}
    for rks in sorted(pieces.values(), key=lambda m: m[0]):
        if len(rks) < 2:
            singleton_keyed += 1
            continue
        if len(rks) > COMPONENT_RECORD_CAP:
            refusals["component_over_cap"] += 1
            if len(refused_detail["oversize_components"]) < DETAIL_CAP:
                refused_detail["oversize_components"].append(
                    {"root": _rec_node(rks[0]), "records": len(rks),
                     "sample_members": rks[:10]})
            continue
        piece_index[piece_of_rec[rks[0]]] = len(materializable)
        materializable.append({"members": rks})

    def ident(p):
        if p is None:
            return None
        if p in piece_index:
            return ("comp", piece_index[p])
        return ("rec", pieces[p][0])

    multi_cik = 0
    for comp in materializable:
        rks = comp["members"]
        p = piece_of_rec[rks[0]]
        tier = "exact_key" if len({uf_a.find(_rec_node(rk)) for rk in rks}) == 1 else "crosswalk"
        all_keys = sorted({kv for rk in rks for kv in rec_keys[rk]})
        withheld = [k for k in all_keys if k in contested and contested[k] != p]
        keys = [k for k in all_keys if k not in set(withheld)]
        via = []
        if tier == "crosswalk":
            comp_ciks = {v for t, v in all_keys if t == "cik"}
            comp_crds = {v for t, v in all_keys if t == "crd"}
            via = [e for e in xwalk_used
                   if e["cik"] in comp_ciks and e["crd"] in comp_crds]
        cs = sorted(piece_ciks.get(p, ()))
        multi_cik += len(cs) > 1
        block = {}
        _cap_list(block, "joined", [e for c in cs for e in joined_by_cik.get(c, ())])
        _cap_list(block, "refused", [e for c in cs for e in refused_by_cik.get(c, ())])
        _cap_list(block, "ambiguous", [e for c in cs for e in amb_by_cik.get(c, ())])
        _cap_list(block, "attached", [e for e in attach_ev if piece_of_rec[e["record_key"]] == p])
        _cap_list(block, "contested", [
            {"type": k[0], "value": k[1],
             "owner": pieces[contested[k]][0] if contested[k] is not None else None}
            for k in all_keys if k in contested])
        if block:
            block["gate_version"] = gate.GATE_VERSION
        fs = [F(c) for c in cs] if (block or len(cs) > 1) else []
        lasts = [f["last"] for f in fs if f["last"]]
        # a heavy filer whose first filing is unknown (truncated recent list) is the oldest
        firsts = [f["first"].isoformat() if f["first"] else "0001-01-01"
                  for f in fs if f["first"] or f["first_floor"]]
        active = bool(data_asof and lasts and (data_asof - max(lasts)).days < gate.DORMANT_DAYS)
        comp.update({
            "root": _rec_node(rks[0]), "tier": tier, "keys": keys, "via": via,
            "withheld": withheld, "gate": block or None,
            # SPEC_150 main piece (assign_ids keeper): owns a CRD, operating,
            # active, oldest -- then the older rules (largest, smallest key)
            "main_rank": (0 if any(t == "crd" for t, _v in keys) else 1,
                          0 if (not fs or any(gate.operating(f) for f in fs)) else 1,
                          0 if active else 1,
                          min(firsts) if firsts else "9999-12-31"),
        })

    gated_singletons = sum(1 for p, rks in pieces.items() if len(rks) == 1
                           and any(c in refused_by_cik for c in piece_ciks.get(p, ())))
    contested_by_type, no_owner_by_type = {}, {}
    for k, owner in contested.items():
        contested_by_type[k[0]] = contested_by_type.get(k[0], 0) + 1
        if owner is None:
            no_owner_by_type[k[0]] = no_owner_by_type.get(k[0], 0) + 1
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
        "gate": {
            "gate_version": gate.GATE_VERSION,
            "profiles_supplied": len(profiles),
            "ciks_in_gated_pairs": len({c for d in decisions for c in (d["a"], d["b"])}),
            "pairs_considered": len(decisions),
            "pairs_joined": sum(joined_count.values()),
            "joined_by_rule": joined_count,
            "refused_by_reason": refused_count,
            "ambiguous_by_flag": amb_count,
            "anchor_attach": attach_how,
            "contested_keys": contested_by_type,
            "contested_no_owner": no_owner_by_type,
            "components_multi_cik": multi_cik,
            "gated_single_record_pieces": gated_singletons,
        },
    }
    return {"components": materializable, "record_keys": rec_keys,
            "refusals": refusals, "refused_detail": refused_detail,
            "metrics": metrics, "xwalk_used": xwalk_used,
            "key_owner": {k: ident(o) for k, o in contested.items()},
            "gate_refused_by_cik": refused_by_cik, "gate_decisions": decisions}


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


def canonical_row(members, by_key, withheld=()):
    """-> (columns dict, field_conflicts dict) for one component.

    AGREEMENT ONLY on every column except canonical_name. See the module
    docstring: two members asserting different values leaves the column NULL
    and writes both values into field_conflicts.

    SPEC_150: `withheld` are the (type, value) keys this component carries but
    another piece owns (a contested EIN / CRD): they never become a canonical
    column here, and are listed under `contested`.
    """
    cols, conflicts = {}, {}
    withheld = {tuple(k) for k in withheld or ()}
    for out_col, src_col, canon in _AGREEMENT_COLS:
        vals = sorted({v for v in (canon(by_key[rk].get(src_col))
                                   for rk in members)
                       if v and (out_col, v) not in withheld})
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
    if withheld:
        conflicts["contested"] = [{"type": t, "value": v} for t, v in sorted(withheld)]
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
    # SPEC_150: a component from plan() carries `main_rank` (owns a CRD,
    # operating, active, oldest), which decides first.
    keeper = {}
    for idx, ids in enumerate(claims):
        for eid in ids:
            cand = (tuple(comps[idx].get("main_rank") or ()), -len(comps[idx]["members"]),
                    comps[idx]["members"][0], idx)
            if keeper.get(eid) is None or cand < keeper[eid]:
                keeper[eid] = cand
    splits = []
    for idx, ids in enumerate(claims):
        claims[idx] = [eid for eid in ids if keeper[eid][-1] == idx]
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
# SPEC_154: GLEIF LEI records get their own name+state call (lei_conflict instead of ein_conflict)
WEAK_LEI_SOURCES = ("gleif",)
# records of a weak source are never candidates in any weak call: the dol5500 rows stay exactly
# what they were before GLEIF existed, and two weak sources never vouch for each other
WEAK_EXCLUDED_CANDIDATES = WEAK_SOURCES + WEAK_LEI_SOURCES
WEAK_TIER = "name_state"
# Distinct candidate identities (entities or lone records) kept per sponsor. A
# common name can match dozens; the rest are counted, never silently dropped.
WEAK_CANDIDATE_CAP = 5
# Sponsor-level summary: the best status a sponsor reached.
WEAK_STATUS_RANK = {"corroborates": 0, "conflict": 1, "ambiguous": 2, "candidate": 3}


def _source_of(rec):
    return rec.get("source") or rec["record_key"].split(":", 1)[0]


def weak_name_state(records, rec_keys, comps, sources=WEAK_SOURCES,
                    cap=WEAK_CANDIDATE_CAP, exclude=WEAK_EXCLUDED_CANDIDATES):
    """Name+state candidates for records of `sources`. PURE, and it changes no
    component: the resolver stays identifier-only (module docstring). Every row
    is evidence for a reviewer, with its contradictions written down.

    A candidate is a record of any OTHER source with the identical
    (name_norm, state). Its identity is its component (when it is in one) or
    itself. Status, per (sponsor, candidate):

      corroborates  the candidate sits in the sponsor's own strong component
      conflict      a contradiction with strong evidence, listed in `conflicts`:
                    ein_conflict (the candidate's identity carries EINs and none
                    is the sponsor's), lei_conflict (SPEC_154, the same for LEIs)
                    and/or strong_match_elsewhere (the sponsor is already strongly
                    matched to a different component)
      ambiguous     no conflict, but the name+state names >1 foreign identity
      candidate     exactly one foreign identity, no conflict

    Records of `sources` and of `exclude` (every weak source, SPEC_154) are
    never candidates.

    -> {"rows": [...], "metrics": {...}}; row["candidate_comp"] is an index
    into `comps` or None.
    """
    sources = set(sources)
    not_candidates = sources | set(exclude or ())
    comp_of = {rk: i for i, c in enumerate(comps) for rk in c["members"]}
    comp_eins = {i: sorted({v for t, v in c["keys"] if t == "ein"})
                 for i, c in enumerate(comps)}
    comp_leis = {i: sorted({v for t, v in c["keys"] if t == "lei"})
                 for i, c in enumerate(comps)}

    index = {}
    for rec in records:
        if _source_of(rec) in not_candidates:
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
        own_leis = sorted({v for t, v in rec_keys.get(rk, ()) if t == "lei"})
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
                cand_leis = (comp_leis[ci] if ci is not None else
                             sorted({v for t, v in rec_keys.get(c, ()) if t == "lei"}))
                if cand_leis and own_leis and not set(own_leis) & set(cand_leis):
                    conflicts.append({"reason": "lei_conflict", "record_lei": own_leis[0],
                                      "candidate_leis": cand_leis[:12]})
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


def source_metrics(records, rec_keys, comps, weak, new_components=(), source="dol5500",
                   key_type="ein"):
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
        if any(t == key_type for t, _v in rec_keys.get(rk, ())):
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
        _cols, conflicts = canonical_row(comps[ci]["members"], by_key, comps[ci].get("withheld"))
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
        f"with_{key_type}_key": with_ein,
        f"matched_strong_{key_type}": matched,
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
ATTACH_ONLY_SOURCES = ("pefirm", "industrial", "peportco", "portco", "threepl", "famoffice", "lpfund",
                       "atsboard")

# Independence: two records of ONE family are one source. ADV roster and IAPD
# are both Form ADV (Item 1.I, the adviser's own filing), so they never confirm
# each other. Every attach-only table is its own family. `own_site` is the
# probe finding the entity's legal name on the domain's own homepage.
SOURCE_FAMILY = {
    "adv": "form_adv", "iapd": "form_adv", "edgar": "edgar", "f13": "form_13f",
    "formd": "form_d", "dol5500": "form_5500",
    "pefirm": "pe_firm_web", "industrial": "industrial_seed", "peportco": "pe_portfolio",
    "portco": "portfolio_agentic", "threepl": "three_pl", "famoffice": "family_office",
    "lpfund": "lp_fund", "atsboard": "ats_board",      # SPEC_155: a verified job board's careers domain
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


def domain_links(records, rec_keys, comps, probes=None, cap=DOMAIN_SUBJECT_CAP, key_owner=None):
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

    # SPEC_150: a contested EIN / CRD belongs to its owner (plan()'s key_owner),
    # or to nobody -- never to whichever holder happens to come first
    contested = dict(key_owner or {})
    key_owner = {}
    for rk, kept in rec_keys.items():
        ident = ("comp", comp_of[rk]) if rk in comp_of else ("rec", rk)
        for k in kept:
            if k not in contested:
                key_owner.setdefault(k, ident)
    key_owner.update({k: o for k, o in contested.items() if o is not None})

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

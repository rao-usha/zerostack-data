"""
Ontology gold sets (SPEC_162, PLAN_100 §6).

A gold set is committed, and hashed, **before any model output exists**: the column map
(R1 grounding), the CQ probes and labels, and the gold SQL with fixtures (R2). The
manifest pins every file by sha256 plus one aggregate ``gold_sha256`` that runs and briefs
record (``onto.brief.gold_sha256``); a changed file changes the hash, and the test suite
fails until the manifest is rewritten on purpose.

    python -m app.ontology.gold --check            # exit 1 when MANIFEST.sha256 is stale
    python -m app.ontology.gold --write-manifest   # re-pin after a reviewed gold change

Hashes are taken over content with CRLF folded to LF, so a Windows checkout hashes the
same as Linux.

Note: ``app/ontology/__init__.py`` belongs to SPEC_164; until it lands ``app.ontology`` is a
namespace package and this sub-package works the same.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

GOLD_ROOT = Path(__file__).resolve().parent
DOMAINS = ("healthcare_provider",)
MANIFEST_NAME = "MANIFEST.sha256"
_NAME_RE = re.compile(r"^--\s*name:\s*(CQ\d{2})\s*$", re.M)


def domain_dir(domain: str = "healthcare_provider") -> Path:
    if domain not in DOMAINS:
        raise ValueError(f"unknown gold domain {domain!r}; known: {DOMAINS}")
    return GOLD_ROOT / domain


def _file_hash(path: Path) -> str:
    data = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def gold_files(domain: str = "healthcare_provider") -> List[Path]:
    root = domain_dir(domain)
    return sorted(p for p in root.rglob("*") if p.is_file() and p.name != MANIFEST_NAME
                  and "__pycache__" not in p.parts)


def compute_manifest(domain: str = "healthcare_provider") -> Dict[str, Any]:
    root = domain_dir(domain)
    files = {p.relative_to(root).as_posix(): _file_hash(p) for p in gold_files(domain)}
    agg = hashlib.sha256("\n".join(f"{k}:{v}" for k, v in sorted(files.items())).encode()).hexdigest()
    return {"files": files, "gold_sha256": agg}


def render_manifest(manifest: Dict[str, Any]) -> str:
    lines = [f"{h}  {name}" for name, h in sorted(manifest["files"].items())]
    lines.append(f"gold_sha256  {manifest['gold_sha256']}")
    return "\n".join(lines) + "\n"


def read_manifest(domain: str = "healthcare_provider") -> Dict[str, Any]:
    path = domain_dir(domain) / MANIFEST_NAME
    files: Dict[str, str] = {}
    agg: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        left, _, right = line.partition("  ")
        if left == "gold_sha256":
            agg = right.strip()
        else:
            files[right.strip()] = left.strip()
    return {"files": files, "gold_sha256": agg}


def verify(domain: str = "healthcare_provider") -> List[str]:
    """Differences between the committed manifest and the files on disk (empty = clean)."""
    want, have = read_manifest(domain), compute_manifest(domain)
    problems = []
    for name in sorted(set(want["files"]) | set(have["files"])):
        if name not in have["files"]:
            problems.append(f"missing file: {name}")
        elif name not in want["files"]:
            problems.append(f"unpinned file: {name}")
        elif want["files"][name] != have["files"][name]:
            problems.append(f"changed file: {name}")
    if want["gold_sha256"] != have["gold_sha256"]:
        problems.append(f"gold_sha256 {want['gold_sha256']} != {have['gold_sha256']}")
    return problems


def gold_sha256(domain: str = "healthcare_provider") -> str:
    return compute_manifest(domain)["gold_sha256"]


def load_sql_blocks(path: Path) -> Dict[str, str]:
    """``-- name: CQnn`` blocks of a .sql file -> SQL text (trailing ';' removed)."""
    parts = _NAME_RE.split(path.read_text(encoding="utf-8"))
    return {parts[i]: parts[i + 1].strip().rstrip(";").strip() for i in range(1, len(parts), 2)}


def load_gold_sql(domain: str = "healthcare_provider") -> Dict[str, str]:
    return load_sql_blocks(domain_dir(domain) / "cq_gold.sql")


def load_column_map(domain: str = "healthcare_provider") -> Dict[str, Any]:
    return json.loads((domain_dir(domain) / "column_map.json").read_text(encoding="utf-8"))


def load_labels(domain: str = "healthcare_provider") -> Dict[str, Any]:
    return json.loads((domain_dir(domain) / "cq_labels.json").read_text(encoding="utf-8"))


def load_expected(domain: str = "healthcare_provider") -> Dict[str, Any]:
    return json.loads((domain_dir(domain) / "cq_fixtures" / "expected.json").read_text(encoding="utf-8"))


def column_universe(domain: str = "healthcare_provider", include_schema_only: bool = False) -> List[str]:
    """``table.column`` of every in-universe column (R1(a) denominator, held-now)."""
    cmap = load_column_map(domain)
    out = []
    for table, spec in sorted(cmap["tables"].items()):
        if spec.get("schema_only") and not include_schema_only:
            continue
        out += [f"{table}.{c}" for c, v in sorted(spec["columns"].items()) if v.get("in_universe")]
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", default="healthcare_provider", choices=DOMAINS)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true")
    g.add_argument("--write-manifest", action="store_true")
    args = ap.parse_args(argv)
    if args.write_manifest:
        m = compute_manifest(args.domain)
        path = domain_dir(args.domain) / MANIFEST_NAME
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(render_manifest(m))
        print(f"wrote {path} (gold_sha256 {m['gold_sha256']})")
        return 0
    problems = verify(args.domain)
    for p in problems:
        print(p, file=sys.stderr)
    if not problems:
        print(f"{args.domain} gold is pinned ({gold_sha256(args.domain)})")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

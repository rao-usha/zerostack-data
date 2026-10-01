"""
Web domains as an identity carrier -- normalization (SPEC_148). PURE, stdlib only.

`classify(value)` turns whatever a source stored in a website column
('HTTP://WWW.Acme.COM/about', 'acme.co.uk', 'info@acme.com', 'linkedin.com/company/acme')
into the REGISTRABLE domain (eTLD+1) -- or None with the reason it was refused:

    empty      nothing there
    invalid    not a hostname (no dot, spaces, an IP address, a bare public suffix)
    generic:*  a host that names a platform, not the company: social networks,
               site builders and their tenant hosts, ATS / job boards, webmail,
               directories, parking pages, regulators, URL shorteners

Registrable domain = the public suffix + one label, from the Public Suffix List
(ICANN section, vendored at app/entities/data/public_suffix_list_icann.dat with
its MPL-2.0 header, VERSION / COMMIT pinned). The private section is left out on
purpose: it would make 'acme.wixsite.com' a registrable domain of its own, and a
tenant on a site builder is not a domain the company owns -- those hosts are
dropped as generic:builder instead. Refreshing the list is a file replace; the
version line is reported by `psl_version()`.

A domain is never a merge key (PE-TARGETS-DEPTH-PLAN phase 2: "never as a merge
key"). This module only decides what string a website column means.
"""

from __future__ import annotations

import ipaddress
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple

PSL_PATH = Path(__file__).resolve().parent / "data" / "public_suffix_list_icann.dat"

# Registrable domains (eTLD+1) that name a platform, never one company. Grouped
# so the drop is reported by category. Measured 2026-09-30 on ADV/IAPD websites:
# linkedin.com and facebook.com alone were 4,296 entity canonical_domain values.
GENERIC: Dict[str, Tuple[str, ...]] = {
    "social": (
        "linkedin.com", "lnkd.in", "facebook.com", "fb.com", "fb.me", "twitter.com", "x.com",
        "instagram.com", "youtube.com", "youtu.be", "tiktok.com", "pinterest.com",
        "medium.com", "substack.com", "podbean.com", "vimeo.com", "threads.net",
        "reddit.com", "tumblr.com", "flickr.com", "soundcloud.com", "spotify.com",
        "wikipedia.org", "github.com", "gitlab.com",
        # podcast / app-store pages: 16 ADV CRDs name apple.com (measured 2026-09-30)
        "podcasts.apple.com", "apps.apple.com", "itunes.apple.com", "music.amazon.com",
    ),
    "builder": (
        "wix.com", "wixsite.com", "editorx.io", "godaddy.com", "godaddysites.com",
        "secureserver.net", "squarespace.com", "weebly.com", "wordpress.com", "wordpress.org",
        "blogspot.com", "blogger.com", "business.site", "site123.me", "webflow.io",
        "carrd.co", "jimdo.com", "jimdosite.com", "strikingly.com", "mystrikingly.com",
        "yolasite.com", "square.site", "myshopify.com", "hubspotpagebuilder.com",
        "hs-sites.com", "wixstudio.io", "sites.google.com", "google.com", "googleusercontent.com",
        "netlify.app", "vercel.app", "herokuapp.com", "azurewebsites.net", "github.io",
        "advisorwebsites.com", "fmgsuite.com", "fmgwebsites.com", "twentyoverten.com",
    ),
    "ats": (
        "smartrecruiters.com", "greenhouse.io", "lever.co", "ashbyhq.com", "workable.com", "myworkdayjobs.com",
        "myworkdaysite.com", "workday.com", "icims.com", "jobvite.com", "bamboohr.com",
        "applytojob.com", "recruitee.com", "breezy.hr", "paylocity.com", "ultipro.com",
        "ukg.com", "taleo.net", "successfactors.com", "paycomonline.net", "dayforcehcm.com",
        "rippling.com", "rippling-ats.com", "jazzhr.com", "teamtailor.com", "personio.de",
        "pinpointhq.com", "indeed.com", "glassdoor.com", "ziprecruiter.com", "monster.com",
        "careerbuilder.com", "wellfound.com", "angel.co", "builtin.com", "hiringthing.com",
    ),
    "webmail": (
        "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
        "msn.com", "aol.com", "icloud.com", "me.com", "comcast.net", "att.net",
        "verizon.net", "protonmail.com", "proton.me", "mail.com", "gmx.com", "zoho.com",
    ),
    "directory": (
        "crunchbase.com", "pitchbook.com", "bloomberg.com", "zoominfo.com", "dnb.com",
        "yelp.com", "bbb.org", "manta.com", "yellowpages.com", "mapquest.com",
        "opencorporates.com", "bizapedia.com", "craft.co", "owler.com", "cbinsights.com",
        "linktr.ee", "about.me", "brokercheck.finra.org", "finra.org", "sec.gov",
        "adviserinfo.sec.gov", "investor.gov", "naic.org", "wsj.com", "reuters.com",
    ),
    "parking": (
        "example.com", "example.org", "example.net", "sedoparking.com", "parkingcrew.net",
        "hugedomains.com", "dan.com", "afternic.com", "godaddy.net", "bodis.com",
        "above.com", "domainmarket.com", "undeveloped.com", "sedo.com",
    ),
    "shortener": (
        "bit.ly", "goo.gl", "tinyurl.com", "ow.ly", "t.co", "buff.ly", "rebrand.ly",
        "lnk.to", "tiny.cc", "is.gd",
    ),
}
_GENERIC_INDEX = {d: cat for cat, doms in GENERIC.items() for d in doms}

_SCHEME = re.compile(r"^[a-z][a-z0-9+.\-]*://")
_LABEL = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_PLACEHOLDER = {"none", "n/a", "na", "null", "tbd", "nil", "-", "unknown", "no website"}


@lru_cache(maxsize=1)
def _rules():
    normal, wild, exc = set(), set(), set()
    version = None
    for line in PSL_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("// VERSION:"):
            version = line.split(":", 1)[1].strip()
        if not line or line.startswith("//"):
            continue
        rule = line.split()[0].lower()
        rule = rule.encode("idna").decode("ascii") if not rule.isascii() else rule
        if rule.startswith("!"):
            exc.add(rule[1:])
        elif rule.startswith("*."):
            wild.add(rule[2:])
        else:
            normal.add(rule)
    return normal, wild, exc, version


def psl_version() -> Optional[str]:
    return _rules()[3]


def public_suffix(host: str) -> str:
    """The public suffix of a lowercase ASCII host (PSL algorithm, '*' default)."""
    normal, wild, exc, _v = _rules()
    labels = host.split(".")
    for i in range(len(labels)):
        cand = ".".join(labels[i:])
        if cand in exc:                      # exception: the suffix is one label shorter
            return ".".join(labels[i + 1:])
        if cand in normal:
            return cand
        if i + 1 < len(labels) and ".".join(labels[i + 1:]) in wild:
            return cand                      # '*.ck' matches 'foo.ck'
    return labels[-1]


def host_of(value) -> Optional[str]:
    """Lowercase ASCII hostname from a URL, bare host or e-mail address, or None."""
    s = str(value or "").strip().strip("\"'<>").casefold()
    if not s or s in _PLACEHOLDER:
        return None
    s = _SCHEME.sub("", s)
    s = re.split(r"[/?#\\]", s, maxsplit=1)[0]
    s = s.rsplit("@", 1)[-1]                 # userinfo or an e-mail address
    if s.startswith("[") or s.count(":") > 1:
        return None                          # IPv6 literal
    s = s.split(":", 1)[0].strip(".")
    if not s or " " in s or "." not in s:
        return None
    if not s.isascii():
        try:
            s = s.encode("idna").decode("ascii")
        except UnicodeError:
            return None
    return s


def registrable(host: Optional[str]) -> Optional[str]:
    """eTLD+1 of a host, or None for an IP, an invalid host or a bare suffix."""
    if not host:
        return None
    try:
        ipaddress.ip_address(host)
        return None
    except ValueError:
        pass
    labels = host.split(".")
    if not all(_LABEL.match(lab) for lab in labels):
        return None
    if labels[-1].isdigit():
        return None
    suffix = public_suffix(host)
    n = suffix.count(".") + 1
    if len(labels) <= n:
        return None                          # the host IS a public suffix
    return ".".join(labels[-(n + 1):])


def classify(value) -> Tuple[Optional[str], str]:
    """-> (registrable domain, 'ok') or (None, reason)."""
    if value is None or not str(value).strip():
        return None, "empty"
    host = host_of(value)
    reg = registrable(host)
    if not reg:
        return None, "invalid"
    for cand in (host, reg):
        cat = _GENERIC_INDEX.get(cand)
        if cat:
            return None, f"generic:{cat}"
    # a subdomain of a generic platform host ('acme.wixsite.com', 'boards.greenhouse.io')
    parts = host.split(".")
    for i in range(1, len(parts) - 1):
        cat = _GENERIC_INDEX.get(".".join(parts[i:]))
        if cat:
            return None, f"generic:{cat}"
    return reg, "ok"


def domain(value) -> Optional[str]:
    return classify(value)[0]

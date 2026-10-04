"""
Rights sign-offs that are in force (SPEC_142, PLAN_088 decision 8).

**Code truth plus a committed hash.** A dataset's ``reviewed`` is True only
when ``REVIEWED[key]`` holds the ``rights_hash`` of its *current* rights block
(``DatasetSpec.rights_hash``: licence, licence URL, redistribution,
attribution, storage, commercial use, share-alike, PII class, origin). Change
any of those in ``rights.py`` or ``datasets.py`` and the hash no longer
matches, so the dataset silently drops back to unreviewed and re-enters the
review queue.

The ``catalog_rights_review`` table is the audit trail (who decided what,
when, against which hash); it never flips anything by itself. To bring a
sign-off into force a human runs::

    python -m app.catalog.rights_review --emit --out app/catalog/rights_reviewed.py

reviews the diff (it lists only ``confirm_current`` decisions whose hash still
matches the code) and commits it. Nothing in the application writes this file.

Format: ``{dataset_key: (rights_hash, review_id)}``.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, Tuple

REVIEWED: Dict[str, Tuple[str, int]] = {
    'bea_gdp_industry': ('75b77724b2eb2d285b8d081738787acda8827abc6984cfe5b095f26bd1167272', 9),
    'bea_international': ('75b77724b2eb2d285b8d081738787acda8827abc6984cfe5b095f26bd1167272', 10),
    'bea_nipa': ('75b77724b2eb2d285b8d081738787acda8827abc6984cfe5b095f26bd1167272', 11),
    'bea_regional': ('75b77724b2eb2d285b8d081738787acda8827abc6984cfe5b095f26bd1167272', 12),
    'bls_series': ('b6b34e7f5cab4436e3421a6f4b753b3aea5b18781e01847dea667a446a532bc9', 8),
    'bts_border_crossing': ('79de810a070c6278d3d7cd8087866f340f6593f67d87415c2e09b47044a153b0', 13),
    'bts_faf_regional': ('2308dc3d93a27b52bd8eb139622cade220de79793b71daec4adee3fce7c8dee1', 14),
    'census_acs5': ('c2ce7fc3116463f730d85f6ebc3772f0c7149e7d363f938290095cc3e79259a6', 15),
    'census_acs_county_tract': ('c2ce7fc3116463f730d85f6ebc3772f0c7149e7d363f938290095cc3e79259a6', 16),
    'census_cbp': ('c2ce7fc3116463f730d85f6ebc3772f0c7149e7d363f938290095cc3e79259a6', 17),
    'census_cbp_county_yearly': ('c2ce7fc3116463f730d85f6ebc3772f0c7149e7d363f938290095cc3e79259a6', 18),
    'cftc_cot': ('9b51da046d15d8126bef80c09fd59767121b4fa11610a375afa44ffb58182954', 19),
    'eia_electricity': ('aeb82dfe22eed886000b7ab91e35c61187db5e794f771494f8b45393b8eb5072', 20),
    'eia_natural_gas': ('aeb82dfe22eed886000b7ab91e35c61187db5e794f771494f8b45393b8eb5072', 21),
    'eia_petroleum': ('1e1d9b0d05225e03f3ccbcbe38606dc8ea94098a3b8e128a8fa9b9c85b4daa7e', 22),
    'eia_retail_gas_prices': ('aeb82dfe22eed886000b7ab91e35c61187db5e794f771494f8b45393b8eb5072', 23),
    'eia_steo': ('6fc89b0ac63e962f32e27d691c6ab957322d0d4a905679a449a6d1bdf4a67213', 24),
    'epa_echo_facilities': ('0aba1558ff4239712b87bc9cc90374217f039ba7c308fb041c9103556581d7fb', 25),
    'fcc_broadband': ('b8dadc5968cd531c3188dbbcd41d6112633b46cb31267954429bec25d25f8689', 26),
    'irs_soi': ('4b4248c47d94ed98da02fd20e402a97fef61104079fb12c8132046e176e8d366', 27),
    'osha': ('17064a8ee9b75e1e256499a4880255382e8b41e269efdb0b9263e1f8d2e0cb79', 28),
    'sec_13f': ('b00bc581832f55c096481d419aa161f4fbead2bf39b0cdfc2677843e7f83aa86', 1),
    'sec_companyfacts': ('329cca3754ad15bccf91f7402f003a7666aad182089bbd6442f98ee4b3a63671', 2),
    'treasury_auctions': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 7),
    'treasury_daily_balance': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 3),
    'treasury_debt_outstanding': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 4),
    'treasury_interest_rates': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 5),
    'treasury_monthly_statement': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 6),
    'us_trade_exports_hs': ('b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f', 29),
    'us_trade_exports_state': ('b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f', 30),
    'us_trade_imports_hs': ('b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f', 31),
    'us_trade_port_trade': ('b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f', 32),
    'us_trade_summary': ('b5cd10667dbfdacf249b1e9eee1e966dbd0ef61b9fbad884a6d580ab61745a4f', 33),
    'usaspending_awards': ('34992fdc69062666c908887cc232f0440be2de613d6596df524bc3a4cbc08e04', 34),
    'usda_nass': ('25b495336903c2c98eecdda217a32faf2738f4f63c012e02d2cfcf20c5ec9475', 35),
}


def apply_review(spec):
    """``spec`` with ``reviewed`` set from the committed sign-offs (hash must match)."""
    entry = REVIEWED.get(spec.key)
    reviewed = bool(entry) and entry[0] == spec.rights_hash
    return spec if spec.reviewed == reviewed else replace(spec, reviewed=reviewed)

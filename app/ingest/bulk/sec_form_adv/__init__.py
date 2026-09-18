"""SEC Form ADV bulk sources (SPEC_112): monthly RIA/ERA roster + IAPD compilation feed."""

from app.ingest.bulk.sec_form_adv import iapd_feed, roster  # noqa: F401  (registers sec_adv_roster, sec_iapd_feed)

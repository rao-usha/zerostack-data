"""
The old job-postings run is retired (SPEC_152, owner decision 2026-10-02).

``job_postings:all`` fetched Ashby boards although ``api.ashbyhq.com/robots.txt`` answers 401
(NexData's ``app.core.open_web`` treats 401/403 as disallow-all), and its ATS detector requested
company websites with no robots.txt or terms check. Its network paths now refuse before any
request. Stored rows (``job_postings``, ``job_posting_snapshots``, ``company_ats_config``,
``job_posting_alerts``) stay readable; nothing is deleted.

Use the gated lane instead: ``app.sources.ats_boards`` (SPEC_151), dispatch key ``ats_boards``,
``python -m app.sources.ats_boards.collect --preset migrated``.
"""

RETIRED_ON = "2026-10-02"
POINTER = ("use the gated lane app.sources.ats_boards (SPEC_151; dispatch key 'ats_boards'; "
           "python -m app.sources.ats_boards.collect --preset migrated)")

OLD_RUN_RETIRED = (f"job_postings old run retired {RETIRED_ON} (SPEC_152): its fetches had no robots.txt or "
                   f"terms gate; {POINTER}")
ASHBY_BLOCKED = (f"Ashby fetch refused (SPEC_152): api.ashbyhq.com/robots.txt answers 401 and NexData treats "
                 f"that as disallow-all; owner decision {RETIRED_ON}: no override; {POINTER}")
DETECTOR_RETIRED = (f"ATS website detection refused (SPEC_152): it requested company websites with no robots.txt "
                    f"or terms check; {POINTER}")


class OldLaneRetired(RuntimeError):
    """Raised by every network path of the retired job-postings run."""

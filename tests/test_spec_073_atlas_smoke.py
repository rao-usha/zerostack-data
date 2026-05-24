"""
Tests for SPEC 073 — Atlas headless-Chrome smoke harness.

Wraps `scripts/smoke_atlas.py` as a single heavy test. Skipped by
default because it spawns headless Chrome and takes ~3-5 minutes;
opt in with `ATLAS_SMOKE=1` (in CI or pre-deploy).

    ATLAS_SMOKE=1 pytest tests/test_spec_073_atlas_smoke.py -v
"""
import os
import subprocess
import sys
import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("ATLAS_SMOKE") != "1",
    reason="ATLAS_SMOKE=1 not set — set it to run the headless harness "
           "(takes ~3-5 minutes, spawns Chrome)",
)


def test_atlas_smoke_harness_passes_all_scenarios():
    """The harness exits 0 iff every layer/dynamic/place scenario passes."""
    script = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "smoke_atlas.py",
    )
    proc = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, timeout=600,
    )
    print(proc.stdout)
    if proc.stderr:
        print("STDERR:", proc.stderr, file=sys.stderr)
    assert proc.returncode == 0, (
        f"Atlas smoke harness reported failures — see report at "
        f"docs/atlas_smoke_report.csv\n\n{proc.stdout[-2000:]}"
    )

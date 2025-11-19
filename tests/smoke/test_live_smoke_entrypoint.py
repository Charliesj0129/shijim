from __future__ import annotations

import os

import pytest

from shijim.tools import live_smoke_test


@pytest.mark.skipif(os.getenv("SHIJIM_LIVE_SMOKE") != "1", reason="live smoke test disabled")
def test_live_smoke_entrypoint_runs():
    assert live_smoke_test.run_smoke_test(duration_seconds=int(os.getenv("SHIJIM_SMOKE_TEST_DURATION", "10"))) == 0

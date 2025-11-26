import json
from unittest.mock import MagicMock, patch

import pytest

from shijim.tools.hft_converter import load_events, main


@pytest.fixture
def mock_save_events():
    with patch("shijim.tools.hft_converter.save_events_as_npz") as mock:
        yield mock

@pytest.fixture
def mock_process_pool():
    with patch("shijim.tools.hft_converter.ProcessPoolExecutor") as mock:
        yield mock

def test_load_events(tmp_path):
    f = tmp_path / "test.jsonl"
    data = {
        "event_id": 1, "exch_ts": 100, "local_ts": 101,
        "side": 1, "price": 100.0, "quantity": 10.0
    }
    f.write_text(json.dumps(data) + "\n", encoding="utf-8")

    events = load_events(f)
    assert len(events) == 1
    assert events[0].event_id == 1

def test_main_single_file(tmp_path, mock_save_events):
    inp = tmp_path / "input.jsonl"
    inp.write_text(
        '{"event_id": 1, "exch_ts": 100, "local_ts": 101, "side": 1, '
        '"price": 100.0, "quantity": 10.0}\n',
        encoding="utf-8",
    )
    out = tmp_path / "output.npz"

    main([str(inp), str(out)])

    mock_save_events.assert_called_once()
    args, _ = mock_save_events.call_args
    assert len(args[0]) == 1 # 1 event

def test_main_directory_parallel(tmp_path, mock_save_events, mock_process_pool):
    inp_dir = tmp_path / "input"
    inp_dir.mkdir()
    (inp_dir / "1.jsonl").write_text('{}\n', encoding="utf-8")
    (inp_dir / "2.jsonl").write_text('{}\n', encoding="utf-8")

    out_dir = tmp_path / "output"

    # Mock executor
    executor = MagicMock()
    mock_process_pool.return_value.__enter__.return_value = executor

    main([str(inp_dir), str(out_dir), "--workers", "2"])

    # Check if executor was used
    mock_process_pool.assert_called_once_with(max_workers=2)
    assert executor.submit.call_count == 2

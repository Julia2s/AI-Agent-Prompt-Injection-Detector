import json
import logging
import sys
from unittest.mock import Mock

from mlops import pipeline


def test_pipeline_cli_logs_completion_without_handoff(monkeypatch, capsys, tmp_path):
    root = logging.getLogger()
    saved_root = root.handlers[:], root.level
    framework_loggers = [logging.getLogger(name) for name in (
        "uvicorn", "uvicorn.error", "uvicorn.access", "faststream"
    )]
    saved_frameworks = [(item, item.handlers[:], item.level, item.propagate) for item in framework_loggers]
    monkeypatch.setattr(sys, "argv", ["pipeline", "--output", str(tmp_path)])
    run = Mock(return_value={"tracking_uri": "private-value"})
    monkeypatch.setattr(pipeline, "run", run)
    try:
        pipeline.main()
        output = capsys.readouterr().out
        entry = json.loads(output)
        assert entry["service"] == "data-pipeline"
        assert entry["message"] == "Data pipeline completed"
        assert "private-value" not in output
        run.assert_called_once()
    finally:
        root.handlers[:], root.level = saved_root
        for item, handlers, level, propagate in saved_frameworks:
            item.handlers[:], item.level, item.propagate = handlers, level, propagate

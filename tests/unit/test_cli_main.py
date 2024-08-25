import pytest
from unittest.mock import patch
import argparse
import logging
import os
import sys
import nightcrawler.cli.main as main_module


@pytest.fixture(autouse=True)
def mock_modules():
    with patch("nightcrawler.cli.main.extractor") as mock_extractor, patch(
        "nightcrawler.cli.main.processor"
    ) as mock_processor, patch("nightcrawler.cli.main.fullrun") as mock_fullrun:
        yield {
            "extractor": mock_extractor,
            "processor": mock_processor,
            "fullrun": mock_fullrun,
        }


def test_config_logs(tmpdir):
    log_file = tmpdir.join("test.log")
    args = argparse.Namespace(log_level="INFO", log_file=str(log_file))
    main_module.config_logs(args)
    assert os.path.exists(log_file)
    assert logging.getLogger(main_module.LOGGER_NAME).level == logging.INFO


def test_parse_args():
    args_list = ["extract", "keyword", "--log-level", "DEBUG"]
    with patch("nightcrawler.cli.version.__version__", "1.0.0"):
        args = main_module.parse_args(args_list)
    assert args.module == "extract"
    assert args.keyword == "keyword"
    assert args.log_level == "DEBUG"


def test_run(mock_modules):
    args = argparse.Namespace(
        module="extract", keyword="keyword", log_level="DEBUG", log_file=None
    )
    with patch.object(
        main_module, "parse_args", return_value=args
    ) as mock_parse_args, patch.object(
        main_module, "config_logs"
    ) as mock_config_logs, patch.object(main_module, "apply") as mock_apply:
        main_module.run(["extract", "keyword", "--log-level", "DEBUG"])
        mock_parse_args.assert_called_once_with(
            ["extract", "keyword", "--log-level", "DEBUG"]
        )
        mock_config_logs.assert_called_once()
        mock_apply.assert_called_once()


def test_main():
    with patch.object(sys, "argv", ["main.py", "extract", "keyword"]), patch.object(
        main_module, "run"
    ) as mock_run:
        main_module.main()
        mock_run.assert_called_once_with(["extract", "keyword"])

from unittest.mock import patch
import argparse
import logging
import os
import sys
import nightcrawler.cli.main as main_module

def test_log_level_info(tmpdir):
    log_file = tmpdir.join("test.log")
    args = argparse.Namespace(log_level="INFO", log_file=str(log_file))
    main_module.config_logs(args)
    assert os.path.exists(log_file)
    assert logging.getLogger(main_module.LOGGER_NAME).level == logging.INFO


def test_parse_args_accepts_extract_keyword_debug():
    args_list = ["extract", "keyword", "--log-level", "DEBUG"]
    with patch("nightcrawler.cli.version.__version__", "1.0.0"):
        args = main_module.parse_args(args_list)
    assert args.module == "extract"
    assert args.keyword == "keyword"
    assert args.log_level == "DEBUG"


def test_run_with_arguments():
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


def test_main_if_extract_executed_with_keyword():
    with patch.object(sys, "argv", ["main.py", "extract", "keyword"]), patch.object(
        main_module, "run"
    ) as mock_run:
        main_module.main()
        mock_run.assert_called_once_with(["extract", "keyword"])

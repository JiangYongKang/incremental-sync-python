import logging

import pytest


@pytest.fixture(autouse=True)
def _log_to_console(caplog):
    """单测日志中打印位点与变更流水。"""
    logging.getLogger("sync.applier").setLevel(logging.INFO)
    with caplog.at_level(logging.INFO, logger="sync.applier"):
        yield

"""测试公共配置：引擎流水日志（位点 + 变更流水）实时打印到控制台。

运行 ``uv run pytest -s`` 可看到每条变更的 seq、操作、主键、数据以及
批次提交位点；即使不加 -s，日志也会被 caplog 捕获，可在断言中检查。
"""

from __future__ import annotations

import logging
import sys

import pytest

ENGINE_LOGGER = logging.getLogger("sync.engine")


@pytest.fixture(autouse=True)
def _stream_engine_logs(caplog):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))
    ENGINE_LOGGER.addHandler(handler)
    with caplog.at_level(logging.INFO, logger="sync.engine"):
        yield
    ENGINE_LOGGER.removeHandler(handler)

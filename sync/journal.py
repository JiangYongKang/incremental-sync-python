"""原子提交日志：把数据、位点、结构、暂存、死信绑成一个原子落盘单元。

崩溃恢复模型：
- 每个同步边界（事务整体生效 / 事务整体拒绝 / 隐式单条应用）由 persist()
  一次性写入一份**完整状态快照**，落盘采用 tmp 文件 + os.replace，对外原子。
- 快照内容包含：目标端数据、已确认位点、表结构、键高水位、事务暂存区、
  死信列表。因此数据、位点、结构、被拒记录“一起生效或一起不生效”。
- 进程中途被杀或写盘报错：tmp 残留不会被读取，旧快照保持完好；
  recover() 返回最后一份完整快照，由应用器重放恢复。

并发可见性（重要）：快照在目标端状态**整库发布之前**就已写完——应用器先把
整批决议结果（全部新行/位点/死信）序列化进快照并原子落盘，成功后才对内存
目标端做一次不可变快照替换。因此在“写盘进行中 / 已写盘尚未发布”的窗口里，
并发读者读到的始终是上一份完整状态，落盘与可见性之间不存在半个事务。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("sync.applier")


class CommitLogger:
    def __init__(self, path: Path, snapshot: Callable[[], dict[str, Any]],
                 fail_after_bytes: int | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._snapshot = snapshot
        self._lock = threading.RLock()
        # 测试钩子：写入超过该字节数后模拟写盘失败（如磁盘错误）
        self._fail_after_bytes = fail_after_bytes

    def persist(self) -> None:
        """原子写入当前快照；中途失败保证旧文件完好。"""
        with self._lock:
            payload = json.dumps(self._snapshot(), ensure_ascii=False).encode("utf-8")
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            try:
                with tmp.open("wb") as f:
                    if self._fail_after_bytes is not None:
                        f.write(payload[:self._fail_after_bytes])
                        f.flush()
                        os.fsync(f.fileno())
                        raise OSError("模拟写盘失败（注入）")
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
                logger.info("journal persisted bytes=%d", len(payload))
            except BaseException:
                tmp.unlink(missing_ok=True)
                raise

    def recover(self) -> dict[str, Any] | None:
        """返回最后一份完整快照；无日志或仅有残缺 tmp 时返回 None。"""
        with self._lock:
            if not self.path.exists():
                return None
            # 崩溃残留的 tmp 一律忽略：它从不构成已确认状态
            self.path.with_suffix(self.path.suffix + ".tmp").unlink(missing_ok=True)
            return json.loads(self.path.read_text(encoding="utf-8"))

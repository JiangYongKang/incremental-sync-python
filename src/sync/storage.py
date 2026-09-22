"""基于本地文件的目标端存储（模拟真实数据库）。

状态保存在单个 JSON 文件中，所有提交通过"临时文件 + os.replace"原子落盘，
保证进程在任意时刻崩溃后重启，状态要么是提交前、要么是提交后，不存在中间态。

存储层不做业务校验（schema、位点规则由上层模块负责），只提供行级
CRUD、死信追加与快照替换，所有对状态的修改只允许通过 :meth:`LocalStore.commit`
生成一个新的原子快照。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from sync.models import RejectedRecord


class LocalStore:
    """线程安全的本地 JSON 存储。"""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._state: dict[str, Any] = self._load()

    # ------------------------------------------------------------------ 持久化
    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as f:
                return json.load(f)
        # tables：目标端行数据；dead_letters：被拒绝记录；
        # schemas：各表已注册的结构版本；checkpoint：已确认位点。
        # 四者放在同一个状态对象中，使"数据 + 位点 + 结构"能在单个原子快照内提交。
        return {
            "tables": {},
            "dead_letters": [],
            "schemas": {},
            "checkpoint": 0,
        }

    def _persist_locked(self) -> None:
        """原子写盘：先写临时文件再 rename，调用方须持有锁。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def reload(self) -> None:
        """从磁盘重新加载状态（模拟新进程启动）。"""
        with self._lock:
            self._state = self._load()

    def get_checkpoint(self) -> int:
        """读取已确认位点（轻量，不拷贝整份状态）。"""
        with self._lock:
            return int(self._state.get("checkpoint", 0))

    # ------------------------------------------------------------------ 行数据
    def _bucket(self, table: str) -> dict[str, dict[str, Any]]:
        return self._state["tables"].setdefault(table, {})

    @staticmethod
    def encode_key(key: dict[str, Any]) -> str:
        """把主键值编码为稳定的存储键。"""
        return json.dumps(key, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def get_row(self, table: str, key: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            return dict(self._bucket(table).get(self.encode_key(key), {})) or None

    def put_row(self, table: str, key: dict[str, Any], row: dict[str, Any]) -> None:
        with self._lock:
            self._bucket(table)[self.encode_key(key)] = dict(row)

    def delete_row(self, table: str, key: dict[str, Any]) -> bool:
        with self._lock:
            return self._bucket(table).pop(self.encode_key(key), None) is not None

    def all_rows(self, table: str) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._bucket(table).values()]

    def row_count(self, table: str) -> int:
        with self._lock:
            return len(self._bucket(table))

    def table_names(self) -> list[str]:
        with self._lock:
            return sorted(self._state["tables"].keys())

    # ------------------------------------------------------------------ 死信
    def append_dead_letters_locked(self, records: list[RejectedRecord]) -> None:
        """追加死信。必须在 :meth:`commit` 回调内调用（与位点同事务）。"""
        self._state["dead_letters"].extend(r.to_dict() for r in records)

    def dead_letters(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(d) for d in self._state["dead_letters"]]

    # ------------------------------------------------------------------ 快照提交
    def commit(self, mutate) -> None:
        """在锁内执行一次状态变更并原子落盘。

        ``mutate`` 是一个接收内部 state 的回调，回调抛出异常则整个提交中止、
        磁盘状态保持不变。
        """
        with self._lock:
            import copy

            backup = copy.deepcopy(self._state)
            try:
                mutate(self._state)
                self._persist_locked()
            except BaseException:
                self._state = backup
                raise

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def snapshot(self) -> dict[str, Any]:
        """返回状态的深拷贝快照（测试/演示用）。"""
        import copy

        with self._lock:
            return copy.deepcopy(self._state)

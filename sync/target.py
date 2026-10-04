"""目标端模拟：本地 JSON 文件存储的行集合。"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Callable, Iterable


class LocalTarget:
    """每张表是 {pk: row} 的字典，持久化到本地 JSON 文件。

    pk 在内存中保持原类型，序列化时统一转为 str（JSON 对象键要求），
    读取时通过 get/rows 返回的键均为 str 形式；调用方负责规范化比较。

    原子可见性：apply_atomic() 把一组跨表行变更在同一把锁内一次性生效，
    read_snapshot()/all_tables() 在同一把锁内一次性读取——任何时刻读者
    看到的都是若干次完整 apply_atomic 的组合，绝不会看到半组变更。
    """

    def __init__(self, path: Path, op_hook: Callable[[], None] | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        # 测试钩子：行变更之间的让出点（模拟并发交错），生产为 None
        self._op_hook = op_hook
        if self.path.exists():
            self._data: dict[str, dict[str, dict[str, Any]]] = json.loads(
                self.path.read_text(encoding="utf-8") or "{}"
            )
        else:
            self._data = {}
            self.flush()

    @staticmethod
    def _key(pk: Any) -> str:
        return str(pk)

    def get(self, table: str, pk: Any) -> dict[str, Any] | None:
        with self._lock:
            row = self._data.get(table, {}).get(self._key(pk))
            return dict(row) if row is not None else None

    def upsert(self, table: str, pk: Any, row: dict[str, Any]) -> None:
        with self._lock:
            self._data.setdefault(table, {})[self._key(pk)] = dict(row)
        if self._op_hook is not None:
            self._op_hook()  # 锁外让出：逐行写路径的并发交错暴露点

    def delete(self, table: str, pk: Any) -> None:
        with self._lock:
            self._data.get(table, {}).pop(self._key(pk), None)
        if self._op_hook is not None:
            self._op_hook()

    def rows(self, table: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._data.get(table, {}).items()}

    def apply_atomic(self, ops: Iterable[tuple[str, str, Any, dict[str, Any] | None]]) -> None:
        """把一组行变更（可跨表）在同一把锁内一次性生效。

        ops 元素为 ("upsert", table, pk, row) 或 ("delete", table, pk, None)。
        整组生效期间锁不释放，读者不可能看到只应用了一部分 ops 的中间态。
        """
        with self._lock:
            for op in ops:
                kind, table, pk, row = op
                if kind == "upsert":
                    assert row is not None
                    self._data.setdefault(table, {})[self._key(pk)] = dict(row)
                else:
                    self._data.get(table, {}).pop(self._key(pk), None)
                if self._op_hook is not None:
                    self._op_hook()

    def read_snapshot(self, tables: Iterable[str] | None = None
                      ) -> dict[str, dict[str, dict[str, Any]]]:
        """在同一把锁内读取多张表的一致快照（tables 为 None 时读全部表）。

        多次单表 rows() 之间可能插入其他事务的提交；需要跨表一致视图时
        必须用本方法一次性读取。
        """
        with self._lock:
            names = list(tables) if tables is not None else list(self._data)
            return {t: {k: dict(v) for k, v in self._data.get(t, {}).items()}
                    for t in names}

    def all_tables(self) -> dict[str, dict[str, dict[str, Any]]]:
        """全部表的深拷贝（原子快照使用）。"""
        return self.read_snapshot()

    def replace_all(self, data: dict[str, dict[str, dict[str, Any]]]) -> None:
        """以权威快照整体替换内存数据（崩溃恢复使用）。"""
        with self._lock:
            self._data = {t: {k: dict(v) for k, v in rows.items()}
                          for t, rows in data.items()}

    def flush(self) -> None:
        with self._lock:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.path)

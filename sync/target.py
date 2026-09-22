"""目标端模拟：本地 JSON 文件存储的行集合。"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any


class LocalTarget:
    """每张表是 {pk: row} 的字典，持久化到本地 JSON 文件。

    pk 在内存中保持原类型，序列化时统一转为 str（JSON 对象键要求），
    读取时通过 get/rows 返回的键均为 str 形式；调用方负责规范化比较。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
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

    def delete(self, table: str, pk: Any) -> None:
        with self._lock:
            self._data.get(table, {}).pop(self._key(pk), None)

    def rows(self, table: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._data.get(table, {}).items()}

    def flush(self) -> None:
        with self._lock:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.path)

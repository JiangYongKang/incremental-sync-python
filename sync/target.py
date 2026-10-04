"""目标端模拟：本地 JSON 文件存储的行集合。

并发可见性模型（整组原子可见）：
- 已发布数据 ``_data`` 一经发布即视为**不可变**；任何修改都先在私有深拷贝上
  完成，再通过一次引用替换原子发布（``replace_all``）。发布之间不允许就地
  变更，因此读路径永远不会抓到“改了一半”的字典。
- 单次 ``view()`` / ``all_tables()`` 拿到的是某一时刻的**整库一致快照**：
  一个跨表事务要么所有表的行都在快照里，要么都不在；多次决议批量发布时，
  快照内容只可能是若干个**完整事务**的组合。
- ``generation`` 在每次发布时 +1，调用方可据此断言两次读取是否属于同一版本。
"""
from __future__ import annotations

import json
import os
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
        self._generation = 0
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

    @staticmethod
    def clone(data: dict[str, dict[str, dict[str, Any]]]
              ) -> dict[str, dict[str, dict[str, Any]]]:
        """整库深拷贝（行 dict 也复制，保证发布后副本与快照互不影响）。"""
        return {t: {k: dict(v) for k, v in rows.items()}
                for t, rows in data.items()}

    def get(self, table: str, pk: Any) -> dict[str, Any] | None:
        with self._lock:
            row = self._data.get(table, {}).get(self._key(pk))
            return dict(row) if row is not None else None

    def upsert(self, table: str, pk: Any, row: dict[str, Any]) -> None:
        """旧接口：整库拷贝后单次原子发布（该次写入整体可见）。

        新代码不应在一个事务内多次调用本方法（多次发布之间读者可能读到
        中间版本）；事务级写入请在私有副本上修改后调用 replace_all 一次发布。
        """
        with self._lock:
            data = self.clone(self._data)
            data.setdefault(table, {})[self._key(pk)] = dict(row)
            self._data = data
            self._generation += 1

    def delete(self, table: str, pk: Any) -> None:
        """旧接口：整库拷贝后单次原子发布。"""
        with self._lock:
            data = self.clone(self._data)
            data.get(table, {}).pop(self._key(pk), None)
            self._data = data
            self._generation += 1

    def rows(self, table: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._data.get(table, {}).items()}

    def all_tables(self) -> dict[str, dict[str, dict[str, Any]]]:
        """全部表的深拷贝——跨表一致的整库快照（原子快照/读取使用）。"""
        with self._lock:
            return self.clone(self._data)

    def view(self) -> tuple[int, dict[str, dict[str, dict[str, Any]]]]:
        """返回 (generation, 整库一致快照)。

        同一 generation 内多次读取必然相同；读者可凭 generation 识别版本。
        """
        with self._lock:
            return self._generation, self.clone(self._data)

    def generation(self) -> int:
        with self._lock:
            return self._generation

    def replace_all(self, data: dict[str, dict[str, dict[str, Any]]]) -> None:
        """以权威快照整体替换内存数据并原子发布（版本号 +1）。"""
        with self._lock:
            self._data = self.clone(data)
            self._generation += 1

    def flush(self) -> None:
        with self._lock:
            payload = json.dumps(self._data, ensure_ascii=False, indent=2)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

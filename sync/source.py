"""上游数据源模拟：本地 JSONL 变更日志，位点单调递增。"""
from __future__ import annotations

import json
from pathlib import Path

from .models import ChangeRecord, Op


def record_to_dict(r: ChangeRecord) -> dict:
    return {
        "seq": r.seq,
        "table": r.table,
        "pk": r.pk,
        "op": r.op.value,
        "fields": r.fields,
        "schema_version": r.schema_version,
    }


def record_from_dict(d: dict) -> ChangeRecord:
    return ChangeRecord(
        seq=d["seq"],
        table=d["table"],
        pk=d["pk"],
        op=Op(d["op"]),
        fields=d["fields"],
        schema_version=d["schema_version"],
    )


class LocalSource:
    """把变更追加到本地日志文件，并支持按位点区间读取。

    位点语义：seq 从 1 开始严格递增、无空洞；append 时校验单调性，
    保证“已确认位点之后继续读取”是良定义的。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("")

    def append(self, record: ChangeRecord) -> None:
        expected = self.next_seq()
        if record.seq != expected:
            raise ValueError(f"seq 不连续: 期望 {expected}, 收到 {record.seq}")
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record_to_dict(record), ensure_ascii=False) + "\n")

    def read_from(self, after_seq: int, limit: int | None = None) -> list[ChangeRecord]:
        """读取 seq > after_seq 的变更，按 seq 升序。"""
        out: list[ChangeRecord] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            r = record_from_dict(json.loads(line))
            if r.seq > after_seq:
                out.append(r)
                if limit is not None and len(out) >= limit:
                    break
        out.sort(key=lambda r: r.seq)
        return out

    def next_seq(self) -> int:
        last = 0
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                last = json.loads(line)["seq"]
        return last + 1

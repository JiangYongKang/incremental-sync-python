"""上游数据源模拟：本地 JSONL 变更日志，位点单调递增。

流中包含两类事件（SyncEvent）：
- 变更记录 ChangeRecord（可选携带 txn_id 事务标记）；
- 事务提交标记 TxnCommit：声明某事务的全部成员 seq 已发齐。
"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from .models import ChangeRecord, Op, SyncEvent, TxnCommit


def record_to_dict(r: ChangeRecord) -> dict:
    return {
        "kind": "record",
        "seq": r.seq,
        "table": r.table,
        "pk": r.pk,
        "op": r.op.value,
        "fields": r.fields,
        "schema_version": r.schema_version,
        "txn_id": r.txn_id,
    }


def record_from_dict(d: dict) -> ChangeRecord:
    return ChangeRecord(
        seq=d["seq"],
        table=d["table"],
        pk=d["pk"],
        op=Op(d["op"]),
        fields=d["fields"],
        schema_version=d["schema_version"],
        txn_id=d.get("txn_id"),
    )


def commit_to_dict(c: TxnCommit) -> dict:
    return {"kind": "commit", "seq": c.seq, "txn_id": c.txn_id,
            "members": list(c.members)}


def event_from_dict(d: dict) -> SyncEvent:
    if d.get("kind", "record") == "commit":
        return TxnCommit(seq=d["seq"], txn_id=d["txn_id"],
                         members=tuple(d["members"]))
    return record_from_dict(d)


class LocalSource:
    """把变更追加到本地日志文件，并支持按位点区间读取。

    位点语义：seq 从 1 开始严格递增、无空洞（变更记录与提交标记各自占一个
    seq）；append 时校验单调性，保证“已确认位点之后继续读取”良定义。
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

    def append_commit(self, commit: TxnCommit) -> None:
        expected = self.next_seq()
        if commit.seq != expected:
            raise ValueError(f"seq 不连续: 期望 {expected}, 收到 {commit.seq}")
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(commit_to_dict(commit), ensure_ascii=False) + "\n")

    def append_txn(self, txn_id: str,
                   records: list[ChangeRecord]) -> TxnCommit:
        """连续写入一个事务的全部变更及提交标记，自动分配 seq。"""
        members: list[int] = []
        written: list[ChangeRecord] = []
        for r in records:
            seq = self.next_seq()
            members.append(seq)
            r = replace(r, seq=seq, txn_id=txn_id)
            self.append(r)
            written.append(r)
        commit = TxnCommit(seq=self.next_seq(), txn_id=txn_id,
                           members=tuple(members))
        self.append_commit(commit)
        return commit

    def read_events_from(self, after_seq: int,
                         limit: int | None = None) -> list[SyncEvent]:
        """读取 seq > after_seq 的全部事件（含提交标记），按 seq 升序。"""
        out: list[SyncEvent] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            ev = event_from_dict(json.loads(line))
            if ev.seq > after_seq:
                out.append(ev)
                if limit is not None and len(out) >= limit:
                    break
        out.sort(key=lambda e: e.seq)
        return out

    def read_from(self, after_seq: int, limit: int | None = None) -> list[ChangeRecord]:
        """读取 seq > after_seq 的变更记录（不含提交标记），按 seq 升序。"""
        out: list[ChangeRecord] = []
        for ev in self.read_events_from(after_seq, limit=None):
            if isinstance(ev, ChangeRecord):
                out.append(ev)
                if limit is not None and len(out) >= limit:
                    break
        return out

    def next_seq(self) -> int:
        last = 0
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                last = json.loads(line)["seq"]
        return last + 1

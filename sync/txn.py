"""事务缓冲：按事务标记分组变更，跟踪完整性，支持持久化。

上游按业务事务推送：同一 tx_id 的多条变更（可跨表、可在位点流中
与其他事务交错）必须凑齐才允许整体生效。每条事务记录携带 tx_total
（该事务的记录总条数），缓冲中“已见条数（含被单独消费的过期记录）
达到 tx_total”即视为凑齐。缓冲内容可序列化，进程重启后未凑齐的
事务不丢失、不部分生效。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import ChangeRecord


@dataclass
class _TxState:
    total: int | None = None                          # 事务记录总条数
    records: dict[int, ChangeRecord] = field(default_factory=dict)  # seq -> 缓冲中的记录
    consumed: int = 0                                 # 被单独消费（过期跳过）的条数

    @property
    def complete(self) -> bool:
        return (self.total is not None
                and len(self.records) + self.consumed >= self.total)


class TxnBuffer:
    """未完成事务的缓冲。纯内存记账，序列化由调用方落盘。"""

    def __init__(self):
        self._txs: dict[str, _TxState] = {}

    def add(self, r: ChangeRecord) -> None:
        """缓冲一条事务记录；重复 seq 由调用方先行去重。"""
        assert r.tx_id is not None
        st = self._txs.setdefault(r.tx_id, _TxState())
        st.total = r.tx_total
        st.records[r.seq] = r

    def note_meta(self, r: ChangeRecord) -> None:
        """记录被单独消费（如过期跳过）时，仍登记事务存在性并计入已见条数。"""
        assert r.tx_id is not None
        st = self._txs.setdefault(r.tx_id, _TxState())
        st.total = r.tx_total
        st.consumed += 1

    def is_complete(self, tx_id: str) -> bool:
        st = self._txs.get(tx_id)
        return st is not None and st.complete

    def buffered_seqs(self) -> set[int]:
        return {seq for st in self._txs.values() for seq in st.records}

    def ready_tx_ids(self) -> list[str]:
        """已凑齐、待整体生效的事务，按最小 seq 排序保证处理顺序确定。"""
        ready = [tid for tid, st in self._txs.items() if st.complete]
        ready.sort(key=lambda tid: min(self._txs[tid].records) if self._txs[tid].records else 0)
        return ready

    def pop(self, tx_id: str) -> list[ChangeRecord]:
        """取出并移除一个事务的全部记录（按 seq 升序）。"""
        st = self._txs.pop(tx_id, None)
        if st is None:
            return []
        return [st.records[s] for s in sorted(st.records)]

    def records_of(self, tx_id: str) -> list[ChangeRecord]:
        """查看一个事务的全部记录（按 seq 升序），不移除。"""
        st = self._txs.get(tx_id)
        if st is None:
            return []
        return [st.records[s] for s in sorted(st.records)]

    # ---- 持久化 ----

    def to_dict(self) -> dict:
        from .source import record_to_dict
        return {
            tid: {"total": st.total,
                  "consumed": st.consumed,
                  "records": [record_to_dict(st.records[s]) for s in sorted(st.records)]}
            for tid, st in self._txs.items()
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TxnBuffer":
        from .source import record_from_dict
        buf = cls()
        for tid, st in d.items():
            buf._txs[tid] = _TxState(
                total=st["total"],
                records={r["seq"]: record_from_dict(r) for r in st["records"]},
                consumed=st.get("consumed", 0),
            )
        return buf

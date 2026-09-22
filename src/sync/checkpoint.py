"""位点（checkpoint）管理。

位点语义：

- 全流使用单调连续的整数序列号 ``seq``，从 1 开始，0 表示尚无任何提交。
- ``committed_seq`` 是"已确认位点"：所有 ``seq <= committed_seq`` 的事件
  都已随数据写入在同一个原子快照中落盘。
- 批次提交规则（先校验、后写入，任一不满足则整批拒绝、位点不动）：

  * 批次内事件必须按 ``seq`` 唯一且升序；
  * 首个 ``seq`` 必须等于 ``committed_seq + 1``：
      - 小于等于 committed_seq       -> 位点回退（CheckpointRollbackError）
      - 大于 committed_seq + 1       -> 跳段（CheckpointGapError）
  * 批次内 ``seq`` 不连续           -> 跳段（CheckpointGapError）

- 重复投递：首个 ``seq == committed_seq + 1`` 不成立的整批重复会被拒绝；
  而"新批次里混入旧事件"由引擎按 seq 幂等去重（见 engine 模块）。
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol

from sync.errors import CheckpointGapError, CheckpointRollbackError


class _SeqCarrier(Protocol):
    seq: int


class CheckpointManager:
    @staticmethod
    def get(state: dict[str, Any]) -> int:
        return int(state.get("checkpoint", 0))

    @staticmethod
    def validate_batch(state: dict[str, Any], events: Iterable[_SeqCarrier]) -> list:
        """校验批次位点合法性，返回按 seq 排序后的事件列表。

        - 空批次直接返回，不改变位点。
        - 位点回退 / 跳段 / 批次内乱序或重复时抛出，不产生任何副作用。
        """

        ordered = sorted(events, key=lambda e: e.seq)
        if not ordered:
            return ordered

        seqs = [e.seq for e in ordered]
        committed = CheckpointManager.get(state)
        expected_first = committed + 1

        if seqs[0] < expected_first:
            # 整个批次落在历史区间（常见于整体重复投递或重启后重复拉取）
            raise CheckpointRollbackError(
                f"checkpoint rollback rejected: committed_seq={committed}, "
                f"batch starts at seq={seqs[0]} (<= {committed}); "
                "resume from seq=" + str(expected_first)
            )
        if seqs[0] > expected_first:
            raise CheckpointGapError(
                f"checkpoint gap rejected: committed_seq={committed}, "
                f"batch starts at seq={seqs[0]} but expected {expected_first}"
            )

        # 批次内：连续 + 唯一（排序后相邻相等即重复，差 >1 即空洞）
        for prev, cur in zip(seqs, seqs[1:]):
            if cur == prev:
                raise CheckpointGapError(
                    f"duplicate seq {cur} inside one batch"
                )
            if cur != prev + 1:
                raise CheckpointGapError(
                    f"checkpoint gap inside batch: seq {prev} followed by {cur}"
                )
        return ordered

    @staticmethod
    def advance(state: dict[str, Any], new_seq: int) -> None:
        """在提交回调内推进位点。只允许单调前进。"""

        current = CheckpointManager.get(state)
        if new_seq < current:
            raise CheckpointRollbackError(
                f"cannot move checkpoint backwards: {current} -> {new_seq}"
            )
        state["checkpoint"] = new_seq

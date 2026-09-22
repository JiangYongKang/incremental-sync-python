"""位点规则：正常接续、位点回退、跳段、批次内空洞/重复。"""

from __future__ import annotations

import pytest

from sync.checkpoint import CheckpointManager as C
from sync.errors import CheckpointGapError, CheckpointRollbackError


class E:
    def __init__(self, seq: int) -> None:
        self.seq = seq


def test_continuous_batches_advance():
    st = {"checkpoint": 0}
    ordered = C.validate_batch(st, [E(1), E(2)])
    assert [e.seq for e in ordered] == [1, 2]
    C.advance(st, 2)
    ordered = C.validate_batch(st, [E(3)])
    assert [e.seq for e in ordered] == [3]
    C.advance(st, 3)
    assert C.get(st) == 3


def test_out_of_order_input_is_sorted():
    st = {"checkpoint": 0}
    ordered = C.validate_batch(st, [E(3), E(1), E(2)])
    assert [e.seq for e in ordered] == [1, 2, 3]


def test_rollback_whole_batch_rejected():
    st = {"checkpoint": 5}
    with pytest.raises(CheckpointRollbackError):
        C.validate_batch(st, [E(5)])
    with pytest.raises(CheckpointRollbackError):
        C.validate_batch(st, [E(4), E(5)])
    assert C.get(st) == 5  # 位点不动


def test_gap_at_batch_start_rejected():
    st = {"checkpoint": 5}
    with pytest.raises(CheckpointGapError):
        C.validate_batch(st, [E(7), E(8)])
    assert C.get(st) == 5


def test_gap_inside_batch_rejected():
    st = {"checkpoint": 0}
    with pytest.raises(CheckpointGapError):
        C.validate_batch(st, [E(1), E(3)])


def test_duplicate_seq_inside_batch_rejected_at_manager_level():
    st = {"checkpoint": 0}
    with pytest.raises(CheckpointGapError):
        C.validate_batch(st, [E(1), E(1)])


def test_advance_never_goes_backwards():
    st = {"checkpoint": 9}
    with pytest.raises(CheckpointRollbackError):
        C.advance(st, 8)

"""本地存储：原子提交与重启加载。"""

from __future__ import annotations

import os

from sync.models import RejectedRecord
from sync.errors import RejectReason
from sync.storage import LocalStore


def test_commit_is_atomic_on_failure(tmp_path):
    store = LocalStore(tmp_path / "s.json")

    def ok(state):
        state["tables"].setdefault("t", {})[store.encode_key({"id": 1})] = {"id": 1}

    store.commit(ok)

    def bad(state):
        state["tables"]["t"][store.encode_key({"id": 2})] = {"id": 2}
        raise RuntimeError("boom mid-commit")

    try:
        store.commit(bad)
    except RuntimeError:
        pass

    # 内存状态回滚
    assert store.row_count("t") == 1
    # 磁盘状态不变（重新加载验证）
    reloaded = LocalStore(tmp_path / "s.json")
    assert reloaded.row_count("t") == 1


def test_state_file_contains_single_snapshot(tmp_path):
    store = LocalStore(tmp_path / "s.json")
    store.commit(lambda st: st["tables"].setdefault("t", {}).update(
        {store.encode_key({"id": 1}): {"id": 1}}))
    reloaded = LocalStore(tmp_path / "s.json")
    assert reloaded.get_row("t", {"id": 1}) == {"id": 1}
    # 不残留临时文件
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]


def test_dead_letters_roundtrip(tmp_path):
    store = LocalStore(tmp_path / "s.json")

    def mutate(st):
        store.append_dead_letters_locked([
            RejectedRecord(seq=1, table="t", op="insert",
                           reason=RejectReason.TYPE_MISMATCH,
                           detail="bad", event={"seq": 1})
        ])

    store.commit(mutate)
    reloaded = LocalStore(tmp_path / "s.json")
    letters = reloaded.dead_letters()
    assert letters[0]["reason"] == "type_mismatch"
    assert letters[0]["event"] == {"seq": 1}

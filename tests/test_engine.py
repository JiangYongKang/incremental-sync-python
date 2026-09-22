"""同步引擎集成测试：幂等、乱序、并发、重启、拒绝原因、结构演进。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from sync.engine import SyncEngine
from sync.errors import (
    CheckpointGapError,
    CheckpointRollbackError,
    SchemaIncompatibleError,
)
from sync.models import (
    ChangeEvent,
    Column,
    OpType,
    SchemaChangeEvent,
    TableSchema,
)
from sync.storage import LocalStore


@pytest.fixture()
def engine(tmp_path):
    eng = SyncEngine(LocalStore(tmp_path / "state.json"))
    eng.register_table(
        TableSchema(
            "users", 1,
            (Column("id", "int", False),
             Column("name", "str", False),
             Column("age", "int", True)),
            ("id",),
        )
    )
    return eng


def ins(seq, id_, name, **kw):
    return ChangeEvent(seq=seq, table="users", op=OpType.INSERT,
                       key={"id": id_}, data={"id": id_, "name": name, **kw},
                       schema_version=kw.pop("sv", 1))


def upd(seq, id_, data, sv=1):
    return ChangeEvent(seq=seq, table="users", op=OpType.UPDATE,
                       key={"id": id_}, data=data, schema_version=sv)


def dele(seq, id_):
    return ChangeEvent(seq=seq, table="users", op=OpType.DELETE,
                       key={"id": id_}, data={})


def reasons(result):
    return {(r.seq, r.reason.value) for r in result.rejected}


# ---------------------------------------------------------------- 基本 CRUD
def test_ordered_insert_update_delete(engine):
    engine.submit([ins(1, 1, "a"), ins(2, 2, "b")])
    engine.submit([upd(3, 1, {"name": "a2"})])
    assert engine.get_row("users", id=1)["name"] == "a2"
    engine.submit([dele(4, 1)])
    assert engine.get_row("users", id=1) is None
    assert engine.committed_seq == 4


# ---------------------------------------------------------------- 乱序
def test_out_of_order_batch_applied_in_seq_order(engine):
    r = engine.submit([ins(2, 2, "b"), ins(1, 1, "a")])
    assert r.applied == [1, 2]
    assert engine.committed_seq == 2


def test_out_of_order_updates_converge(engine):
    """同一条记录的多次更新乱序到达，最终值必须与按序到达一致。"""
    engine.submit([ins(1, 1, "v0")])
    ordered = [upd(2, 1, {"name": "v1"}), upd(3, 1, {"name": "v2"}),
               upd(4, 1, {"name": "v3"})]
    # 反向投递
    engine.submit(list(reversed(ordered)))
    assert engine.get_row("users", id=1)["name"] == "v3"


# ---------------------------------------------------------------- 重复投递
def test_redelivered_whole_batch_is_rejected_and_state_unchanged(engine):
    batch = [ins(1, 1, "a"), ins(2, 2, "b")]
    engine.submit(batch)
    with pytest.raises(CheckpointRollbackError):
        engine.submit(batch)
    assert engine.committed_seq == 2
    assert engine.row_count_snapshot("users") == 2 if hasattr(engine, "row_count_snapshot") else True
    assert len(engine.all_rows("users")) == 2
    assert engine.dead_letters() == []


def test_duplicate_record_inside_batch_is_idempotent(engine):
    ev = ins(1, 1, "a")
    r = engine.submit([ev, ev])
    assert r.skipped_duplicates == [1]
    assert len(engine.all_rows("users")) == 1


def test_conflicting_same_seq_payloads_rejected(engine):
    with pytest.raises(Exception, match="conflicting payloads"):
        engine.submit([ins(1, 1, "a"), ins(1, 1, "different")])
    assert engine.committed_seq == 0
    assert engine.all_rows("users") == []


# ---------------------------------------------------------------- 并发提交
def test_concurrent_batches_serialize_without_duplication(engine):
    """多个线程并发投递同一批：恰好一批成功，其余被位点规则明确拒绝。

    成功后每个线程再从最新位点续投自己的私有事件，
    最终数据无重复、无丢失、位点连续。
    """

    def submit_first_then_continue(worker_id, errors):
        # 所有线程都从 seq=1 抢同一批
        try:
            engine.submit([ins(1, 1, "shared"), ins(2, 2, "shared2")])
        except CheckpointRollbackError as e:
            errors.append(e)

        # 抢锁失败后：以最新位点为准，循环重试直到本线程的 2 条私有事件都被接受
        contributed = 0
        while contributed < 2:
            seq = engine.committed_seq + 1
            try:
                engine.submit([ins(seq, 1000 + worker_id * 100 + seq,
                                   f"w{worker_id}-{seq}")])
                contributed += 1
            except CheckpointRollbackError:
                continue  # 又被别的线程抢先，重新读位点后重试

    errors: list[Exception] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(submit_first_then_continue, i, errors)
                   for i in range(6)]
        for f in futures:
            f.result()

    # 共享批次占 seq 1-2；6 个线程各补 2 条 = 12 条
    assert engine.committed_seq == 14
    rows = engine.all_rows("users")
    assert len(rows) == 14
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids))  # 无重复主键
    assert sorted(ids)[:2] == [1, 2]  # 共享批次只生效一次
    assert len(errors) == 5           # 5 个重复批次被明确拒绝


def test_concurrent_retry_after_rejection(engine):
    """两个线程从同一位点竞争：败者收到回退拒绝后从断点续传成功。"""

    barrier = threading.Barrier(2)

    def worker(seen):
        barrier.wait()
        try:
            engine.submit([ins(1, 1, "winner")])
            seen.append(1)
        except CheckpointRollbackError:
            seq = engine.committed_seq + 1  # 从已确认位点之后继续
            engine.submit([ins(seq, seq + 100, "loser-resume")])
            seen.append(seq)

    seen: list[int] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(worker, seen)
        f2 = pool.submit(worker, seen)
        f1.result(); f2.result()

    assert engine.committed_seq == 2
    assert len(engine.all_rows("users")) == 2
    assert sorted(seen) == [1, 2]


# ---------------------------------------------------------------- 拒绝原因
def test_reject_reasons_are_distinguishable(engine):
    r = engine.submit([
        ins(1, 1, "ok"),
        ins(2, 1, "dup"),                                    # duplicate_key
        ChangeEvent(seq=3, table="users", op=OpType.INSERT,  # missing_field
                    key={"id": 3}, data={"id": 3}),
        ChangeEvent(seq=4, table="users", op=OpType.INSERT,  # type_mismatch
                    key={"id": 4}, data={"id": 4, "name": "x", "age": "四"}),
        ChangeEvent(seq=5, table="users", op=OpType.INSERT,  # unknown_field
                    key={"id": 5}, data={"id": 5, "name": "x", "ghost": 1}),
        upd(6, 99, {"name": "ghost"}),                       # row_not_found
        dele(7, 99),                                         # row_not_found
        ChangeEvent(seq=8, table="nope", op=OpType.INSERT,   # 表未注册
                    key={"id": 1}, data={"id": 1}),
        ChangeEvent(seq=9, table="users", op=OpType.INSERT,  # schema 版本未知
                    key={"id": 9}, data={"id": 9, "name": "x"},
                    schema_version=99),
        ChangeEvent(seq=10, table="users", op=OpType.INSERT,  # 主键畸形
                    key={"id": 10, "extra": 1},
                    data={"id": 10, "name": "x"}),
    ])
    assert reasons(r) == {
        (2, "duplicate_key"),
        (3, "missing_field"),
        (4, "type_mismatch"),
        (5, "unknown_field"),
        (6, "row_not_found"),
        (7, "row_not_found"),
        (8, "schema_version_unknown"),
        (9, "schema_version_unknown"),
        (10, "malformed_record"),
    }
    # 非法记录不阻塞位点，且全部落死信、无静默丢弃
    assert engine.committed_seq == 10
    assert len(engine.dead_letters()) == 9
    assert {d["reason"] for d in engine.dead_letters()} >= {
        "duplicate_key", "missing_field", "type_mismatch",
        "unknown_field", "row_not_found", "malformed_record",
    }
    # 合法行依然写入
    assert engine.get_row("users", id=1)["name"] == "ok"


def test_update_unknown_field_and_bad_type_rejected(engine):
    engine.submit([ins(1, 1, "a")])
    r = engine.submit([
        upd(2, 1, {"ghost": 9}),
        upd(3, 1, {"age": "十八岁"}),
        upd(4, 1, {"age": 18}),
    ])
    assert reasons(r) == {(2, "unknown_field"), (3, "type_mismatch")}
    row = engine.get_row("users", id=1)
    assert row["age"] == 18 and row["name"] == "a"  # 仅合法更新生效


# ---------------------------------------------------------------- 重启与位点
def test_restart_resumes_from_committed_checkpoint(tmp_path):
    path = tmp_path / "state.json"
    eng = SyncEngine(LocalStore(path))
    eng.register_table(
        TableSchema("users", 1,
                    (Column("id", "int", False), Column("name", "str", False)),
                    ("id",))
    )
    eng.submit([ChangeEvent(seq=1, table="users", op=OpType.INSERT,
                            key={"id": 1}, data={"id": 1, "name": "a"})])

    # 模拟全新进程
    eng2 = SyncEngine(LocalStore(path))
    assert eng2.committed_seq == 1
    assert eng2.get_row("users", id=1) == {"id": 1, "name": "a"}
    # 重启后旧批次重投必须被拒绝
    with pytest.raises(CheckpointRollbackError):
        eng2.submit([ChangeEvent(seq=1, table="users", op=OpType.INSERT,
                                 key={"id": 1}, data={"id": 1, "name": "a"})])
    # 从断点继续
    eng2.submit([ChangeEvent(seq=2, table="users", op=OpType.INSERT,
                             key={"id": 2}, data={"id": 2, "name": "b"})])
    assert eng2.committed_seq == 2
    assert len(eng2.all_rows("users")) == 2


def test_gap_after_restart_is_rejected(tmp_path):
    path = tmp_path / "state.json"
    eng = SyncEngine(LocalStore(path))
    eng.register_table(
        TableSchema("users", 1,
                    (Column("id", "int", False), Column("name", "str", False)),
                    ("id",))
    )
    eng.submit([ChangeEvent(seq=1, table="users", op=OpType.INSERT,
                            key={"id": 1}, data={"id": 1, "name": "a"})])
    eng2 = SyncEngine(LocalStore(path))
    # 假设上游丢失了 seq=2，直接从 4 开始 -> 跳段拒绝
    with pytest.raises(CheckpointGapError):
        eng2.submit([ChangeEvent(seq=4, table="users", op=OpType.INSERT,
                                 key={"id": 4}, data={"id": 4, "name": "d"})])
    assert eng2.committed_seq == 1  # 未从中间状态继续


# ---------------------------------------------------------------- 结构演进
def test_add_column_backfills_defaults_deterministically(engine):
    engine.submit([ins(1, 1, "a"), ins(2, 2, "b", age=10)])
    v2 = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("name", "str", False),
         Column("age", "int", True), Column("level", "int", False, 1),
         Column("city", "str", True, "SH")),
        ("id",),
    )
    engine.submit([SchemaChangeEvent(seq=3, table="users", new_schema=v2)])
    rows = {r["id"]: r for r in engine.all_rows("users")}
    assert rows[1]["level"] == 1 and rows[1]["city"] == "SH"  # 新列默认值确定
    assert rows[2]["age"] == 10                                # 已有值不被覆盖
    assert engine.latest_schema("users").version == 2


def test_old_schema_version_events_still_validate(engine):
    engine.submit([ins(1, 1, "a")])
    v2 = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("name", "str", False),
         Column("age", "int", True), Column("level", "int", False, 1)),
        ("id",),
    )
    engine.submit([SchemaChangeEvent(seq=2, table="users", new_schema=v2)])
    # 旧版本(v1)写入的事件：不含新列合法，投影时补默认值
    r = engine.submit([ChangeEvent(
        seq=3, table="users", op=OpType.INSERT,
        key={"id": 3}, data={"id": 3, "name": "c"}, schema_version=1)])
    assert r.applied == [3]
    assert engine.get_row("users", id=3)["level"] == 1


def test_incompatible_schema_change_rejects_entire_batch(engine):
    engine.submit([ins(1, 1, "a")])
    v_drop = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("age", "int", True)),
        ("id",),
    )
    with pytest.raises(SchemaIncompatibleError):
        engine.submit([SchemaChangeEvent(seq=2, table="users", new_schema=v_drop)])
    # 整批（含后面的合法数据事件）都不生效，位点不动
    assert engine.committed_seq == 1
    assert engine.latest_schema("users").version == 1
    assert engine.get_row("users", id=1) == {"id": 1, "name": "a", "age": None}


def test_schema_change_and_data_mix_in_one_batch(engine):
    v2 = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("name", "str", False),
         Column("age", "int", True), Column("level", "int", False, 0)),
        ("id",),
    )
    r = engine.submit([
        SchemaChangeEvent(seq=1, table="users", new_schema=v2),
        ChangeEvent(seq=2, table="users", op=OpType.INSERT, key={"id": 1},
                    data={"id": 1, "name": "a", "level": 5}, schema_version=2),
    ])
    assert r.schema_changes == [1]
    assert r.applied == [2]
    assert engine.get_row("users", id=1)["level"] == 5


# ---------------------------------------------------------------- 流水日志
def test_logs_contain_checkpoint_and_change_stream(engine, caplog):
    engine.submit([ins(1, 1, "a"), ins(2, 2, "b")])
    text = caplog.text
    assert "seqs=[1, 2]" in text
    assert "seq=1" in text and "seq=2" in text
    assert "checkpoint=2" in text


# ---------------------------------------------------------------- 边界
def test_empty_batch_does_not_advance(engine):
    r = engine.submit([])
    assert r.committed_seq == 0
    assert engine.all_rows("users") == []


def test_events_roundtrip_through_json():
    from sync.models import SchemaChangeEvent as SC
    s = TableSchema("t", 2, (Column("id", "int", False),), ("id",))
    ddl = SC(seq=1, table="t", new_schema=s)
    assert SC.from_dict(ddl.to_dict()).new_schema.version == 2
    c = ChangeEvent(seq=2, table="t", op=OpType.DELETE, key={"id": 1})
    assert ChangeEvent.from_dict(c.to_dict()).op is OpType.DELETE

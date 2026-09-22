"""本地增量同步演示：不依赖任何外部服务。

运行方式::

    uv run python main.py            # 全流程演示（结束后清理临时状态）
    uv run python main.py --keep     # 保留状态文件 ./demo-state/state.json

脚本用本地 JSON 文件同时模拟"上游变更流"（脚本内手工构造）与"目标端存储"，
依次演示：

1. 建表（v1 结构）；
2. 乱序批次到达 -> 引擎按 seq 排序应用；
3. 整批重复投递 -> 位点回退被明确拒绝，目标端零变化；
4. 含非法记录（主键冲突/缺字段/类型错）的批次 -> 进死信、原因可区分，位点照常推进；
5. 兼容结构演进（新增带默认值的列）-> 存量行确定性回填；
6. 不兼容结构演进（删列）-> 整批拒绝、位点不动；
7. 模拟进程重启 -> 从已确认位点继续。
"""

from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

from sync.engine import SyncEngine
from sync.errors import CheckpointRollbackError, SchemaIncompatibleError
from sync.models import (
    ChangeEvent,
    Column,
    OpType,
    SchemaChangeEvent,
    TableSchema,
)
from sync.storage import LocalStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger("demo")

STATE_DIR = Path("demo-state")


def ev(seq: int, op: str, key: dict, data: dict | None = None, sv: int = 1):
    return ChangeEvent(
        seq=seq, table="orders", op=OpType(op), key=key,
        data=data or {}, schema_version=sv,
    )


def main() -> int:
    keep = "--keep" in sys.argv[1:]
    if STATE_DIR.exists():
        shutil.rmtree(STATE_DIR)
    STATE_DIR.mkdir(parents=True)

    store = LocalStore(STATE_DIR / "state.json")
    engine = SyncEngine(store)

    LOG.info("=== 1. 建表 orders v1: id PK, amount NOT NULL, note 可空 ===")
    engine.register_table(TableSchema(
        "orders", 1,
        (Column("id", "int", False),
         Column("amount", "float", False),
         Column("note", "str", True)),
        ("id",),
    ))

    LOG.info("=== 2. 乱序批次 [seq=2, seq=1] ===")
    engine.submit([
        ev(2, "insert", {"id": 2}, {"id": 2, "amount": 20.0, "note": "b"}),
        ev(1, "insert", {"id": 1}, {"id": 1, "amount": 10.0, "note": "a"}),
    ])
    LOG.info("目标端: %s", engine.all_rows("orders"))

    LOG.info("=== 3. 整批重复投递（模拟 at-least-once 重发） ===")
    batch = [
        ev(2, "insert", {"id": 2}, {"id": 2, "amount": 20.0, "note": "b"}),
        ev(1, "insert", {"id": 1}, {"id": 1, "amount": 10.0, "note": "a"}),
    ]
    try:
        engine.submit(batch)
    except CheckpointRollbackError as e:
        LOG.info("重复投递被明确拒绝: %s", e)

    LOG.info("=== 4. 非法记录批次（seq 3-6），合法记录与非法记录混排 ===")
    engine.submit([
        ev(3, "insert", {"id": 1}, {"id": 1, "amount": 9.9}),   # 主键冲突
        ev(4, "insert", {"id": 4}, {"id": 4, "note": "no-amount"}),  # 缺必填
        ev(5, "insert", {"id": 5}, {"id": 5, "amount": "五块"}),     # 类型不匹配
        ev(6, "insert", {"id": 6}, {"id": 6, "amount": 60.0}),      # 合法
    ])
    LOG.info("死信清单: %s", [
        (d["seq"], d["reason"]) for d in engine.dead_letters()
    ])

    LOG.info("=== 5. 兼容结构演进：新增 status NOT NULL DEFAULT 'new' (seq=7) ===")
    v2 = TableSchema(
        "orders", 2,
        (Column("id", "int", False),
         Column("amount", "float", False),
         Column("note", "str", True),
         Column("status", "str", False, "new")),
        ("id",),
    )
    engine.submit([SchemaChangeEvent(seq=7, table="orders", new_schema=v2)])
    LOG.info("回填后的存量行: %s", engine.all_rows("orders"))

    LOG.info("=== 6. 不兼容演进：删除 note 列（seq=8），必须被拒绝 ===")
    v_bad = TableSchema(
        "orders", 3,
        (Column("id", "int", False),
         Column("amount", "float", False),
         Column("status", "str", False, "new")),
        ("id",),
    )
    try:
        engine.submit([
            SchemaChangeEvent(seq=8, table="orders", new_schema=v_bad),
            ev(9, "insert", {"id": 9}, {"id": 9, "amount": 90.0}),
        ])
    except SchemaIncompatibleError as e:
        LOG.info("不兼容结构被拒绝: %s", e)
    LOG.info("位点仍停在: %s（seq=9 的数据也未写入）", engine.committed_seq)

    LOG.info("=== 7. 模拟进程重启：重新打开同一状态文件 ===")
    del engine
    engine2 = SyncEngine(LocalStore(STATE_DIR / "state.json"))
    LOG.info("重启后已确认位点: %s", engine2.committed_seq)
    engine2.submit([ev(8, "insert", {"id": 8},
                       {"id": 8, "amount": 80.0, "status": "paid"}, sv=2)])
    LOG.info("重启续传后目标端: %s", engine2.all_rows("orders"))

    LOG.info("=== 最终断言摘要 ===")
    rows = engine2.all_rows("orders")
    assert engine2.committed_seq == 8
    assert sorted(r["id"] for r in rows) == [1, 2, 6, 8]
    assert all("status" in r for r in rows)
    assert {(d["seq"], d["reason"]) for d in engine2.dead_letters()} == {
        (3, "duplicate_key"),
        (4, "missing_field"),
        (5, "type_mismatch"),
    }
    LOG.info("全部断言通过：结果与按序只投递一次完全一致。")

    if not keep:
        shutil.rmtree(STATE_DIR)
        LOG.info("已清理临时状态目录（加 --keep 可保留 %s）。", STATE_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""本地增量同步演示服务：全部状态落在 ./data 目录，无外部依赖。"""
from pathlib import Path

from fastapi import FastAPI

from sync.applier import SyncApplier
from sync.checkpoint import CheckpointStore
from sync.models import Column
from sync.pipeline import run_once
from sync.schema_registry import SchemaRegistry
from sync.source import LocalSource
from sync.target import LocalTarget

DATA_DIR = Path("data")

schemas = SchemaRegistry()
schemas.register("users", "id", [
    Column("id", "int", nullable=False),
    Column("name", "str", nullable=False),
    Column("age", "int", nullable=True, default=0),
])
schemas.register("orders", "id", [
    Column("id", "int", nullable=False),
    Column("user_id", "int", nullable=False),
    Column("amount", "int", nullable=False),
])
source = LocalSource(DATA_DIR / "source.jsonl")
target = LocalTarget(DATA_DIR / "target.json")
checkpoints = CheckpointStore(DATA_DIR / "checkpoint.json")
applier = SyncApplier(target, checkpoints, schemas, DATA_DIR / "applier_state.json")

app = FastAPI(title="incremental-sync-demo")


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/sync/run")
def sync_run():
    """从已确认位点继续同步一批；重复调用幂等。"""
    result = run_once(source, applier)
    return {
        "applied": [r.seq for r in result.applied],
        "duplicates": [r.seq for r in result.duplicates],
        "rejected": [{"seq": rj.record.seq, "reason": rj.reason.value,
                      "detail": rj.detail} for rj in result.rejected],
        "committed_txns": result.committed_txns,
        "rejected_txns": [rt.txn_id for rt in result.rejected_txns],
        "staged_txns": result.staged_txns,
        "confirmed_seq": result.committed_seq,
    }


@app.get("/sync/status")
def sync_status():
    # 一次整组一致读取：数据、位点、死信取自同一决议版本，并发提交进行中
    # 也只会读到提交前或提交后的完整状态（不会读到半个跨表事务）。
    view = applier.consistent_view()
    return {
        "confirmed_seq": view.confirmed_seq,
        "next_source_seq": source.next_seq(),
        "generation": view.generation,
        "tables": {t: view.tables.get(t, {}) for t in ["users", "orders"]},
        "dead_letters": [
            {"txn_id": d.txn_id, "commit_seq": d.commit_seq,
             "primary_reason": d.primary_reason.value}
            for d in sorted(view.dead.values(), key=lambda d: d.commit_seq)
        ],
    }

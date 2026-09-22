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
        "confirmed_seq": result.committed_seq,
    }


@app.get("/sync/status")
def sync_status():
    return {
        "confirmed_seq": checkpoints.load(),
        "next_source_seq": source.next_seq(),
        "tables": {t: target.rows(t) for t in ["users"]},
    }

"""同步管线：从源端读取已确认位点之后的变更并应用。"""
from __future__ import annotations

from .applier import SyncApplier
from .models import ApplyResult
from .source import LocalSource


def run_once(source: LocalSource, applier: SyncApplier,
             batch_size: int = 100) -> ApplyResult:
    """从已确认位点继续读取一批并应用；重复调用安全（幂等）。"""
    confirmed = applier.checkpoints.load()
    records = source.read_from(confirmed, limit=batch_size)
    return applier.apply_batch(records)

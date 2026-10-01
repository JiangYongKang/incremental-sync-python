"""同步管线：从源端读取已确认位点之后的事件并应用。"""
from __future__ import annotations

from sync.applier import SyncApplier
from sync.models import ApplyResult
from sync.source import LocalSource


def run_once(source: LocalSource, applier: SyncApplier,
             batch_size: int = 100) -> ApplyResult:
    """从已确认位点继续读取一批事件（变更记录+事务提交标记）并应用；重复调用安全。"""
    confirmed = applier.checkpoints.load()
    events = source.read_events_from(confirmed, limit=batch_size)
    return applier.apply_batch(events)

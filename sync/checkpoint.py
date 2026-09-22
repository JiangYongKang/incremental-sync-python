"""位点与断点管理：持久化已确认位点，拒绝回退与跳段。"""
from __future__ import annotations

import json
import threading
from pathlib import Path


class CheckpointError(Exception):
    """位点回退或跳段。"""


class CheckpointStore:
    """已确认位点持久化在本地 JSON 文件。

    不变式：位点只允许连续推进（new == prev + n 且中间无空洞由调用方保证，
    本类强制要求 commit 声明 expected_prev 并与持久化值一致），
    回退（new <= prev）或跳段（expected_prev != prev）一律拒绝。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def load(self) -> int:
        """返回已确认位点；无存档时为 0。"""
        if not self.path.exists():
            return 0
        return json.loads(self.path.read_text(encoding="utf-8"))["confirmed_seq"]

    def commit(self, seq: int, expected_prev: int) -> None:
        """仅允许从 expected_prev 连续推进到 seq；回退或跳段抛 CheckpointError。"""
        with self._lock:
            prev = self.load()
            if expected_prev != prev:
                raise CheckpointError(
                    f"位点跳段: 已确认 {prev}, 调用方声明前驱 {expected_prev}"
                )
            if seq <= prev:
                raise CheckpointError(f"位点回退: 已确认 {prev}, 拒绝推进到 {seq}")
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps({"confirmed_seq": seq}), encoding="utf-8")
            tmp.replace(self.path)

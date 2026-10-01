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
    commit 强制要求声明 expected_prev 并与当前值一致），
    回退（new <= prev）或跳段（expected_prev != prev）一律拒绝。

    内存视图：应用器在原子提交边界先用 stage() 更新内存值（权威快照随之写入
    journal），快照落盘成功后再由 rewrite() 重写旁路文件；崩溃重启时以
    journal 快照为准，rewrite() 强制对齐文件。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._value = self._read()

    def _read(self) -> int:
        if not self.path.exists():
            return 0
        return json.loads(self.path.read_text(encoding="utf-8"))["confirmed_seq"]

    def _write(self, seq: int) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps({"confirmed_seq": seq}), encoding="utf-8")
        tmp.replace(self.path)

    def load(self) -> int:
        """返回当前已确认位点（内存视图）；无存档时为 0。"""
        with self._lock:
            return self._value

    def commit(self, seq: int, expected_prev: int) -> None:
        """仅允许从 expected_prev 连续推进到 seq；回退或跳段抛 CheckpointError。"""
        with self._lock:
            prev = self._value
            if expected_prev != prev:
                raise CheckpointError(
                    f"位点跳段: 已确认 {prev}, 调用方声明前驱 {expected_prev}"
                )
            if seq <= prev:
                raise CheckpointError(f"位点回退: 已确认 {prev}, 拒绝推进到 {seq}")
            self._value = seq
            self._write(seq)

    def stage(self, seq: int) -> None:
        """仅更新内存视图；磁盘文件随后随原子快照重写（崩溃则快照未生效，无影响）。"""
        with self._lock:
            self._value = seq

    def rewrite(self, seq: int) -> None:
        """以权威快照值强制重写文件（恢复/原子提交后对齐旁路文件）。"""
        with self._lock:
            self._value = seq
            self._write(seq)

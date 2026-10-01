"""被拒事务的持久化留存：完整记录、可事后查看、重投结论一致。"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from .models import TxRejection
from .source import record_from_dict, record_to_dict


def tx_rejection_to_dict(t: TxRejection) -> dict:
    return {
        "tx_id": t.tx_id,
        "reason": t.reason.value,
        "detail": t.detail,
        "records": [record_to_dict(r) for r in t.records],
    }


def tx_rejection_from_dict(d: dict) -> TxRejection:
    from .models import RejectReason
    return TxRejection(
        tx_id=d["tx_id"],
        reason=RejectReason(d["reason"]),
        detail=d["detail"],
        records=tuple(record_from_dict(r) for r in d["records"]),
    )


class RejectionStore:
    """被拒事务按 tx_id 落盘（JSON 文件，原子替换）。

    幂等：同一 tx_id 重复写入时校验内容一致（重投结论稳定），不重复留存。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        if self.path.exists():
            self._data: dict[str, dict] = json.loads(
                self.path.read_text(encoding="utf-8") or "{}"
            )
        else:
            self._data = {}

    def put(self, rejection: TxRejection) -> None:
        with self._lock:
            d = tx_rejection_to_dict(rejection)
            existing = self._data.get(rejection.tx_id)
            if existing is not None:
                if existing != d:
                    raise ValueError(
                        f"事务 {rejection.tx_id} 的拒绝结论与已留存不一致"
                    )
                return  # 重投结论相同：幂等跳过
            self._data[rejection.tx_id] = d
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            tmp.replace(self.path)

    def get(self, tx_id: str) -> TxRejection | None:
        with self._lock:
            d = self._data.get(tx_id)
            return tx_rejection_from_dict(d) if d is not None else None

    def list(self) -> list[TxRejection]:
        with self._lock:
            return [tx_rejection_from_dict(d) for d in self._data.values()]

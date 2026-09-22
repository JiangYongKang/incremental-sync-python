"""重复投递、乱序到达、并发提交下的一致性。"""
import random
import threading

from helpers import make_env, sample_changes


def test_in_order_baseline(tmp_path):
    _, _, target, checkpoints, applier = make_env(tmp_path)
    result = applier.apply_batch(sample_changes())
    assert len(result.applied) == 5
    assert result.committed_seq == 5
    assert checkpoints.load() == 5
    assert target.rows("users") == {"2": {"id": 2, "name": "bobby", "age": 0}}


def test_duplicate_delivery_is_idempotent(tmp_path):
    _, _, target, _, applier = make_env(tmp_path)
    changes = sample_changes()
    r1 = applier.apply_batch(changes)
    r2 = applier.apply_batch(changes)          # 整批重复投递
    r3 = applier.apply_batch(changes[:2])      # 部分重复
    assert len(r1.applied) == 5
    assert len(r2.applied) == 0 and len(r2.duplicates) == 5
    assert len(r3.applied) == 0 and len(r3.duplicates) == 2
    assert target.rows("users") == {"2": {"id": 2, "name": "bobby", "age": 0}}


def test_out_of_order_same_final_state(tmp_path):
    changes = sample_changes()
    # 基线：按序只投递一次
    _, _, target_a, _, applier_a = make_env(tmp_path / "a")
    applier_a.apply_batch(changes)
    # 乱序 + 重复混合，逐条乱序投递
    _, _, target_b, _, applier_b = make_env(tmp_path / "b")
    shuffled = changes[:]
    random.Random(42).shuffle(shuffled)
    for r in shuffled:
        applier_b.apply_batch([r])
    applier_b.apply_batch(changes)  # 再整体重复一遍
    assert target_b.rows("users") == target_a.rows("users")


def test_concurrent_commit_same_final_state(tmp_path):
    changes = sample_changes()
    _, _, target_a, _, applier_a = make_env(tmp_path / "a")
    applier_a.apply_batch(changes)

    _, _, target_b, checkpoints_b, applier_b = make_env(tmp_path / "b")
    errors = []

    def worker(batch):
        try:
            applier_b.apply_batch(batch)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    # 4 个线程并发提交互相重叠、乱序的批次
    batches = [changes[2:], changes[:3], list(reversed(changes)), changes]
    threads = [threading.Thread(target=worker, args=(b,)) for b in batches]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert target_b.rows("users") == target_a.rows("users")
    assert checkpoints_b.load() == 5  # 位点恰好推进到连续末尾，无重复无丢失

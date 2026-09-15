"""领域纯逻辑测试：不一致原因分类、退避、JSON 比较。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config
from app.model import (classify, drift_ratio, next_backoff_delay,
                       split_batches, state_matches, states_equal, canonical)


def test_states_equal_ignores_key_order_and_whitespace():
    assert states_equal('{"a":1,"b":2}', '{"b": 2, "a": 1}')
    assert not states_equal('{"a":1}', '{"a":2}')
    assert canonical(None) is None
    print("ok: states_equal")


def test_classify_reasons():
    C = classify

    def call(desired_v=1, reported_v=1, desired='{"x":1}',
             reported='{"x":1}', online=True, status=None,
             ever=True):
        return C(desired_version=desired_v, reported_version=reported_v,
                 desired_json=desired, reported_json=reported,
                 online=online, open_command_status=status,
                 ever_reported=ever)

    assert call(desired_v=0, ever=False) == (True, "NEVER_DESIRED")
    # 内容一致即 IN_SYNC（desired/reported 版本号无关）
    assert call(desired_v=5, reported_v=12,
                desired='{"x":1}', reported='{"x":1}') == (True, "IN_SYNC")
    # 设了期望，从未上报
    ok, r = call(desired_v=1, reported_v=0, desired='{"x":1}',
                 reported="{}", online=False,
                 status=config.ST_QUEUED, ever=False)
    assert r == "NEVER_REPORTED"
    # 离线排队 / 在线待发 / 在途 / 重试 / 失败 / 过期 / 取代 / 已确认未上报
    assert call(status=config.ST_QUEUED, online=False,
                desired='{"x":2}', reported='{"x":1}')[1] == "DEVICE_OFFLINE"
    assert call(status=config.ST_QUEUED, online=True,
                desired='{"x":2}', reported='{"x":1}')[1] == "PENDING_DELIVERY"
    for st, expected in [
        (config.ST_SENT, "IN_FLIGHT"),
        (config.ST_RETRYING, "RETRYING"),
        (config.ST_FAILED, "FAILED_DISPATCH"),
        (config.ST_EXPIRED, "EXPIRED"),
        (config.ST_SUPERSEDED, "SUPERSEDED"),
    ]:
        _, r = call(status=st, desired='{"x":2}', reported='{"x":1}')
        assert r == expected, (st, r)
    # ACKED 但没补报匹配内容 vs ACK 之后设备漂移
    assert call(status=config.ST_ACKED, desired='{"x":2}',
                reported='{"x":1}')[1] == "ACKED_NOT_REPORTED"
    ok, r = C(desired_version=2, reported_version=5,
              desired_json='{"x":2}', reported_json='{"x":1}',
              online=True, open_command_status=config.ST_ACKED,
              ever_reported=True, acked_at=100.0,
              reported_updated_at=200.0)
    assert r == "CONTENT_MISMATCH", r
    # 没有未决命令但内容不同 -> CONTENT_MISMATCH
    assert call(status=None, desired='{"led":true}',
                reported='{"led":false}')[1] == "CONTENT_MISMATCH"
    print("ok: classify reasons")


def test_backoff_exponential_capped():
    assert next_backoff_delay(0) == 0
    assert next_backoff_delay(1, base=2) == 2
    assert next_backoff_delay(2, base=2) == 4
    assert next_backoff_delay(3, base=2) == 8
    assert next_backoff_delay(10, base=2, cap=60) == 60
    print("ok: backoff")


def test_release_batching_and_drift():
    batches = split_batches(["a", "b", "c", "d", "e"], 40)
    assert batches == [["a", "b"], ["c", "d"], ["e"]]
    assert split_batches(["a"], 10) == [["a"]]
    target = {"a": 1, "b": {"c": 2, "d": 3}}
    assert drift_ratio(target, {"a": 1, "b": {"c": 2, "d": 9}}) == 1 / 3
    assert state_matches(target, {"a": 1, "b": {"c": 2, "d": 9}}, 0.34)
    assert not state_matches(target, {"a": 1, "b": {"c": 2, "d": 9}}, 0.3)
    print("ok: release batches and drift threshold")


if __name__ == "__main__":
    test_states_equal_ignores_key_order_and_whitespace()
    test_classify_reasons()
    test_backoff_exponential_capped()
    test_release_batching_and_drift()
    print("ALL MODEL TESTS PASSED")

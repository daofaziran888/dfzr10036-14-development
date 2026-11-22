"""迟到事件分段引擎的回归测试。

只依赖标准库：``cd backend && python -m unittest discover -s tests``。
覆盖关闭位点含边界、两种到达顺序、幂等、回放、续跑关联与提交区间。
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import late_window as lw  # noqa: E402


STREAM = "order_events_cdc"


class BoundaryTest(unittest.TestCase):
    def setUp(self) -> None:
        lw._reset_scenario(STREAM)

    def test_close_position_is_inclusive(self) -> None:
        inside = lw.classify_position(
            source_position=120, close_position=120,
            window_id="W-0001", window_version="v1")
        outside = lw.classify_position(
            source_position=121, close_position=120,
            window_id="W-0001", window_version="v1")
        self.assertEqual(inside["归属"], "原窗口")
        self.assertEqual(inside["边界"], "关闭位点(含边界)")
        self.assertEqual(outside["归属"], "下一窗口")
        self.assertNotEqual(outside["窗口编号"], "W-0001")

    def test_two_arrival_orders_are_equivalent(self) -> None:
        result = lw.compare_arrival_orders(positions=[119, 120, 121], close_position=120)
        self.assertTrue(result["equivalent"])
        self.assertEqual(result["window_versions"]["119"]["窗口版本"], "v1")
        self.assertEqual(result["window_versions"]["120"]["窗口编号"], "W-0001")
        self.assertEqual(result["window_versions"]["121"]["窗口编号"], "W-0002")

    def test_mixed_order_results_match(self) -> None:
        states = []
        for order in ("A", "B"):
            setup = lw.setup_closed_boundary_scenario(arrival_order=order)
            states.append(sorted(
                (row["源位点"], row["窗口编号"], row["窗口版本"], row["status"])
                for row in setup["state"]["segments"]
            ))
        self.assertEqual(states[0], states[1])


class IdempotencyTest(unittest.TestCase):
    def setUp(self) -> None:
        lw._reset_scenario(STREAM)
        lw.setup_closed_boundary_scenario(arrival_order="A")

    def test_repeated_late_event_applies_once(self) -> None:
        before_segments = len(lw.state(stream=STREAM)["segments"])
        before_compensations = len(lw.state(stream=STREAM)["compensations"])
        first = lw.admit_event(
            stream=STREAM, window_id="W-0001", window_version="v1",
            close_position=120, source_position=120, event_ref="evt-120")
        second = lw.admit_event(
            stream=STREAM, window_id="W-0001", window_version="v1",
            close_position=120, source_position=120, event_ref="evt-120")
        self.assertTrue(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(lw.state(stream=STREAM)["segments"]), before_segments)
        self.assertEqual(len(lw.state(stream=STREAM)["compensations"]), before_compensations)

    def test_repeated_confirmation_applies_once(self) -> None:
        first = lw.confirm_compensation("COMP-0120")
        second = lw.confirm_compensation("COMP-0120")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["commit"]["actual_commit_position"], 120)
        self.assertEqual(second["commit"]["actual_commit_position"], 120)


class ProgressTest(unittest.TestCase):
    def setUp(self) -> None:
        lw._reset_scenario(STREAM)
        lw.setup_closed_boundary_scenario(arrival_order="A")

    def test_pending_segment_blocks_commit(self) -> None:
        commit = lw.compute_commit(STREAM, "W-0001", "v1")
        self.assertEqual(commit["actual_commit_position"], 119)
        self.assertEqual(commit["状态"], "待复核阻塞")
        self.assertEqual(commit["阻塞分段"], "LSEG-0002")

    def test_confirm_moves_commit_to_close(self) -> None:
        lw.confirm_compensation("COMP-0120")
        commit = lw.compute_commit(STREAM, "W-0001", "v1")
        self.assertEqual(commit["actual_commit_position"], 120)
        self.assertEqual(commit["确认区间"], "119-120")
        self.assertEqual(commit["状态"], "窗口可提交")

    def test_replay_from_119_is_idempotent_and_keeps_next_window(self) -> None:
        lw.confirm_compensation("COMP-0120")
        first = lw.replay(stream=STREAM, from_position=119)
        self.assertTrue(first["ok"])
        self.assertEqual(first["actual_commit_position"], 120)
        self.assertEqual([row["源位点"] for row in first["consumable_records"]], [119, 120])
        self.assertEqual(
            [row["源位点"] for row in first["duplicate_records"]], [119])
        self.assertEqual(
            [(row["源位点"], row["窗口编号"]) for row in first["next_window_records"]],
            [(121, "W-0002")],
        )
        self.assertEqual(first["next_window_records"][0]["状态"], "正常可消费")

        second = lw.replay(stream=STREAM, from_position=119)
        self.assertEqual(second["newly_delivered"], [])
        self.assertEqual(
            sorted(row["源位点"] for row in second["duplicate_records"]), [119, 120])
        self.assertEqual(second["actual_commit_position"], 120)

    def test_replay_before_confirm_is_blocked(self) -> None:
        result = lw.replay(stream=STREAM, from_position=119)
        self.assertFalse(result["ok"])
        self.assertEqual([row["源位点"] for row in result["pending_review_segments"]], [120])
        self.assertEqual(result["actual_commit_position"], 119)

    def test_replay_before_log_start_is_rejected(self) -> None:
        result = lw.replay(stream=STREAM, from_position=100)
        self.assertFalse(result["ok"])
        self.assertIn("旧日志不可回放", result["message"])

    def test_interrupted_compensation_is_queryable(self) -> None:
        rows = lw.list_compensations(stream=STREAM, interrupted_only=True)
        self.assertEqual([row["补偿编号"] for row in rows], ["COMP-0120"])
        self.assertEqual(rows[0]["交付状态"], "已交付下游")
        self.assertEqual(rows[0]["确认状态"], "待确认")

    def test_resume_links_affected_window_with_receipt(self) -> None:
        resume = lw.resume(stream=STREAM)
        self.assertEqual(resume["replay_start"], 119)
        self.assertTrue(resume["affected_windows"])
        linked = resume["affected_windows"][0]
        self.assertEqual(linked["窗口编号"], "W-0001")
        self.assertEqual(linked["窗口版本"], "v1")
        self.assertEqual(linked["回执编号"], "RCPT-0001")
        self.assertEqual(linked["关联状态"], "已关联成功回执")
        self.assertEqual(
            [row["补偿编号"] for row in resume["pending_compensations"]], ["COMP-0120"])

    def test_full_verify_passes(self) -> None:
        lw.confirm_compensation("COMP-0120")
        lw.replay(stream=STREAM, from_position=119)
        result = lw.verify()
        self.assertTrue(result["ok"], result)
        self.assertTrue(all(check["通过"] for check in result["checks"]))


class BusinessModuleTest(unittest.TestCase):
    """乱序策略的入窗判定走业务服务入口，但不把 22 个模块口径搅乱。"""

    def setUp(self) -> None:
        lw._reset_scenario(STREAM)

    def test_policy_late_judgment_creates_segments(self) -> None:
        from app.services.out_of_order import OutOfOrderService

        service = OutOfOrderService()
        lw.ensure_window(
            stream=STREAM, window_id="W-0001", window_version="v1",
            start_position=118, close_position=120, log_start_position=119,
            status="已关闭")
        result = service.judge_late_events(1, [119, 120, 121], arrival_order="B")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["segments"]), 2)
        self.assertEqual(len(result["next_window_records"]), 1)
        self.assertTrue(result["verification"]["equivalent"])

        # 重复提交同一批事件不新增分段
        before = len(lw.state(stream=STREAM)["segments"])
        repeat = service.judge_late_events(1, [119, 120, 121])
        assert repeat is not None
        self.assertEqual(len(repeat["segments"]), 0)
        self.assertEqual(len(lw.state(stream=STREAM)["segments"]), before)


if __name__ == "__main__":
    unittest.main()

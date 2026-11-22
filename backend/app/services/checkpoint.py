"""检查点服务：断点恢复时触发水位重置和回填任务。

续跑时以「受影响窗口 + 已有成功回执」为依据安排回放，保存位点走迟到分段
推导出的实际提交位置，而不是无条件从检查点里的字符串位点全量重放。
"""
from __future__ import annotations

from typing import Any

from app.services import late_window
from app.services.base import BaseModuleService
from app.store import store


class CheckpointService(BaseModuleService):
    MODULE = "checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["检查点编号", "数据流", "保存位点", "保存时间", "恢复状态", "丢失行数", "回放代价", "恢复耗时"]
    REQUIRED_FIELDS = ["检查点编号", "数据流", "保存位点"]
    STATUS_ORDER = ["已保存", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {"触发恢复": "恢复中", "确认恢复": "已恢复", "标记失败": "恢复失败"}
    NEGATIVE_ACTIONS = []

    def run_continuation(self, entry_id: int) -> dict[str, Any] | None:
        """续跑：关联受影响窗口与成功回执，按分段推导的区间安排回放。

        数据流链路: checkpoint -> checkpoint_affected_window -> late_event_segment
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        stream = str(entry.get("数据流", ""))
        resume = late_window.resume(stream=stream, checkpoint_id=str(entry.get("检查点编号", "")))
        entry["关联窗口"] = "、".join(
            f"{row.get('窗口编号')}/{row.get('窗口版本')}"
            for row in resume.get("affected_windows", [])
        )
        entry["续跑回放起点"] = str(resume.get("replay_start", ""))
        entry["回执关联"] = "、".join(
            str(row.get("回执编号")) for row in resume.get("affected_windows", [])
            if row.get("回执编号")
        )
        entry["实际提交位置"] = str(
            (resume.get("commit") or {}).get("actual_commit_position", ""))
        entry["恢复状态"] = "中断-待续跑"
        entry["status"] = "恢复中"
        entry["pending"] = True
        return resume

    def replay_from_checkpoint(self, entry_id: int) -> dict[str, Any] | None:
        """断点恢复的续跑动作：从检查点保存位点发起迟到分段回放。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        stream = str(entry.get("数据流", ""))
        from_position = late_window.to_int(entry.get("保存位点", 0))
        if from_position <= 0:
            return {
                "ok": False,
                "message": f"检查点 {entry.get('检查点编号')} 的保存位点不是可回放位点",
            }
        result = late_window.replay(stream=stream, from_position=from_position)
        entry["最近回放"] = (result.get("replay_run") or {}).get("回放编号", "")
        entry["实际提交位置"] = str(result.get("actual_commit_position", ""))
        if result.get("ok"):
            entry["恢复状态"] = "已恢复-分段回放完成"
        else:
            entry["恢复状态"] = "回放受阻-存在待复核分段"
        entry["pending"] = not result.get("ok")
        entry["abnormal"] = not result.get("ok")
        return result

    def trigger_watermark_reset_and_backfill(self, entry_id: int) -> dict[str, Any]:
        """断点恢复后触发水位重置和回填任务。

        数据流链路: checkpoint -> watermark_mgr + backfill_run
        水位只推进到迟到分段推导出的实际提交位置。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"watermark": None, "backfill": None}

        if entry.get("status") == "已恢复":
            commit = late_window.compute_commit(tag)
            actual = commit.get("actual_commit_position")
            target = str(actual) if actual is not None else str(entry.get("保存位点", ""))

            # 重置水位
            for wm in store.rows("watermark_mgr"):
                if str(wm.get("数据流", "")) == tag:
                    previous = str(wm.get("提交位点", ""))
                    if previous != target:
                        wm["上一位点"] = previous
                    wm["当前位点"] = target
                    wm["提交位点"] = target
                    wm["位点口径"] = "迟到事件分段"
                    wm["确认区间"] = commit.get("确认区间", "")
                    wm["status"] = "推进中"
                    wm["pending"] = True
                    wm["abnormal"] = False
                    results["watermark"] = wm
                    break

            # 如果丢失行数或未确认分段，创建回填任务
            loss = int(entry.get("丢失行数", 0))
            pending = [
                row for row in late_window.state(stream=tag)["segments"]
                if row.get("status") == "待复核"
            ]
            if loss > 0 or pending:
                bf_rows = store.rows("backfill_run")
                new_bf_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
                backfill = {
                    "id": new_bf_id,
                    "任务编号": f"BACK-{new_bf_id:04d}",
                    "目标表": tag,
                    "起始位点": target,
                    "截止位点": "latest_binlog",
                    "回填模式": "按迟到分段回放",
                    "并行粒度": "按窗口版本",
                    "当前进度": "0%",
                    "回填速率": "0 eps",
                    "待复核分段": "、".join(str(row.get("分段编号")) for row in pending),
                    "status": "回填中",
                    "pending": True,
                    "abnormal": bool(pending),
                }
                bf_rows.append(backfill)
                results["backfill"] = backfill

        return results

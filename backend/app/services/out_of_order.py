"""乱序处理服务：乱序策略生效后级联延迟监控和断点恢复。

跨过已关闭窗口的事件流不再直接计入「丢弃行数」，而是先做入窗阈值判定，
把结果落成可回验的迟到事件分段；重复提交同一迟到记录只生效一次。
"""
from __future__ import annotations

from typing import Any, Iterable

from app.services import late_window
from app.services.base import BaseModuleService
from app.store import store


class OutOfOrderService(BaseModuleService):
    MODULE = "out_of_order"
    ENTITY = "乱序策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "数据流", "时间字段", "允许迟到", "乱序窗口", "纠错模式", "丢弃行数", "修正行数"]
    REQUIRED_FIELDS = ["策略编号", "数据流", "时间字段"]
    STATUS_ORDER = ["草稿", "生效中", "超窗口丢弃", "已停用"]
    ACTION_RULES = {"启效策略": "生效中", "扩窗容忍": "超窗口丢弃", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    # ---- 迟到事件入窗判定 ----

    def _window_basis(self, entry: dict[str, Any]) -> dict[str, Any]:
        """从策略取窗口版本与关闭位点；缺省时按数据流已有登记补齐。"""
        stream = str(entry.get("数据流", ""))
        window_id = str(entry.get("窗口编号") or f"W-{int(entry.get('id', 0)):04d}")
        version = str(entry.get("窗口版本") or "v1")
        window = late_window.find_window(stream, window_id, version)
        close_position = int(entry.get("窗口关闭位点") or (window or {}).get("关闭位点") or 0)
        if window is None or not close_position:
            window = late_window.ensure_window(
                stream=stream,
                window_id=window_id,
                window_version=version,
                start_position=max(0, close_position - 120),
                close_position=close_position,
                log_start_position=max(0, close_position - 1),
                status="已关闭",
                policy_id=str(entry.get("策略编号", "")),
            )
        log_start = int(window.get("日志起点", max(0, close_position - 1)))
        return {
            "stream": stream,
            "window_id": window_id,
            "window_version": version,
            "close_position": close_position,
            "log_start_position": log_start,
            "policy_id": str(entry.get("策略编号", "")),
        }

    def judge_late_events(
        self,
        entry_id: int,
        positions: Iterable[int] | None = None,
        *,
        arrival_order: str = "A",
    ) -> dict[str, Any] | None:
        """对跨过已关闭窗口的事件做入窗阈值判定，形成迟到事件分段。

        - ``源位点 <= 关闭位点``：归原窗口，生成待复核分段与补偿行；
        - ``源位点 > 关闭位点``：进入下一窗口，正常可消费，不丢行；
        - 同一 (流, 窗口版本, 源位点, 事件引用) 只登记一次。

        数据流链路: out_of_order -> late_event_segment + compensation_row
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        if entry.get("status") not in ("生效中", "超窗口丢弃"):
            return {
                "ok": False,
                "message": f"策略当前状态「{entry.get('status')}」，未生效的策略不做迟到判定",
                "policy": entry,
            }

        basis = self._window_basis(entry)
        if basis["close_position"] <= 0:
            return {
                "ok": False,
                "message": "策略缺少窗口关闭位点，无法做入窗阈值判定",
                "policy": entry,
            }

        position_list = list(positions) if positions else []
        window = late_window.find_window(
            basis["stream"], basis["window_id"], basis["window_version"])
        if not position_list:
            # 没有显式位点时不臆造事件，返回阈值与当前分段供回验。
            return {
                "ok": True,
                "policy": entry,
                "basis": basis,
                "window": window,
                "segments": [
                    row for row in late_window.state(stream=basis["stream"])["segments"]
                    if row.get("窗口编号") == basis["window_id"]
                    and row.get("窗口版本") == basis["window_version"]
                ],
                "message": "未提供源位点，仅返回入窗阈值与已登记分段",
            }

        batch = late_window.admit_batch(
            stream=basis["stream"],
            window_id=basis["window_id"],
            window_version=basis["window_version"],
            close_position=basis["close_position"],
            positions=position_list,
            arrival_order=arrival_order,
            log_start_position=basis["log_start_position"],
            policy_id=basis["policy_id"],
        )
        # 只有真正归原窗口的新分段落成丢弃复核，下一窗口记录不计数丢弃。
        newly_pending = [
            item["segment"] for item in batch["results"]
            if not item["duplicate"] and item["segment"].get("归属") == "原窗口"
        ]
        next_window = [
            item["segment"] for item in batch["results"]
            if item["segment"].get("归属") == "下一窗口"
        ]
        entry["迟到事件分段"] = "、".join(
            str(segment.get("分段编号")) for segment in newly_pending) or entry.get("迟到事件分段", "")
        entry["入窗阈值"] = f"源位点 <= {basis['close_position']}（含边界）"
        entry["窗口版本"] = basis["window_version"]
        entry["窗口关闭位点"] = basis["close_position"]
        entry["超窗口丢弃"] = len(newly_pending)
        entry["下一窗口接收"] = len(next_window)
        return {
            "ok": True,
            "policy": entry,
            "basis": basis,
            "batch": batch,
            "segments": newly_pending,
            "next_window_records": next_window,
            "verification": late_window.compare_arrival_orders(
                positions=position_list,
                close_position=basis["close_position"],
                window_id=basis["window_id"],
                window_version=basis["window_version"],
                log_start_position=basis["log_start_position"],
            ),
            "message": (
                f"迟到判定完成：原窗口待复核 {len(newly_pending)} 条，"
                f"下一窗口接收 {len(next_window)} 条"
            ),
        }

    # ---- 既有链路 ----

    def trigger_lag_monitor_update(self, entry_id: int) -> dict[str, Any] | None:
        """乱序策略生效后，更新lag_monitor中的乱序相关延迟指标。

        数据流链路: out_of_order -> lag_monitor
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "生效中":
            return None

        tag = str(entry.get("数据流", ""))
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag:
                lm["状态"] = "正常"
                lm["延迟秒数"] = f"乱序纠错中: {entry.get('修正行数', 0)} 行"
                return lm

        # 无现有lag_monitor，创建新记录
        lm_rows = store.rows("lag_monitor")
        new_id = max((int(r.get("id", 0)) for r in lm_rows), default=0) + 1
        new_lm = {
            "id": new_id,
            "指标编号": f"LAG_-{new_id:04d}",
            "数据流": tag,
            "源端位点": f"binlog@{tag}",
            "处理位点": f"乱序窗口{entry.get('乱序窗口', '未知')}",
            "延迟秒数": f"乱序纠错中: {entry.get('修正行数', 0)} 行",
            "告警阈值": "30s",
            "最近采样": "2026-11-18 10:33:00",
            "采样间隔": "10s",
            "status": "正常",
            "pending": True,
            "abnormal": False,
        }
        lm_rows.append(new_lm)
        return new_lm

    def handle_window_overflow(self, entry_id: int, positions: Iterable[int] | None = None) -> dict[str, Any] | None:
        """乱序超窗口丢弃时，先做迟到入窗判定，再触发checkpoint保底和告警。

        数据流链路: out_of_order(超窗口丢弃) -> late_event_segment
                    + compensation_row + checkpoint + alert_rule
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "超窗口丢弃":
            return None

        tag = str(entry.get("数据流", ""))
        judgment = self.judge_late_events(entry_id, positions)
        results: dict[str, Any] = {
            "checkpoint": None,
            "alert": None,
            "segments": (judgment or {}).get("segments", []),
            "next_window_records": (judgment or {}).get("next_window_records", []),
            "admission": (judgment or {}).get("basis"),
            "verification": (judgment or {}).get("verification"),
        }

        # 创建告警
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        pending_count = len(results["segments"])
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "乱序迟到待复核",
            "告警条件": (
                f"关闭位点 {entry.get('窗口关闭位点', '未知')} 内迟到 {pending_count} 条需复核"
                if pending_count else f"丢弃 > {entry.get('丢弃行数', '阈值')}行"
            ),
            "告警级别": "P3-注意",
            "通知渠道": "飞书群",
            "聚合窗口": "1min",
            "静默时段": "无",
            "触发次数": 1,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        # 创建保底checkpoint，保存位点取旧日志仍可回放的起点。
        basis = results["admission"] or {}
        cp_rows = store.rows("checkpoint")
        new_cp_id = max((int(r.get("id", 0)) for r in cp_rows), default=0) + 1
        cp = {
            "id": new_cp_id,
            "检查点编号": f"CHEC-{new_cp_id:04d}",
            "数据流": tag,
            "保存位点": str(basis.get("log_start_position", "")),
            "保存时间": "2026-11-18 10:34:00",
            "恢复状态": "紧急保底",
            # 旧口径的丢弃行数原样保留做历史对照，迟到待复核另列，不混为一谈。
            "丢失行数": int(entry.get("丢弃行数", 0)),
            "迟到待复核行数": pending_count,
            "下一窗口接收行数": len(results["next_window_records"]),
            "回放代价": "按迟到分段增量回放",
            "恢复耗时": "预估15min",
            "关联窗口": f"{basis.get('window_id', '')}/{basis.get('window_version', '')}",
            "场景": "迟到事件边界回放",
            "status": "已保存",
            "pending": True,
            "abnormal": True,
        }
        cp_rows.append(cp)
        results["checkpoint"] = cp

        late_window.link_affected_windows(
            stream=tag,
            checkpoint_id=str(cp["检查点编号"]),
            replay_start=int(basis.get("log_start_position", 0) or 0),
        )
        results["affected_windows"] = late_window.affected_windows(tag)
        return results

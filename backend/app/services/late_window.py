"""迟到事件分段引擎：给跨过已关闭窗口的事件流提供可回验的入窗判定。

核心约束（与 out_of_order / checkpoint / watermark_mgr 共用）：

1. 入窗阈值以「窗口关闭位点」为准，且**含边界**：
   ``源位点 <= 关闭位点`` 归原窗口，``源位点 > 关闭位点`` 进入下一窗口。
   例如关闭位点为 120 时，位点 119、120 属于原窗口，121 属于下一窗口。
2. 归原窗口但晚于关窗到达的记录必须落成「迟到事件分段」，并生成补偿行，
   不能像旧的超窗口逻辑那样直接计入丢弃。
3. 进入下一窗口的记录是正常可消费记录，不能被迟到纠偏连带丢行。
4. 分段同时记录窗口版本与源位点，重复提交同一迟到记录只生效一次；
   回放同样按源位点幂等，已经成功回执覆盖或已经消费的位置不会二次交付。
5. 提交进度由分段状态推导：只有从已提交位点开始连续可确认的分段才能推进，
   遇到待复核分段就停在它前面，不会越过关闭位点。

账本刻意与 store 里的 22 个业务模块分开存放：这些是引擎运行期数据，
通过 /api/late-window/* 读写，不改变运营概览的模块口径。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable

from app.store import store

WINDOW_TABLE = "window_registry"
SEGMENT_TABLE = "late_event_segment"
COMP_TABLE = "compensation_row"
RECEIPT_TABLE = "late_receipt"
PROGRESS_TABLE = "commit_progress"
AFFECTED_TABLE = "checkpoint_affected_window"
REPLAY_TABLE = "late_replay_run"

CONFIRMED_STATES = {"已确认", "已消费"}
PENDING_STATES = {"待复核"}

# 引擎运行期账本；与 store 的业务模块表分开。
_LEDGER: dict[str, list[dict[str, Any]]] = {
    table: [] for table in (
        WINDOW_TABLE, SEGMENT_TABLE, COMP_TABLE, RECEIPT_TABLE,
        PROGRESS_TABLE, AFFECTED_TABLE, REPLAY_TABLE,
    )
}


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _rows(table: str) -> list[dict[str, Any]]:
    return _LEDGER[table]


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _to_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


# 对外暴露的位点解析：checkpoint 等模块用同一套口径解析保存位点。
to_int = _to_int


def _next_window_id(window_id: str) -> str:
    """W-0001 -> W-0002；没有数字后缀时追加 -2。"""
    matched = re.search(r"(\d+)(?!.*\d)", window_id)
    if not matched:
        return f"{window_id}-2"
    digits = matched.group(1)
    start, end = matched.span(1)
    return f"{window_id[:start]}{int(digits) + 1:0{len(digits)}d}{window_id[end:]}"


def _resolve(identifier: Any, table: str, field: str) -> dict[str, Any] | None:
    rows = _rows(table)
    if isinstance(identifier, dict):
        return identifier
    text = str(identifier).strip()
    for row in rows:
        if str(row.get(field, "")) == text:
            return row
    if text.isdigit():
        for row in rows:
            if int(row.get("id", 0)) == int(text):
                return row
    return None


# ---------------------------------------------------------------------------
# 入窗判定（纯函数，便于两种到达顺序做等价校验）
# ---------------------------------------------------------------------------

def classify_position(
    *,
    source_position: int,
    close_position: int,
    window_id: str,
    window_version: str,
    next_window_id: str | None = None,
    next_window_version: str = "v1",
    log_start_position: int | None = None,
) -> dict[str, Any]:
    """返回单条事件相对关闭界线的归属判定，不读写账本。"""
    inside = source_position <= close_position
    if inside:
        return {
            "源位点": source_position,
            "归属": "原窗口",
            "窗口编号": window_id,
            "窗口版本": window_version,
            "迟到落点": "关闭界线内(原窗口)",
            "迟到处置": "生成补偿行并置为待复核",
            "界线侧": "界线前",
            "边界": "关闭位点(含边界)" if source_position == close_position else "关闭位点内",
            "分段类型": "迟到复核",
            "需要补偿": True,
            "可回放": log_start_position is None or source_position >= log_start_position,
            "入窗阈值": f"源位点 <= {close_position}（含边界）",
            "判定依据": (
                f"位点 {source_position} <= 关闭位点 {close_position}，"
                f"归属原窗口 {window_id}/{window_version}"
            ),
        }
    return {
        "源位点": source_position,
        "归属": "下一窗口",
        "窗口编号": next_window_id or _next_window_id(window_id),
        "窗口版本": next_window_version,
        "迟到落点": "关闭界线外(下一窗口)",
        "迟到处置": "正常入窗，不丢行",
        "界线侧": "界线后",
        "边界": "关闭位点后",
        "分段类型": "正常入窗",
        "需要补偿": False,
        "可回放": False,
        "入窗阈值": f"源位点 > {close_position}（进入下一窗口）",
        "判定依据": (
            f"位点 {source_position} > 关闭位点 {close_position}，"
            f"进入下一窗口，不参与原窗口迟到纠偏"
        ),
    }


def compare_arrival_orders(
    *,
    positions: Iterable[int],
    close_position: int,
    window_id: str = "W-0001",
    window_version: str = "v1",
    next_window_id: str | None = None,
    next_window_version: str = "v1",
    log_start_position: int | None = None,
) -> dict[str, Any]:
    """同一组事件的两种到达顺序：归属必须一致，顺序只影响到达批次。"""
    ordered = list(positions)
    orders = {"A": ordered, "B": list(reversed(ordered))}

    def plan(name: str, sequence: list[int]) -> list[dict[str, Any]]:
        return [
            {
                **classify_position(
                    source_position=position,
                    close_position=close_position,
                    window_id=window_id,
                    window_version=window_version,
                    next_window_id=next_window_id,
                    next_window_version=next_window_version,
                    log_start_position=log_start_position,
                ),
                "到达顺序": f"顺序-{name}",
                "到达序号": index + 1,
            }
            for index, position in enumerate(sequence)
        ]

    plans = {name: plan(name, sequence) for name, sequence in orders.items()}
    assignments = {
        name: sorted(items, key=lambda item: item["源位点"])
        for name, items in plans.items()
    }
    signature = {
        name: {
            item["源位点"]: (item["窗口编号"], item["窗口版本"], item["分段类型"])
            for item in items
        }
        for name, items in plans.items()
    }
    return {
        "ok": True,
        "close_position": close_position,
        "orders": plans,
        "assignments": assignments,
        "equivalent": signature["A"] == signature["B"],
        "window_versions": {
            str(position): {
                "窗口编号": value[0], "窗口版本": value[1], "分段类型": value[2],
            }
            for position, value in sorted(signature["A"].items())
        },
        "rule": (
            f"源位点 <= {close_position} 归原窗口 {window_id}/{window_version}；"
            f"源位点 > {close_position} 进入下一窗口"
        ),
    }


# ---------------------------------------------------------------------------
# 窗口登记
# ---------------------------------------------------------------------------

def ensure_window(
    *,
    stream: str,
    window_id: str,
    window_version: str = "v1",
    start_position: int = 0,
    close_position: int = 0,
    log_start_position: int | None = None,
    status: str = "已关闭",
    policy_id: str = "",
    closed_at: str | None = None,
) -> dict[str, Any]:
    rows = _rows(WINDOW_TABLE)
    for row in rows:
        if (
            str(row.get("数据流", "")) == stream
            and str(row.get("窗口编号", "")) == window_id
            and str(row.get("窗口版本", "")) == window_version
        ):
            row.update({
                "起始位点": start_position,
                "关闭位点": close_position,
                "日志起点": row.get("日志起点", log_start_position if log_start_position is not None else start_position),
                "状态": status,
                "策略编号": policy_id or row.get("策略编号", ""),
            })
            if status == "已关闭" and not row.get("关闭时间"):
                row["关闭时间"] = closed_at or _now()
            return row

    row = {
        "id": _next_id(rows),
        "窗口编号": window_id,
        "数据流": stream,
        "窗口版本": window_version,
        "起始位点": start_position,
        "关闭位点": close_position,
        "日志起点": log_start_position if log_start_position is not None else start_position,
        "状态": status,
        "关闭时间": (closed_at or _now()) if status == "已关闭" else "",
        "策略编号": policy_id,
    }
    rows.append(row)
    return row


def find_window(stream: str, window_id: str, window_version: str | None = None) -> dict[str, Any] | None:
    for row in _rows(WINDOW_TABLE):
        if str(row.get("数据流", "")) != stream or str(row.get("窗口编号", "")) != window_id:
            continue
        if window_version and str(row.get("窗口版本", "")) != window_version:
            continue
        return row
    return None


# ---------------------------------------------------------------------------
# 迟到事件登记（幂等）
# ---------------------------------------------------------------------------

def _segment_key(stream: str, window_id: str, window_version: str, position: int, event_ref: str) -> str:
    return f"{stream}|{window_id}|{window_version}|{position}|{event_ref or 'pos'}"


def admit_event(
    *,
    stream: str,
    window_id: str,
    window_version: str,
    close_position: int,
    source_position: int,
    event_ref: str = "",
    record_summary: str = "",
    arrival_order: str = "A",
    log_start_position: int | None = None,
    next_window_id: str | None = None,
    next_window_version: str = "v1",
    next_close_position: int | None = None,
    policy_id: str = "",
) -> dict[str, Any]:
    """登记单条事件；同一 (流, 窗口版本, 源位点, 事件引用) 只生效一次。"""
    key = _segment_key(stream, window_id, window_version, source_position, event_ref)
    segments = _rows(SEGMENT_TABLE)
    for row in segments:
        if str(row.get("幂等键", "")) == key:
            return {
                "ok": True,
                "duplicate": True,
                "message": f"迟到记录 {key} 已登记，重复提交未重复生效",
                "segment": row,
                "compensation": _compensation_for_segment(str(row.get("分段编号", ""))),
                "admission": {
                    "源位点": row.get("源位点"),
                    "入窗阈值": row.get("入窗阈值"),
                    "归属": row.get("归属"),
                    "窗口编号": row.get("窗口编号"),
                    "窗口版本": row.get("窗口版本"),
                    "迟到处置": row.get("迟到处置"),
                    "判定依据": row.get("判定依据"),
                },
            }

    decision = classify_position(
        source_position=source_position,
        close_position=close_position,
        window_id=window_id,
        window_version=window_version,
        next_window_id=next_window_id,
        next_window_version=next_window_version,
        log_start_position=log_start_position,
    )
    own_close = (
        next_close_position if decision["归属"] == "下一窗口" and next_close_position is not None
        else close_position
    )
    if decision["归属"] == "下一窗口" and next_close_position is None:
        # 未给下一窗口位点时沿用原窗口宽度，避免出现关闭位点小于起始位点。
        origin = find_window(stream, window_id, window_version)
        if origin is not None:
            width = max(1, int(origin.get("关闭位点", close_position))
                        - int(origin.get("起始位点", 0)) + 1)
            own_close = close_position + width
    # 只补登记缺失的窗口；已有窗口的起始/关闭位点以显式登记为准，不能被逐条事件改写。
    if find_window(stream, decision["窗口编号"], decision["窗口版本"]) is None:
        ensure_window(
            stream=stream,
            window_id=decision["窗口编号"],
            window_version=decision["窗口版本"],
            start_position=(source_position if decision["归属"] == "原窗口" else close_position + 1),
            close_position=own_close,
            log_start_position=log_start_position if decision["归属"] == "原窗口" else close_position + 1,
            status="已关闭" if decision["归属"] == "原窗口" else "开放",
            policy_id=policy_id,
            closed_at=_now(),
        )

    segment = {
        "id": _next_id(segments),
        "分段编号": f"LSEG-{_next_id(segments):04d}",
        "数据流": stream,
        "窗口编号": decision["窗口编号"],
        "窗口版本": decision["窗口版本"],
        "源位点": source_position,
        "归属": decision["归属"],
        "迟到落点": decision["迟到落点"],
        "迟到处置": decision["迟到处置"],
        "界线侧": decision["界线侧"],
        "边界": decision["边界"],
        "分段类型": decision["分段类型"],
        "关闭位点": own_close,
        "判定界线位点": close_position,
        "入窗阈值": decision["入窗阈值"],
        "判定依据": decision["判定依据"],
        "可回放": decision["可回放"],
        "回放起点": log_start_position if log_start_position is not None else source_position,
        "到达顺序": f"顺序-{arrival_order}",
        "幂等键": key,
        "事件引用": event_ref,
        "记录摘要": record_summary or f"位点 {source_position} 事件",
        "回执编号": "",
        "补偿编号": "",
        "重放次数": 0,
        "创建时间": _now(),
        "status": "待复核" if decision["需要补偿"] else "正常可消费",
        "pending": decision["需要补偿"],
        "abnormal": decision["需要补偿"],
    }
    segments.append(segment)

    compensation = _create_compensation(segment) if decision["需要补偿"] else None

    return {
        "ok": True,
        "duplicate": False,
        "message": (
            f"位点 {source_position} {decision['归属']}窗口 {decision['窗口编号']}/"
            f"{decision['窗口版本']}，判定 {decision['分段类型']}"
        ),
        "segment": segment,
        "compensation": compensation,
        "admission": {
            "源位点": source_position,
            "入窗阈值": decision["入窗阈值"],
            "归属": decision["归属"],
            "窗口编号": decision["窗口编号"],
            "窗口版本": decision["窗口版本"],
            "迟到处置": decision["迟到处置"],
            "可回放": decision["可回放"],
            "判定依据": decision["判定依据"],
        },
    }


def _create_compensation(segment: dict[str, Any]) -> dict[str, Any]:
    rows = _rows(COMP_TABLE)
    # 补偿编号按源位点确定性生成：两种到达顺序下同一位点拿到同一个编号，
    # 回验脚本和人工复核都不会因为 ID 漂移而确认错行。
    position = int(segment.get("源位点", 0))
    candidate = f"COMP-{position:04d}"
    suffix = 2
    while any(str(row.get("补偿编号", "")) == candidate for row in rows):
        candidate = f"COMP-{position:04d}-{suffix}"
        suffix += 1
    compensation = {
        "id": _next_id(rows),
        "补偿编号": candidate,
        "分段编号": segment["分段编号"],
        "数据流": segment["数据流"],
        "窗口编号": segment["窗口编号"],
        "窗口版本": segment["窗口版本"],
        "源位点": segment["源位点"],
        "下游表": f"ods.{segment['数据流']}_iceberg",
        "交付状态": "中断待查",
        "交付时间": "",
        "中断标记": "",
        "回执编号": "",
        "确认状态": "待确认",
        "重放次数": 0,
        "status": "待确认",
        "pending": True,
        "abnormal": True,
    }
    rows.append(compensation)
    segment["补偿编号"] = compensation["补偿编号"]
    return compensation


def admit_batch(
    *,
    stream: str,
    window_id: str,
    window_version: str,
    close_position: int,
    positions: Iterable[int],
    arrival_order: str = "A",
    log_start_position: int | None = None,
    next_window_id: str | None = None,
    next_close_position: int | None = None,
    event_prefix: str = "evt",
    policy_id: str = "",
) -> dict[str, Any]:
    position_list = list(positions)
    ordered = list(reversed(position_list)) if arrival_order == "B" else list(position_list)
    results = [
        admit_event(
            stream=stream,
            window_id=window_id,
            window_version=window_version,
            close_position=close_position,
            source_position=position,
            event_ref=f"{event_prefix}-{position}",
            record_summary=f"{stream} 位点 {position} 事件",
            arrival_order=arrival_order,
            log_start_position=log_start_position,
            next_window_id=next_window_id,
            next_close_position=next_close_position,
            policy_id=policy_id,
        )
        for position in ordered
    ]
    return {
        "ok": True,
        "arrival_order": arrival_order,
        "close_position": close_position,
        "results": results,
        "accepted": [item["segment"] for item in results],
        "duplicates": [item["segment"] for item in results if item["duplicate"]],
        "admission": {
            "window_id": window_id,
            "window_version": window_version,
            "close_position": close_position,
            "threshold": f"源位点 <= {close_position} 归原窗口（含边界）",
            "inside": [position for position in position_list if position <= close_position],
            "outside": [position for position in position_list if position > close_position],
        },
    }


# ---------------------------------------------------------------------------
# 补偿行 / 回执
# ---------------------------------------------------------------------------

def _compensation_for_segment(segment_id: str) -> dict[str, Any] | None:
    for row in _rows(COMP_TABLE):
        if str(row.get("分段编号", "")) == segment_id:
            return row
    return None


def _segment_for(segment_id: str) -> dict[str, Any] | None:
    for row in _rows(SEGMENT_TABLE):
        if str(row.get("分段编号", "")) == segment_id:
            return row
    return None


def find_compensation(identifier: Any) -> dict[str, Any] | None:
    return _resolve(identifier, COMP_TABLE, "补偿编号")


def list_compensations(
    *,
    stream: str | None = None,
    window_id: str | None = None,
    window_version: str | None = None,
    delivery_status: str | None = None,
    confirm_status: str | None = None,
    interrupted_only: bool = False,
) -> list[dict[str, Any]]:
    rows = list(_rows(COMP_TABLE))
    if stream:
        rows = [row for row in rows if row.get("数据流") == stream]
    if window_id:
        rows = [row for row in rows if row.get("窗口编号") == window_id]
    if window_version:
        rows = [row for row in rows if row.get("窗口版本") == window_version]
    if delivery_status:
        rows = [row for row in rows if row.get("交付状态") == delivery_status]
    if confirm_status:
        rows = [row for row in rows if row.get("确认状态") == confirm_status]
    if interrupted_only:
        rows = [row for row in rows if row.get("中断标记")]
    return rows


def deliver_compensation(
    identifier: Any,
    *,
    interrupted: bool = False,
    receipt_id: str = "",
) -> dict[str, Any] | None:
    """把补偿行交给下游；重复调用只更新交付时间，不重复计数。"""
    row = find_compensation(identifier)
    if row is None:
        return None
    if not row.get("交付时间"):
        row["交付时间"] = _now()
    row["交付状态"] = "已交付下游"
    if interrupted:
        row["中断标记"] = "中断期间交付"
    segment = _segment_for(str(row.get("分段编号", "")))
    if receipt_id:
        row["回执编号"] = receipt_id
        row["确认状态"] = "已确认"
        row["status"] = "已确认"
        row["pending"] = False
        row["abnormal"] = False
        if segment is not None:
            segment["回执编号"] = receipt_id
            segment["status"] = "已确认"
            segment["pending"] = False
            segment["abnormal"] = False
    return row


def confirm_compensation(identifier: Any) -> dict[str, Any] | None:
    row = find_compensation(identifier)
    if row is None:
        return None
    segment = _segment_for(str(row.get("分段编号", "")))
    if row.get("确认状态") == "已确认":
        return {
            "ok": True,
            "duplicate": True,
            "message": f"补偿 {row.get('补偿编号')} 已确认，重复提交未重复生效",
            "compensation": row,
            "segment": segment,
            "commit": compute_commit(str(row.get("数据流", ""))),
        }
    row["确认状态"] = "已确认"
    row["status"] = "已确认"
    row["pending"] = False
    row["abnormal"] = False
    row["确认时间"] = _now()
    if segment is not None and segment.get("status") != "已消费":
        segment["status"] = "已确认"
        segment["pending"] = False
        segment["abnormal"] = False
    commit = compute_commit(str(row.get("数据流", "")))
    return {
        "ok": True,
        "duplicate": False,
        "message": f"补偿 {row.get('补偿编号')} 已确认，分段可进入回放",
        "compensation": row,
        "segment": segment,
        "commit": commit,
    }


def register_receipt(
    *,
    stream: str,
    window_id: str,
    window_version: str,
    cover_start: int,
    cover_end: int,
    result: str = "成功",
    source: str = "下游提交回执",
) -> dict[str, Any]:
    rows = _rows(RECEIPT_TABLE)
    for row in rows:
        if (
            row.get("数据流") == stream
            and row.get("窗口编号") == window_id
            and row.get("窗口版本") == window_version
            and int(row.get("覆盖起始", -1)) == cover_start
            and int(row.get("覆盖截止", -1)) == cover_end
        ):
            return row
    row = {
        "id": _next_id(rows),
        "回执编号": f"RCPT-{_next_id(rows):04d}",
        "数据流": stream,
        "窗口编号": window_id,
        "窗口版本": window_version,
        "覆盖起始": cover_start,
        "覆盖截止": cover_end,
        "位点区间": f"{cover_start}-{cover_end}",
        "结果": result,
        "来源": source,
        "时间": _now(),
    }
    rows.append(row)
    return row


def list_receipts(
    *,
    stream: str | None = None,
    window_id: str | None = None,
    window_version: str | None = None,
) -> list[dict[str, Any]]:
    rows = list(_rows(RECEIPT_TABLE))
    if stream:
        rows = [row for row in rows if row.get("数据流") == stream]
    if window_id:
        rows = [row for row in rows if row.get("窗口编号") == window_id]
    if window_version:
        rows = [row for row in rows if row.get("窗口版本") == window_version]
    return rows


def latest_receipt(stream: str, window_id: str, window_version: str) -> dict[str, Any] | None:
    rows = [
        row for row in _rows(RECEIPT_TABLE)
        if row.get("数据流") == stream
        and row.get("窗口编号") == window_id
        and row.get("窗口版本") == window_version
        and row.get("结果") == "成功"
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: int(row.get("覆盖截止", 0)))


def receipt_covers(position: int, stream: str, window_id: str, window_version: str) -> dict[str, Any] | None:
    for row in _rows(RECEIPT_TABLE):
        if (
            row.get("数据流") == stream
            and row.get("窗口编号") == window_id
            and row.get("窗口版本") == window_version
            and row.get("结果") == "成功"
            and int(row.get("覆盖起始", 0)) <= position <= int(row.get("覆盖截止", -1))
        ):
            return row
    return None


# ---------------------------------------------------------------------------
# 提交进度：只用分段状态推导可确认区间
# ---------------------------------------------------------------------------

def compute_commit(stream: str, window_id: str | None = None,
                   window_version: str | None = None) -> dict[str, Any]:
    windows = [
        row for row in _rows(WINDOW_TABLE)
        if row.get("数据流") == stream
        and row.get("状态") == "已关闭"
        and (window_id is None or row.get("窗口编号") == window_id)
        and (window_version is None or row.get("窗口版本") == window_version)
    ]
    if not windows:
        return {"ok": False, "stream": stream, "message": "没有已关闭窗口可计算提交进度", "windows": []}

    results: list[dict[str, Any]] = []
    for window in windows:
        wid, version = str(window["窗口编号"]), str(window["窗口版本"])
        close = int(window.get("关闭位点", 0))
        segments = [
            row for row in _rows(SEGMENT_TABLE)
            if row.get("数据流") == stream
            and row.get("窗口编号") == wid
            and row.get("窗口版本") == version
        ]
        segments.sort(key=lambda row: int(row.get("源位点", 0)))

        receipt = latest_receipt(stream, wid, version)
        committed = int(receipt.get("覆盖截止", 0)) if receipt else int(window.get("起始位点", 0)) - 1
        progress_row = _progress_row(stream, wid, version)
        if progress_row is not None:
            committed = max(committed, int(progress_row.get("已提交位点", 0)))

        upper = committed
        blocked: dict[str, Any] | None = None
        basis: list[str] = []
        for segment in segments:
            position = int(segment.get("源位点", 0))
            if position <= committed or position > close:
                continue
            state = str(segment.get("status", ""))
            if state in PENDING_STATES or state not in CONFIRMED_STATES:
                blocked = segment
                break
            upper = position
            basis.append(str(segment.get("分段编号", "")))

        actual = min(upper, close)
        status = "待复核阻塞" if blocked is not None else (
            "窗口可提交" if actual >= close else "推进中"
        )
        row = _write_progress(
            stream=stream, window_id=wid, window_version=version,
            committed=committed, upper=actual, blocked=blocked, basis=basis,
            status=status, receipt=receipt,
        )
        results.append({
            **row,
            "窗口关闭位点": close,
            "窗口状态": window.get("状态"),
            "依据回执": receipt.get("回执编号") if receipt else "",
            "分段数据": [
                {
                    "分段编号": segment.get("分段编号"),
                    "源位点": segment.get("源位点"),
                    "窗口版本": segment.get("窗口版本"),
                    "状态": segment.get("status"),
                }
                for segment in segments
            ],
        })

    primary = results[0]
    return {
        "ok": True,
        "stream": stream,
        "windows": results,
        "actual_commit_position": primary.get("实际提交位置"),
        "确认区间": f"{primary.get('已提交位点')}-{primary.get('实际提交位置')}",
        "依据分段": primary.get("依据分段"),
        "阻塞分段": primary.get("阻塞分段"),
        "状态": primary.get("status"),
    }


def _progress_row(stream: str, window_id: str, window_version: str) -> dict[str, Any] | None:
    for row in _rows(PROGRESS_TABLE):
        if (
            row.get("数据流") == stream
            and row.get("窗口编号") == window_id
            and row.get("窗口版本") == window_version
        ):
            return row
    return None


def _write_progress(
    *,
    stream: str,
    window_id: str,
    window_version: str,
    committed: int,
    upper: int,
    blocked: dict[str, Any] | None,
    basis: list[str],
    status: str,
    receipt: dict[str, Any] | None,
) -> dict[str, Any]:
    rows = _rows(PROGRESS_TABLE)
    row = _progress_row(stream, window_id, window_version)
    if row is None:
        row = {
            "id": _next_id(rows),
            "数据流": stream,
            "窗口编号": window_id,
            "窗口版本": window_version,
            "status": status,
            "pending": status == "待复核阻塞",
            "abnormal": status == "待复核阻塞",
        }
        rows.append(row)
    row.update({
        "已提交位点": committed,
        "可确认上界": upper,
        "实际提交位置": upper,
        "确认区间": f"{committed}-{upper}",
        "阻塞分段": str(blocked.get("分段编号", "")) if blocked else "",
        "阻塞源位点": int(blocked.get("源位点", 0)) if blocked else "",
        "依据分段": "、".join(basis),
        "依据回执": str(receipt.get("回执编号", "")) if receipt else "",
        "状态说明": (
            f"被待复核分段 {blocked.get('分段编号')}（位点 {blocked.get('源位点')}）阻塞"
            if blocked else "分段均已确认，可推进到关闭位点"
        ),
        "status": status,
        "pending": status == "待复核阻塞",
        "abnormal": status == "待复核阻塞",
        "更新时间": _now(),
    })
    return row


# ---------------------------------------------------------------------------
# 回放（按源位点幂等）
# ---------------------------------------------------------------------------

def _sync_checkpoints(stream: str, commit: dict[str, Any]) -> None:
    """把分段推导出的提交位置回写到该流的迟到回放检查点。"""
    actual = commit.get("actual_commit_position")
    for row in store.rows("checkpoint"):
        if row.get("数据流") != stream or row.get("场景") != "迟到事件边界回放":
            continue
        row["实际提交位置"] = str(actual if actual is not None else "")
        row["确认区间"] = str(commit.get("确认区间") or "")


def replay(
    *,
    stream: str,
    from_position: int,
    window_id: str | None = None,
    window_version: str | None = None,
) -> dict[str, Any]:
    """从旧日志位点发起回放；只回放原窗口范围内、源位点 >= from_position 的分段。"""
    windows = [
        row for row in _rows(WINDOW_TABLE)
        if row.get("数据流") == stream
        and row.get("状态") == "已关闭"
        and (window_id is None or row.get("窗口编号") == window_id)
        and (window_version is None or row.get("窗口版本") == window_version)
    ]
    if not windows:
        return {"ok": False, "message": "找不到可回放的已关闭窗口", "stream": stream}
    covering = [
        row for row in windows
        if int(row.get("起始位点", 0)) <= from_position <= int(row.get("关闭位点", 0))
    ]
    window = covering[0] if covering else sorted(
        windows, key=lambda row: int(row.get("关闭位点", 0)))[0]
    wid, version, close = str(window["窗口编号"]), str(window["窗口版本"]), int(window["关闭位点"])
    log_start = int(window.get("日志起点", int(window.get("起始位点", 0))))

    if from_position < log_start:
        return {
            "ok": False,
            "stream": stream,
            "window": {"窗口编号": wid, "窗口版本": version, "关闭位点": close},
            "message": f"起点 {from_position} 早于该窗口日志起点 {log_start}，旧日志不可回放",
        }
    if from_position > close:
        return {
            "ok": False,
            "stream": stream,
            "window": {"窗口编号": wid, "窗口版本": version, "关闭位点": close},
            "message": f"起点 {from_position} 晚于关闭位点 {close}，不属于原窗口回放范围",
        }

    scope = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream
        and row.get("窗口编号") == wid
        and row.get("窗口版本") == version
        and from_position <= int(row.get("源位点", 0)) <= close
    ]
    scope.sort(key=lambda row: int(row.get("源位点", 0)))

    consumable: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    delivered: list[dict[str, Any]] = []

    for segment in scope:
        position = int(segment.get("源位点", 0))
        state = str(segment.get("status", ""))
        summary = {
            "分段编号": segment.get("分段编号"),
            "源位点": position,
            "窗口编号": wid,
            "窗口版本": version,
            "归属": segment.get("归属"),
            "状态": state,
        }
        if state in PENDING_STATES or state not in CONFIRMED_STATES:
            if state == "已跳过":
                skipped.append(summary)
            else:
                pending.append(summary)
            continue

        consumable.append(summary)
        receipt = receipt_covers(position, stream, wid, version)
        already_effective = state == "已消费" or bool(segment.get("回执编号")) or receipt is not None
        if already_effective:
            duplicates.append({**summary, "原因": "成功回执已覆盖或此前已消费"})
            if state != "已消费":
                segment["status"] = "已消费"
                segment["pending"] = False
            continue

        segment["status"] = "已消费"
        segment["pending"] = False
        segment["abnormal"] = False
        segment["重放次数"] = int(segment.get("重放次数", 0)) + 1
        compensation = _compensation_for_segment(str(segment.get("分段编号", "")))
        if compensation is not None:
            compensation["交付状态"] = "已交付下游"
            compensation["交付时间"] = compensation.get("交付时间") or _now()
            compensation["重放次数"] = int(compensation.get("重放次数", 0)) + 1
        delivered.append({**summary, "状态": "已消费"})

    next_windows = [
        {
            "窗口编号": row.get("窗口编号"),
            "窗口版本": row.get("窗口版本"),
            "关闭位点": row.get("关闭位点"),
            "状态": row.get("状态"),
        }
        for row in _rows(WINDOW_TABLE)
        if row.get("数据流") == stream
        and row.get("窗口编号") != wid
        and row.get("状态") == "开放"
    ]
    next_records = [
        {
            "分段编号": row.get("分段编号"),
            "源位点": row.get("源位点"),
            "窗口编号": row.get("窗口编号"),
            "窗口版本": row.get("窗口版本"),
            "状态": row.get("status"),
        }
        for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream
        and row.get("窗口编号") != wid
        and row.get("归属") == "下一窗口"
    ]

    commit = compute_commit(stream, wid, version)
    # 回放完成后同步给受影响窗口关联的检查点，保证续跑口径不落后于分段。
    _sync_checkpoints(stream, commit)
    run_rows = _rows(REPLAY_TABLE)
    run = {
        "id": _next_id(run_rows),
        "回放编号": f"LRPL-{_next_id(run_rows):04d}",
        "数据流": stream,
        "窗口编号": wid,
        "窗口版本": version,
        "起始位点": from_position,
        "结束位点": close,
        "可消费记录数": len(consumable),
        "重复跳过数": len(duplicates),
        "待复核分段数": len(pending),
        "实际提交位置": commit.get("actual_commit_position"),
        "结果": "受阻-存在待复核分段" if pending else "已完成",
        "时间": _now(),
    }
    run_rows.append(run)

    return {
        "ok": not pending,
        "stream": stream,
        "from_position": from_position,
        "to_position": close,
        "log_start_position": log_start,
        "window": {
            "窗口编号": wid,
            "窗口版本": version,
            "关闭位点": close,
            "窗口状态": window.get("状态"),
        },
        "window_versions": [
            {
                "源位点": int(row.get("源位点", 0)),
                "窗口编号": row.get("窗口编号"),
                "窗口版本": row.get("窗口版本"),
                "归属": row.get("归属"),
                "分段类型": row.get("分段类型"),
                "状态": row.get("status"),
            }
            for row in scope
        ],
        "consumable_records": consumable,
        "duplicate_records": duplicates,
        "newly_delivered": delivered,
        "pending_review_segments": pending,
        "skipped_segments": skipped,
        "next_window_records": next_records,
        "next_windows": next_windows,
        "compensations": [
            {
                "补偿编号": row.get("补偿编号"),
                "源位点": row.get("源位点"),
                "窗口版本": row.get("窗口版本"),
                "交付状态": row.get("交付状态"),
                "确认状态": row.get("确认状态"),
                "中断标记": row.get("中断标记"),
            }
            for row in _rows(COMP_TABLE)
            if row.get("数据流") == stream and row.get("窗口编号") == wid
            and row.get("窗口版本") == version
        ],
        "commit": commit,
        "actual_commit_position": commit.get("actual_commit_position"),
        "replay_run": run,
    }


# ---------------------------------------------------------------------------
# 受影响窗口 / 续跑关联
# ---------------------------------------------------------------------------

def affected_windows(stream: str | None = None) -> list[dict[str, Any]]:
    rows = list(_rows(AFFECTED_TABLE))
    if stream:
        rows = [row for row in rows if row.get("数据流") == stream]
    return rows


def link_affected_windows(
    *,
    stream: str,
    checkpoint_id: str,
    replay_start: int,
) -> list[dict[str, Any]]:
    """续跑：把受影响窗口和已有成功回执关联起来（幂等）。"""
    linked: list[dict[str, Any]] = []
    rows = _rows(AFFECTED_TABLE)
    for window in _rows(WINDOW_TABLE):
        if window.get("数据流") != stream or window.get("状态") != "已关闭":
            continue
        wid, version = str(window["窗口编号"]), str(window["窗口版本"])
        receipt = latest_receipt(stream, wid, version)
        segments = [
            row for row in _rows(SEGMENT_TABLE)
            if row.get("数据流") == stream
            and row.get("窗口编号") == wid
            and row.get("窗口版本") == version
        ]
        pending = [str(row.get("分段编号")) for row in segments if row.get("status") in PENDING_STATES]
        existing = next(
            (
                row for row in rows
                if row.get("数据流") == stream
                and row.get("窗口编号") == wid
                and row.get("窗口版本") == version
            ),
            None,
        )
        basis = (
            f"成功回执 {receipt['回执编号']} 覆盖 {receipt['位点区间']}，"
            f"从位点 {replay_start} 增量回放至关闭位点 {window.get('关闭位点')}"
            if receipt else f"无成功回执，只能从位点 {replay_start} 做补偿回放"
        )
        payload = {
            "检查点编号": checkpoint_id,
            "数据流": stream,
            "窗口编号": wid,
            "窗口版本": version,
            "关闭位点": int(window.get("关闭位点", 0)),
            "回放起点": replay_start,
            "受影响分段": "、".join(pending) if pending else "、".join(
                str(row.get("分段编号")) for row in segments
            ),
            "待复核数": len(pending),
            "回执编号": receipt.get("回执编号") if receipt else "",
            "回执区间": receipt.get("位点区间") if receipt else "",
            "回放依据": basis,
            "关联状态": "已关联成功回执" if receipt else "无回执-需人工确认",
        }
        if existing is None:
            payload.update({
                "id": _next_id(rows),
                "status": payload["关联状态"],
                "pending": True,
                "abnormal": not bool(receipt),
            })
            rows.append(payload)
            existing = payload
        else:
            existing.update(payload)
            existing["status"] = payload["关联状态"]
        linked.append(existing)
    return linked


def _sync_watermark(stream: str, actual: int, committed: int, window_id: str, version: str) -> None:
    """把分段推导出的可确认区间写回水位模块，便于延期监控看到真实提交位置。"""
    for row in store.rows("watermark_mgr"):
        if str(row.get("数据流", "")) != stream:
            continue
        previous = str(row.get("提交位点", ""))
        if previous != str(actual):
            row["上一位点"] = previous
        row["当前位点"] = str(actual)
        row["提交位点"] = str(actual)
        row["提交窗口"] = window_id
        row["窗口版本"] = version
        row["确认区间"] = f"{committed}-{actual}"
        row["位点口径"] = "迟到事件分段"
        return


def resume(
    *,
    stream: str | None = None,
    checkpoint_id: str | None = None,
) -> dict[str, Any]:
    if stream is None:
        streams = sorted({str(row.get("数据流", "")) for row in _rows(AFFECTED_TABLE)})
        stream = streams[0] if streams else ""
    if not stream:
        return {"ok": False, "message": "没有可续跑的受影响窗口"}

    checkpoint: dict[str, Any] | None = None
    if checkpoint_id:
        for row in store.rows("checkpoint"):
            if str(row.get("检查点编号", "")) == checkpoint_id:
                checkpoint = row
                break
    if checkpoint is None:
        candidates = [
            row for row in store.rows("checkpoint")
            if row.get("数据流") == stream and row.get("场景") == "迟到事件边界回放"
        ]
        checkpoint = candidates[-1] if candidates else None
    cp_id = checkpoint_id or (str(checkpoint.get("检查点编号", "")) if checkpoint else "")
    replay_start = _to_int(checkpoint.get("保存位点", 0)) if checkpoint else 0
    if replay_start <= 0:
        segments = [int(row.get("源位点", 0)) for row in _rows(SEGMENT_TABLE)
                    if row.get("数据流") == stream]
        replay_start = min(segments) if segments else min(
            (int(row.get("日志起点", 0)) for row in _rows(WINDOW_TABLE)
             if row.get("数据流") == stream), default=0)

    linked = link_affected_windows(stream=stream, checkpoint_id=cp_id, replay_start=replay_start)
    if checkpoint is not None:
        checkpoint["关联窗口"] = "、".join(
            f"{row.get('窗口编号')}/{row.get('窗口版本')}" for row in linked)
        checkpoint["续跑回放起点"] = str(replay_start)
        checkpoint["回执关联"] = "、".join(
            str(row.get("回执编号")) for row in linked if row.get("回执编号"))

    pending_compensations = [
        row for row in _rows(COMP_TABLE)
        if row.get("数据流") == stream and row.get("确认状态") == "待确认"
    ]
    commit = compute_commit(stream)
    if checkpoint is not None:
        checkpoint["实际提交位置"] = str(commit.get("actual_commit_position") or "")
        checkpoint["确认区间"] = str(commit.get("确认区间") or "")
    pending_segments = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream and row.get("status") in PENDING_STATES
    ]
    return {
        "ok": True,
        "stream": stream,
        "checkpoint": checkpoint,
        "checkpoint_id": cp_id,
        "replay_start": replay_start,
        "affected_windows": linked,
        "pending_compensations": pending_compensations,
        "pending_segments": pending_segments,
        "commit": commit,
        "plan": {
            "依据": "受影响窗口 + 已有成功回执",
            "回放安排": f"从位点 {replay_start} 回放至受影响窗口关闭位点",
            "窗口版本": [
                {
                    "窗口编号": row.get("窗口编号"),
                    "窗口版本": row.get("窗口版本"),
                    "回执编号": row.get("回执编号"),
                    "回放起点": row.get("回放起点"),
                }
                for row in linked
            ],
        },
    }


# ---------------------------------------------------------------------------
# 场景：关闭位点 120，事件 119/120/121，两种到达顺序
# ---------------------------------------------------------------------------

SCENARIO_STREAM = "order_events_cdc"
SCENARIO_WINDOW = "W-0001"
SCENARIO_CLOSE = 120
SCENARIO_POSITIONS = (119, 120, 121)
SCENARIO_LOG_START = 119


def _reset_scenario(stream: str) -> None:
    for table in (WINDOW_TABLE, SEGMENT_TABLE, COMP_TABLE, RECEIPT_TABLE,
                  PROGRESS_TABLE, AFFECTED_TABLE, REPLAY_TABLE):
        rows = _rows(table)
        rows[:] = [row for row in rows if row.get("数据流") != stream]
    checkpoints = store.rows("checkpoint")
    checkpoints[:] = [row for row in checkpoints if row.get("场景") != "迟到事件边界回放"]


def setup_closed_boundary_scenario(
    *,
    arrival_order: str = "A",
    reset: bool = True,
    stream: str = SCENARIO_STREAM,
    window_id: str = SCENARIO_WINDOW,
    window_version: str = "v1",
    close_position: int = SCENARIO_CLOSE,
    positions: Iterable[int] = SCENARIO_POSITIONS,
    log_start_position: int = SCENARIO_LOG_START,
    policy_id: str = "OOO-0001",
) -> dict[str, Any]:
    """搭建可回验场景：关闭位点 120，119/120 归原窗口，121 归下一窗口。

    - 119 在中断前已成功交付下游，并有成功回执；
    - 120 在中断期间已交给下游，但缺回执，处于待复核；
    - 121 正常进入下一窗口，不能被丢行。
    调用方先确认 120 的补偿，再从位点 119 回放。
    """
    if reset:
        _reset_scenario(stream)
    position_list = list(positions)
    start = min(position_list) - 1 if position_list else 0
    ensure_window(
        stream=stream, window_id=window_id, window_version=window_version,
        start_position=start, close_position=close_position, status="已关闭",
        log_start_position=log_start_position, policy_id=policy_id,
        closed_at="2026-11-22 09:00:00",
    )
    next_window_id = _next_window_id(window_id)
    next_close = close_position + (max(position_list) - close_position if position_list else 1)
    ensure_window(
        stream=stream, window_id=next_window_id, window_version=window_version,
        start_position=close_position + 1, close_position=max(next_close, close_position + 1),
        log_start_position=close_position + 1, status="开放", policy_id=policy_id,
    )
    batch = admit_batch(
        stream=stream,
        window_id=window_id,
        window_version=window_version,
        close_position=close_position,
        positions=position_list,
        arrival_order=arrival_order,
        log_start_position=log_start_position,
        next_window_id=next_window_id,
        next_close_position=max(next_close, close_position + 1),
        event_prefix="evt",
        policy_id=policy_id,
    )

    # 中断前/中断中的交付状态：位点小于关闭位点的分段已成功回执，等于关闭位点的分段
    # 在中断期间交给了下游但没有回执。
    inner = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream and row.get("窗口编号") == window_id
        and row.get("窗口版本") == window_version
    ]
    for segment in sorted(inner, key=lambda row: int(row.get("源位点", 0))):
        position = int(segment.get("源位点", 0))
        compensation = _compensation_for_segment(str(segment.get("分段编号", "")))
        if compensation is None:
            continue
        if position < close_position:
            receipt = register_receipt(
                stream=stream, window_id=window_id, window_version=window_version,
                cover_start=max(start, log_start_position), cover_end=position,
                source="中断前下游成功回执",
            )
            deliver_compensation(
                compensation["补偿编号"], interrupted=False, receipt_id=receipt["回执编号"])
        elif position == close_position:
            deliver_compensation(compensation["补偿编号"], interrupted=True)

    cp_rows = store.rows("checkpoint")
    checkpoint = {
        "id": _next_id(cp_rows),
        "检查点编号": f"CHEC-LATE-{_next_id(cp_rows):04d}",
        "数据流": stream,
        "保存位点": str(log_start_position),
        "保存时间": "2026-11-22 09:10:00",
        "恢复状态": "中断-待续跑",
        "丢失行数": 0,
        "回放代价": f"增量回放位点 {log_start_position}-{close_position}",
        "恢复耗时": "预估2s",
        "场景": "迟到事件边界回放",
        "status": "恢复中",
        "pending": True,
        "abnormal": True,
    }
    cp_rows.append(checkpoint)
    # 续跑关联：把受影响窗口与已有成功回执写回检查点，回验时能直接看到依据。
    resume(stream=stream, checkpoint_id=checkpoint["检查点编号"])

    progress = compute_commit(stream, window_id, window_version)
    _sync_watermark(stream, int(progress.get("actual_commit_position") or 0),
                    int(str(progress.get("确认区间", "0-0")).split("-")[0] or 0),
                    window_id, window_version)
    return {
        "ok": True,
        "scenario": "关闭位点 120 的迟到事件边界",
        "arrival_order": arrival_order,
        "batch": batch,
        "state": state(stream=stream),
        "before_confirmation": {
            "pending_review_segments": progress.get("阻塞分段"),
            "actual_commit_position": progress.get("actual_commit_position"),
            "note": "未确认 120 的补偿前，回放会受阻在 119。",
        },
        "next_step": [
            "POST /api/late-window/compensations/<补偿编号>/confirm 确认 120 的补偿",
            f"POST /api/late-window/replay 从位点 {log_start_position} 发起回放",
        ],
    }


# ---------------------------------------------------------------------------
# 可回验校验
# ---------------------------------------------------------------------------

def verify(
    *,
    stream: str = SCENARIO_STREAM,
    window_id: str = SCENARIO_WINDOW,
    window_version: str = "v1",
    close_position: int = SCENARIO_CLOSE,
    expected_positions: Iterable[int] = SCENARIO_POSITIONS,
) -> dict[str, Any]:
    """对当前账本重新计算一遍，校验入窗阈值、幂等、丢行和提交区间。"""
    positions = list(expected_positions)
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"检查项": name, "通过": passed, "明细": detail})

    window = find_window(stream, window_id, window_version)
    segments = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream
        and row.get("窗口编号") == window_id
        and row.get("窗口版本") == window_version
    ]
    # 入窗判定要覆盖界线两侧：原窗口分段 + 本窗口判定后进入下一窗口的分段。
    judge_positions = {int(row.get("判定界线位点", -1)) for row in segments}
    boundary_segments = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream
        and (
            (row.get("窗口编号") == window_id and row.get("窗口版本") == window_version)
            or (
                row.get("归属") == "下一窗口"
                and int(row.get("判定界线位点", -1)) in judge_positions
            )
        )
    ]
    by_position = {int(row.get("源位点", 0)): row for row in boundary_segments}

    boundary = []
    boundary_ok = True
    for position in positions:
        expected = classify_position(
            source_position=position, close_position=close_position,
            window_id=window_id, window_version=window_version,
        )
        actual = by_position.get(position)
        effective_version = (
            window_version if expected["归属"] == "原窗口"
            else str(actual.get("窗口版本", "")) if actual else ""
        )
        ok = (
            actual is not None
            and actual.get("归属") == expected["归属"]
            and actual.get("窗口版本") == effective_version
        )
        boundary_ok = boundary_ok and ok
        boundary.append({
            "源位点": position,
            "期望归属": expected["归属"],
            "实际归属": actual.get("归属") if actual else None,
            "窗口版本": actual.get("窗口版本") if actual else None,
            "通过": ok,
        })
    add(
        "入窗阈值(关闭位点含边界)",
        boundary_ok,
        {"关闭位点": close_position, "判定": boundary,
         "规则": f"<= {close_position} 归原窗口，> {close_position} 进入下一窗口"},
    )

    # 幂等：原样重投每条已登记记录，不得新增分段或补偿行。
    before_segments = len(_rows(SEGMENT_TABLE))
    before_comps = len(_rows(COMP_TABLE))
    duplicate_hits = []
    for row in segments:
        result = admit_event(
            stream=stream,
            window_id=window_id,
            window_version=window_version,
            close_position=close_position,
            source_position=int(row.get("源位点", 0)),
            event_ref=str(row.get("事件引用", "")),
        )
        duplicate_hits.append({
            "源位点": row.get("源位点"),
            "duplicate": result.get("duplicate"),
            "message": result.get("message"),
        })
    add(
        "迟到记录重复提交只生效一次",
        (
            all(hit["duplicate"] for hit in duplicate_hits)
            and len(_rows(SEGMENT_TABLE)) == before_segments
            and len(_rows(COMP_TABLE)) == before_comps
        ),
        {"重投结果": duplicate_hits,
         "分段数": len(_rows(SEGMENT_TABLE)), "补偿行数": len(_rows(COMP_TABLE))},
    )

    add(
        "窗口版本与源位点可追溯",
        bool(segments) and all(
            row.get("窗口版本") and row.get("源位点") is not None
            and row.get("数据流") == stream
            for row in _rows(SEGMENT_TABLE) if row.get("数据流") == stream
        ),
        [
            {
                "分段编号": row.get("分段编号"),
                "窗口编号": row.get("窗口编号"),
                "窗口版本": row.get("窗口版本"),
                "源位点": row.get("源位点"),
                "归属": row.get("归属"),
            }
            for row in segments
        ],
    )

    next_records = [
        row for row in _rows(SEGMENT_TABLE)
        if row.get("数据流") == stream and row.get("归属") == "下一窗口"
    ]
    expected_outside = [position for position in positions if position > close_position]
    add(
        "正常窗口不丢行",
        len(next_records) == len(expected_outside)
        and all(row.get("status") in ("正常可消费", "已确认", "已消费") for row in next_records),
        [
            {
                "源位点": row.get("源位点"),
                "窗口编号": row.get("窗口编号"),
                "窗口版本": row.get("窗口版本"),
                "状态": row.get("status"),
            }
            for row in next_records
        ],
    )

    at_risk = [
        row for row in segments if row.get("status") in PENDING_STATES
    ]
    compensation_ok = True
    compensation_detail = []
    for row in segments:
        if int(row.get("源位点", 0)) > close_position:
            continue
        compensation = _compensation_for_segment(str(row.get("分段编号", "")))
        compensation_ok = compensation_ok and compensation is not None
        compensation_detail.append({
            "分段编号": row.get("分段编号"),
            "源位点": row.get("源位点"),
            "补偿编号": compensation.get("补偿编号") if compensation else None,
            "交付状态": compensation.get("交付状态") if compensation else None,
            "中断标记": compensation.get("中断标记") if compensation else None,
        })
    add("原窗口迟到事件未直接丢弃", compensation_ok, compensation_detail)

    commit = compute_commit(stream, window_id, window_version)
    actual = commit.get("actual_commit_position")
    pending_positions = [int(row.get("源位点", 0)) for row in at_risk]
    progress_ok = (
        actual is not None
        and int(actual) <= close_position
        and (not pending_positions or int(actual) < min(pending_positions))
    )
    add(
        "提交区间不越过待复核分段",
        progress_ok,
        {
            "实际提交位置": actual,
            "确认区间": commit.get("确认区间"),
            "待复核源位点": pending_positions,
            "阻塞分段": commit.get("阻塞分段"),
        },
    )

    versions = {
        str(row.get("源位点")): {
            "窗口编号": row.get("窗口编号"), "窗口版本": row.get("窗口版本"),
        }
        for row in segments
    }
    add("窗口版本快照", bool(window) and bool(versions), {
        "窗口": {
            "窗口编号": window.get("窗口编号") if window else None,
            "窗口版本": window.get("窗口版本") if window else None,
            "关闭位点": window.get("关闭位点") if window else None,
            "日志起点": window.get("日志起点") if window else None,
        },
        "各源位点归属": versions,
    })

    return {
        "ok": all(check["通过"] for check in checks),
        "stream": stream,
        "window": window_id,
        "window_version": window_version,
        "close_position": close_position,
        "checks": checks,
        "state": {
            "actual_commit_position": actual,
            "commit_interval": commit.get("确认区间"),
            "pending_review_segments": [
                {"分段编号": row.get("分段编号"), "源位点": row.get("源位点")}
                for row in at_risk
            ],
        },
    }


def state(*, stream: str | None = None) -> dict[str, Any]:
    def pick(table: str) -> list[dict[str, Any]]:
        rows = list(_rows(table))
        if stream:
            rows = [row for row in rows if row.get("数据流") == stream]
        return rows

    windows = pick(WINDOW_TABLE)
    segments = pick(SEGMENT_TABLE)
    compensations = pick(COMP_TABLE)
    receipts = pick(RECEIPT_TABLE)
    progress = pick(PROGRESS_TABLE)
    checkpoints = [
        row for row in store.rows("checkpoint")
        if row.get("场景") == "迟到事件边界回放"
        and (stream is None or row.get("数据流") == stream)
    ]
    return {
        "ok": True,
        "stream": stream,
        "windows": windows,
        "segments": segments,
        "compensations": compensations,
        "receipts": receipts,
        "commit_progress": progress,
        "checkpoints": checkpoints,
        "affected_windows": pick(AFFECTED_TABLE),
        "replay_runs": pick(REPLAY_TABLE),
        "counts": {
            "windows": len(windows),
            "segments": len(segments),
            "pending_segments": sum(1 for row in segments if row.get("status") in PENDING_STATES),
            "pending_compensations": sum(
                1 for row in compensations if row.get("确认状态") == "待确认"),
            "interrupted_compensations": sum(1 for row in compensations if row.get("中断标记")),
            "next_window_records": sum(1 for row in segments if row.get("归属") == "下一窗口"),
        },
    }

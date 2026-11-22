"""迟到事件分段接口：入窗判定、补偿确认、回放、续跑和可回验校验。

这些端点读写引擎账本（不并入 22 个业务模块的资源列表），
供跨过已关闭窗口的迟到事件做边界回验和断点续跑。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import late_window

router = APIRouter(prefix="/api/late-window", tags=["迟到事件分段"])


class AdmitPayload(BaseModel):
    """单条迟到事件入窗登记。"""

    stream: str = Field(default=late_window.SCENARIO_STREAM)
    window_id: str = Field(default=late_window.SCENARIO_WINDOW)
    window_version: str = Field(default="v1")
    close_position: int = Field(default=late_window.SCENARIO_CLOSE, ge=0)
    source_position: int = Field(ge=0)
    event_ref: str = ""
    record_summary: str = ""
    arrival_order: str = "A"
    log_start_position: int | None = None
    next_window_id: str | None = None
    next_close_position: int | None = None


class BatchPayload(BaseModel):
    stream: str = Field(default=late_window.SCENARIO_STREAM)
    window_id: str = Field(default=late_window.SCENARIO_WINDOW)
    window_version: str = "v1"
    close_position: int = Field(default=late_window.SCENARIO_CLOSE, ge=0)
    positions: list[int] = Field(
        default_factory=lambda: list(late_window.SCENARIO_POSITIONS))
    arrival_order: str = "A"
    log_start_position: int | None = None
    next_window_id: str | None = None
    next_close_position: int | None = None


class ReplayPayload(BaseModel):
    stream: str = Field(default=late_window.SCENARIO_STREAM)
    from_position: int = Field(ge=0)
    window_id: str | None = None
    window_version: str | None = None


class ReceiptPayload(BaseModel):
    stream: str = Field(default=late_window.SCENARIO_STREAM)
    window_id: str = Field(default=late_window.SCENARIO_WINDOW)
    window_version: str = "v1"
    cover_start: int = Field(ge=0)
    cover_end: int = Field(ge=0)
    result: str = "成功"
    source: str = "下游提交回执"


class LinkPayload(BaseModel):
    stream: str = Field(default=late_window.SCENARIO_STREAM)
    checkpoint_id: str
    replay_start: int = Field(ge=0)


class ComparePayload(BaseModel):
    positions: list[int] = Field(
        default_factory=lambda: list(late_window.SCENARIO_POSITIONS))
    close_position: int = Field(default=late_window.SCENARIO_CLOSE, ge=0)
    window_id: str = late_window.SCENARIO_WINDOW
    window_version: str = "v1"
    next_window_id: str | None = None
    log_start_position: int | None = None


@router.get("/state")
def get_state(stream: str | None = Query(default=None)) -> dict[str, Any]:
    """当前账本：窗口、分段、补偿行、回执、提交进度、检查点与回放记录。"""
    return late_window.state(stream=stream)


@router.get("/verify")
def verify(
    stream: str = Query(default=late_window.SCENARIO_STREAM),
    window_id: str = Query(default=late_window.SCENARIO_WINDOW),
    window_version: str = Query(default="v1"),
    close_position: int = Query(default=late_window.SCENARIO_CLOSE, ge=0),
    positions: str = Query(default="", description="逗号分隔的源位点，缺省 119,120,121"),
) -> dict[str, Any]:
    """重新计算入窗阈值、幂等、丢行和提交区间，逐项给出通过与否。"""
    parsed = [int(item) for item in positions.split(",") if item.strip().isdigit()]
    return late_window.verify(
        stream=stream,
        window_id=window_id,
        window_version=window_version,
        close_position=close_position,
        expected_positions=parsed or late_window.SCENARIO_POSITIONS,
    )


@router.post("/compare-orders")
def compare_orders(payload: ComparePayload) -> dict[str, Any]:
    """同一组事件两种到达顺序的归属对照（纯计算，不写账本）。"""
    return late_window.compare_arrival_orders(
        positions=payload.positions,
        close_position=payload.close_position,
        window_id=payload.window_id,
        window_version=payload.window_version,
        next_window_id=payload.next_window_id,
        log_start_position=payload.log_start_position,
    )


@router.post("/admit")
def admit(payload: AdmitPayload) -> dict[str, Any]:
    """登记单条迟到事件；重复提交同一记录返回 duplicate 且不重复生效。"""
    return late_window.admit_event(
        stream=payload.stream,
        window_id=payload.window_id,
        window_version=payload.window_version,
        close_position=payload.close_position,
        source_position=payload.source_position,
        event_ref=payload.event_ref,
        record_summary=payload.record_summary,
        arrival_order=payload.arrival_order,
        log_start_position=payload.log_start_position,
        next_window_id=payload.next_window_id,
        next_close_position=payload.next_close_position,
    )


@router.post("/admit-batch")
def admit_batch(payload: BatchPayload) -> dict[str, Any]:
    """按一种到达顺序批量登记事件。"""
    return late_window.admit_batch(
        stream=payload.stream,
        window_id=payload.window_id,
        window_version=payload.window_version,
        close_position=payload.close_position,
        positions=payload.positions,
        arrival_order=payload.arrival_order,
        log_start_position=payload.log_start_position,
        next_window_id=payload.next_window_id,
        next_close_position=payload.next_close_position,
    )


@router.post("/scenario")
def setup_scenario(
    arrival_order: str = Query(default="A", pattern="^[AB]$"),
    reset: bool = Query(default=True),
) -> dict[str, Any]:
    """搭建关闭位点 120 的场景：119/120 归原窗口，121 进入下一窗口。"""
    return late_window.setup_closed_boundary_scenario(
        arrival_order=arrival_order, reset=reset)


@router.get("/compensations")
def list_compensations(
    stream: str | None = Query(default=None),
    window_id: str | None = Query(default=None),
    window_version: str | None = Query(default=None),
    delivery_status: str | None = Query(default=None),
    confirm_status: str | None = Query(default=None),
    interrupted_only: bool = Query(default=False),
) -> dict[str, Any]:
    """查询补偿行；中断期间已交给下游的行可通过 interrupted_only 单独查。"""
    rows = late_window.list_compensations(
        stream=stream, window_id=window_id, window_version=window_version,
        delivery_status=delivery_status, confirm_status=confirm_status,
        interrupted_only=interrupted_only,
    )
    return {"items": rows, "total": len(rows)}


@router.get("/compensations/{compensation_id}")
def get_compensation(compensation_id: str) -> dict[str, Any]:
    row = late_window.find_compensation(compensation_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"补偿 {compensation_id} 不存在")
    return row


@router.post("/compensations/{compensation_id}/deliver")
def deliver_compensation(
    compensation_id: str,
    interrupted: bool = Query(default=False),
) -> dict[str, Any]:
    row = late_window.deliver_compensation(compensation_id, interrupted=interrupted)
    if row is None:
        raise HTTPException(status_code=404, detail=f"补偿 {compensation_id} 不存在")
    return {"ok": True, "message": "补偿行已交给下游", "compensation": row}


@router.post("/compensations/{compensation_id}/confirm")
def confirm_compensation(compensation_id: str) -> dict[str, Any]:
    """确认一笔补偿；重复确认返回 duplicate，进度只推进一次。"""
    result = late_window.confirm_compensation(compensation_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"补偿 {compensation_id} 不存在")
    return result


@router.post("/receipts")
def register_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    """登记下游成功回执，作为续跑时关联受影响窗口的依据。"""
    row = late_window.register_receipt(
        stream=payload.stream,
        window_id=payload.window_id,
        window_version=payload.window_version,
        cover_start=payload.cover_start,
        cover_end=payload.cover_end,
        result=payload.result,
        source=payload.source,
    )
    return {"ok": True, "receipt": row}


@router.get("/receipts")
def list_receipts(
    stream: str | None = Query(default=None),
    window_id: str | None = Query(default=None),
    window_version: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = late_window.list_receipts(
        stream=stream, window_id=window_id, window_version=window_version)
    return {"items": rows, "total": len(rows)}


@router.post("/replay")
def replay(payload: ReplayPayload) -> dict[str, Any]:
    """从旧日志位点发起回放；按源位点幂等，待复核分段会阻断推进。"""
    return late_window.replay(
        stream=payload.stream,
        from_position=payload.from_position,
        window_id=payload.window_id,
        window_version=payload.window_version,
    )


@router.get("/commit")
def commit(
    stream: str = Query(default=late_window.SCENARIO_STREAM),
    window_id: str | None = Query(default=None),
    window_version: str | None = Query(default=None),
) -> dict[str, Any]:
    """用迟到事件分段数据确定实际可确认的处理区间。"""
    return late_window.compute_commit(
        stream=stream, window_id=window_id, window_version=window_version)


@router.post("/link-affected")
def link_affected(payload: LinkPayload) -> dict[str, Any]:
    """续跑前把受影响窗口和已有成功回执关联起来。"""
    rows = late_window.link_affected_windows(
        stream=payload.stream,
        checkpoint_id=payload.checkpoint_id,
        replay_start=payload.replay_start,
    )
    return {"ok": True, "affected_windows": rows, "total": len(rows)}


@router.get("/affected")
def affected(stream: str | None = Query(default=None)) -> dict[str, Any]:
    rows = late_window.affected_windows(stream)
    return {"items": rows, "total": len(rows)}


@router.post("/resume")
def resume(
    stream: str | None = Query(default=None),
    checkpoint_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """断点恢复：用受影响窗口与回执安排回放，并给出待复核项。"""
    return late_window.resume(stream=stream, checkpoint_id=checkpoint_id)

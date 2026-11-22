/** 迟到事件分段 API：入窗判定、补偿、回放、续跑与回验。 */
import { request } from './client'

export type LateRow = Record<string, unknown>

export type LateWindowState = {
  ok: boolean
  stream: string | null
  windows: Record<string, unknown>[]
  segments: Record<string, unknown>[]
  compensations: Record<string, unknown>[]
  receipts: Record<string, unknown>[]
  commit_progress: Record<string, unknown>[]
  checkpoints: Record<string, unknown>[]
  affected_windows: Record<string, unknown>[]
  replay_runs: Record<string, unknown>[]
  counts: {
    windows: number
    segments: number
    pending_segments: number
    pending_compensations: number
    interrupted_compensations: number
    next_window_records: number
  }
}

export type LateCommit = {
  ok: boolean
  stream: string
  actual_commit_position: number | null
  确认区间: string
  阻塞分段: string
  状态: string
  windows: Record<string, unknown>[]
}

export type LateReplay = {
  ok: boolean
  stream: string
  from_position: number
  to_position: number
  window: Record<string, unknown>
  window_versions: Record<string, unknown>[]
  consumable_records: Record<string, unknown>[]
  duplicate_records: Record<string, unknown>[]
  newly_delivered: Record<string, unknown>[]
  pending_review_segments: Record<string, unknown>[]
  next_window_records: Record<string, unknown>[]
  compensations: Record<string, unknown>[]
  actual_commit_position: number | null
  commit: LateCommit
}

export type LateVerify = {
  ok: boolean
  checks: { 检查项: string; 通过: boolean; 明细: unknown }[]
  state: { actual_commit_position: number | null; commit_interval: string }
}

export const LATE_STREAM = 'order_events_cdc'

export async function fetchLateState(stream = LATE_STREAM): Promise<LateWindowState> {
  return getJson<LateWindowState>(`/api/late-window/state?stream=${encodeURIComponent(stream)}`)
}

export async function runScenario(order: 'A' | 'B'): Promise<Record<string, unknown>> {
  return postJson(`/api/late-window/scenario?arrival_order=${order}&reset=true`, {})
}

export async function confirmCompensation(id: string): Promise<Record<string, unknown>> {
  return postJson(`/api/late-window/compensations/${id}/confirm`, {})
}

export async function replayLate(from = 119): Promise<LateReplay> {
  return postJson<LateReplay>('/api/late-window/replay', {
    stream: LATE_STREAM,
    from_position: from,
  })
}

export async function verifyLate(): Promise<LateVerify> {
  return getJson<LateVerify>('/api/late-window/verify')
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

async function postJson<T = Record<string, unknown>>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

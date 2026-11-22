<template>
  <section class="page" data-module="late_window">
    <header class="page-head">
      <div>
        <h2>迟到事件分段</h2>
        <p class="page-desc">
          关闭位点含边界：源位点 <= 120 归原窗口，121 进入下一窗口。迟到记录落成待复核分段，
          同一记录重复提交只生效一次；确认补偿后从旧日志位点回放。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="setup-A" type="button" @click="setup('A')">按顺序-A 搭建场景</button>
        <button class="btn" data-testid="setup-B" type="button" @click="setup('B')">按顺序-B 搭建场景</button>
        <button class="btn primary" data-testid="verify" type="button" @click="runVerify">重新校验</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">可消费分段</span>
        <strong class="stat-value" data-testid="stat-segments">{{ counts.segments }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待复核分段</span>
        <strong class="stat-value" data-testid="stat-pending">{{ counts.pending_segments }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">中断期间交付补偿</span>
        <strong class="stat-value" data-testid="stat-interrupted">{{ counts.interrupted_compensations }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">实际提交位置</span>
        <strong class="stat-value" data-testid="stat-commit">{{ commit?.actual_commit_position ?? '—' }}</strong>
      </article>
    </div>

    <p class="status-legend" data-testid="verify-banner">
      校验：{{ verifyOk === null ? '尚未执行' : verifyOk ? '全部通过' : '存在未通过项' }}
      <span v-if="commit"> · 确认区间 {{ commit['确认区间'] }} · {{ commit['状态'] }}</span>
      <span v-if="message" class="error-text"> · {{ message }}</span>
    </p>

    <h3 class="section-title">迟到事件分段</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>分段编号</th><th>数据流</th><th>窗口编号</th><th>窗口版本</th>
          <th>源位点</th><th>归属</th><th>入窗阈值</th><th>到达顺序</th>
          <th>状态</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in segments" :key="String(row['分段编号'])" :data-testid="`segment-${row['源位点']}`">
          <td>{{ row['分段编号'] }}</td>
          <td>{{ row['数据流'] }}</td>
          <td>{{ row['窗口编号'] }}</td>
          <td>{{ row['窗口版本'] }}</td>
          <td>{{ row['源位点'] }}</td>
          <td>{{ row['归属'] }}</td>
          <td>{{ row['入窗阈值'] }}</td>
          <td>{{ row['到达顺序'] }}</td>
          <td>{{ row['status'] }}</td>
          <td class="row-actions">
            <button
              v-if="row['status'] === '待复核'"
              class="link"
              type="button"
              :data-testid="`confirm-${row['源位点']}`"
              @click="confirmFor(row)"
            >确认补偿</button>
          </td>
        </tr>
        <tr v-if="!segments.length">
          <td colspan="10" class="empty-state">暂无分段，先搭建场景</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">补偿行（中断期间交付可查）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>补偿编号</th><th>源位点</th><th>窗口版本</th><th>交付状态</th>
          <th>中断标记</th><th>确认状态</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in compensations" :key="String(row['补偿编号'])" :data-testid="`comp-${row['补偿编号']}`">
          <td>{{ row['补偿编号'] }}</td>
          <td>{{ row['源位点'] }}</td>
          <td>{{ row['窗口版本'] }}</td>
          <td>{{ row['交付状态'] }}</td>
          <td>{{ row['中断标记'] || '—' }}</td>
          <td>{{ row['确认状态'] }}</td>
          <td class="row-actions">
            <button
              v-if="row['确认状态'] === '待确认'"
              class="link"
              type="button"
              :data-testid="`comp-confirm-${row['补偿编号']}`"
              @click="confirmCompensation(String(row['补偿编号']))"
            >确认</button>
          </td>
        </tr>
        <tr v-if="!compensations.length">
          <td colspan="7" class="empty-state">暂无补偿行</td>
        </tr>
      </tbody>
    </table>

    <div class="page-actions replay-bar">
      <button class="btn primary" data-testid="replay" type="button" @click="replay">从位点 119 回放</button>
      <button class="btn" data-testid="resume" type="button" @click="resume">续跑：关联受影响窗口</button>
    </div>

    <template v-if="replayResult">
      <h3 class="section-title">回放结果</h3>
      <p class="status-legend" data-testid="replay-summary">
        可消费 {{ replayResult.consumable_records.length }} 条 ·
        重复跳过 {{ replayResult.duplicate_records.length }} 条 ·
        待复核 {{ replayResult.pending_review_segments.length }} 条 ·
        实际提交位置 {{ replayResult.actual_commit_position }}
      </p>
      <table class="data-table">
        <thead>
          <tr><th>源位点</th><th>窗口编号</th><th>窗口版本</th><th>归属</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in replayResult.window_versions" :key="String(row['源位点'])">
            <td>{{ row['源位点'] }}</td>
            <td>{{ row['窗口编号'] }}</td>
            <td>{{ row['窗口版本'] }}</td>
            <td>{{ row['归属'] }}</td>
            <td>{{ row['状态'] }}</td>
          </tr>
        </tbody>
      </table>
      <p class="status-legend" data-testid="next-window">
        下一窗口正常记录：{{ replayResult.next_window_records.map((row) => row['源位点']).join('、') || '无' }}
      </p>
    </template>

    <template v-if="checks.length">
      <h3 class="section-title">可回验校验</h3>
      <table class="data-table">
        <thead><tr><th>检查项</th><th>结果</th></tr></thead>
        <tbody>
          <tr v-for="check in checks" :key="check['检查项']" :data-testid="`check-${check['检查项']}`">
            <td>{{ check['检查项'] }}</td>
            <td>{{ check['通过'] ? '通过' : '未通过' }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <p class="status-legend" v-if="resumeResult" data-testid="resume-summary">
      续跑起点 {{ resumeResult['replay_start'] }} · 受影响窗口
      {{ (resumeResult['affected_windows'] as Record<string, unknown>[]).map((row) => `${row['窗口编号']}/${row['窗口版本']}@${row['回执编号'] || '无回执'}`).join('、') }}
    </p>

    <footer class="page-foot">
      <span>账号口径：{{ stream }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  LATE_STREAM,
  confirmCompensation as confirmCompensationApi,
  fetchLateState,
  replayLate,
  runScenario,
  verifyLate,
  type LateCommit,
  type LateReplay,
} from '@/api/late-window'
import { fetchJson } from '@/api/client'

type Row = Record<string, unknown>

const stream = LATE_STREAM
const segments = ref<Row[]>([])
const compensations = ref<Row[]>([])
const commit = ref<LateCommit | null>(null)
const counts = ref({
  segments: 0,
  pending_segments: 0,
  pending_compensations: 0,
  interrupted_compensations: 0,
  next_window_records: 0,
  windows: 0,
})
const checks = ref<{ 检查项: string; 通过: boolean; 明细: unknown }[]>([])
const verifyOk = ref<boolean | null>(null)
const replayResult = ref<LateReplay | null>(null)
const resumeResult = ref<Row | null>(null)
const message = ref('')
const errorMessage = ref('')

async function refresh() {
  const state = await fetchLateState(stream)
  segments.value = state.segments
  compensations.value = state.compensations
  counts.value = state.counts
  commit.value = await fetchJson<LateCommit>(
    `/api/late-window/commit?stream=${encodeURIComponent(stream)}`,
  )
}

async function setup(order: 'A' | 'B') {
  errorMessage.value = ''
  try {
    const result = await runScenario(order)
    const before = result['before_confirmation'] as Row | undefined
    message.value = `顺序-${order}：${String(before?.['note'] ?? '')}`
    replayResult.value = null
    resumeResult.value = null
    checks.value = []
    verifyOk.value = null
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '场景搭建失败'
  }
}

async function confirmFor(row: Row) {
  const id = String(row['补偿编号'] ?? '')
  if (!id) {
    errorMessage.value = '该分段缺少补偿编号'
    return
  }
  await confirmCompensation(id)
}

async function confirmCompensation(id: string) {
  errorMessage.value = ''
  try {
    const result = await confirmCompensationApi(id)
    message.value = String(result['message'] ?? '')
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认失败'
  }
}

async function replay() {
  errorMessage.value = ''
  try {
    replayResult.value = await replayLate(119)
    message.value = replayResult.value.ok ? '回放完成' : '回放受阻：存在待复核分段'
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回放失败'
  }
}

async function resume() {
  errorMessage.value = ''
  try {
    const response = await fetch(`/api/late-window/resume?stream=${encodeURIComponent(stream)}`, {
      method: 'POST',
    })
    if (!response.ok) throw new Error(`接口返回 ${response.status}`)
    resumeResult.value = (await response.json()) as Row
    message.value = '续跑已关联受影响窗口与成功回执'
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续跑失败'
  }
}

async function runVerify() {
  errorMessage.value = ''
  try {
    const result = await verifyLate()
    checks.value = result.checks
    verifyOk.value = result.ok
    message.value = `校验${result.ok ? '通过' : '未通过'}`
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '校验失败'
  }
}

onMounted(async () => {
  try {
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据读取失败'
  }
})
</script>

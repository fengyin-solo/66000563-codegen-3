<template>
  <div class="panel">
    <h4>⚡ 熔断器状态</h4>
    <div v-if="activeRules" class="rules-tag">
      生效规则：连续失败 {{ activeRules.failureThreshold }} 次熔断 ·
      冷却 {{ activeRules.cooldownSeconds }}s ·
      最多重试 {{ activeRules.maxRetries }} 次
    </div>
    <div class="cb-list">
      <div v-for="cb in breakers" :key="cb.taskId" class="cb-row" :class="cb.state.toLowerCase()">
        <div class="cb-main">
          <span class="cb-task">{{ nameOf(cb.taskId) }}</span>
          <span class="cb-state">{{ stateLabel(cb.state) }}</span>
        </div>
        <div class="cb-meta">
          <span class="cb-count">连续失败 {{ cb.failureCount }}/{{ activeRules?.failureThreshold ?? '-' }}</span>
          <span v-if="cb.state === 'OPEN'" class="cb-cooldown">
            冷却剩余 {{ remaining(cb.cooldownUntil) }}s
          </span>
          <span v-else-if="cb.state === 'HALF_OPEN'" class="cb-half">试探执行中</span>
          <span v-else-if="cb.state === 'FAILED'" class="cb-failed">已失败</span>
        </div>
      </div>
      <div v-if="!breakers.length" class="empty">尚无执行中的熔断器</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useDAGStore } from '../store/dag'
import type { CircuitBreaker } from '../types'

const store = useDAGStore()
const breakers = computed<CircuitBreaker[]>(() => store.execution?.circuitBreakers || [])
// 正在执行的工作流使用其启动时的规则快照；未执行时展示当前公共规则
const activeRules = computed(() => store.execution?.rules || store.rules)

// 驱动“剩余冷却时间”每秒刷新；冷却结束后由后端把熔断器放开为 HALF_OPEN
const now = ref(Date.now())
let timer: number | undefined
onMounted(() => { timer = window.setInterval(() => { now.value = Date.now() }, 500) })
onUnmounted(() => { if (timer) window.clearInterval(timer) })

function remaining(cooldownUntil: number): string {
  const left = Math.max(0, (cooldownUntil * 1000 - now.value) / 1000)
  return left.toFixed(1)
}

const STATE_LABELS: Record<string, string> = {
  CLOSED: '正常', OPEN: '熔断中', HALF_OPEN: '半开试探', FAILED: '失败'
}
function stateLabel(state: string): string {
  return STATE_LABELS[state] || state
}

const nameMap = computed(() => {
  const m: Record<string, string> = {}
  const nodes = store.execution?.workflow.nodes || store.workflow?.nodes || []
  nodes.forEach(n => { m[n.id] = n.name })
  return m
})
function nameOf(taskId: string): string {
  return nameMap.value[taskId] ? `${nameMap.value[taskId]}（${taskId}）` : taskId
}
</script>

<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#f87171;font-size:12px;margin-bottom:6px}
.rules-tag{font-size:10px;color:#93c5fd;background:#3182ce12;border-radius:4px;padding:4px 6px;margin-bottom:6px;line-height:1.6}
.cb-list{max-height:260px;overflow-y:auto}
.cb-row{display:flex;flex-direction:column;gap:2px;padding:4px 6px;border-radius:4px;font-size:11px;margin:2px 0}
.cb-row.open{background:#ef444415;border:1px solid #ef444444}
.cb-row.half_open{background:#fbbf2415}
.cb-row.failed{background:#7f1d1d30}
.cb-main{display:flex;justify-content:space-between;gap:8px}
.cb-meta{display:flex;justify-content:space-between;gap:8px}
.cb-task{color:#ccc;font-weight:600}.cb-state{font-weight:700}
.cb-count{color:#888;font-size:10px}
.cb-cooldown{color:#ef4444;font-size:10px;font-weight:700;font-family:monospace}
.cb-half{color:#fbbf24;font-size:10px}.cb-failed{color:#f87171;font-size:10px}
.closed .cb-state{color:#22c55e}.open .cb-state{color:#ef4444}
.half_open .cb-state{color:#fbbf24}.failed .cb-state{color:#f87171}
.empty{color:#4a5568;font-size:11px}
</style>

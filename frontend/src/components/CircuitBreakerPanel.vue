<template>
  <div class="panel">
    <h4>⚡ 熔断器状态</h4>
    <div v-if="activeRules" class="rules-summary" :title="rulesTip">
      <span>阈值 {{ activeRules.failureThreshold }}次</span>
      <span>冷却 {{ activeRules.cooldownSeconds }}s</span>
      <span>重试上限 {{ activeRules.maxRetries }}次</span>
    </div>
    <div v-for="cb in breakers" :key="cb.taskId" class="cb-row" :class="stateClass(cb.state)">
      <span class="cb-task">{{ cb.taskId }}</span>
      <span class="cb-state">{{ stateText(cb.state) }}</span>
      <span class="cb-count">{{ cb.failureCount }} 次失败</span>
      <span v-if="cb.state === 'OPEN'" class="cb-cooldown">⏳ 冷却剩余 {{ remaining(cb) }}s</span>
    </div>
    <div v-if="!breakers.length" class="empty">无熔断保护激活</div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useDAGStore, DEFAULT_RULES } from '../store/dag'
import type { CircuitBreaker } from '../types'
const store = useDAGStore()
const breakers = computed(() => store.execution?.circuitBreakers || [])
// 正在执行的环节使用启动时快照的规则；未执行时展示当前公共规则（旧工作流缺字段走默认值兜底）
const activeRules = computed(() => store.execution?.rules ?? store.rules ?? DEFAULT_RULES)
const rulesTip = computed(() =>
  `熔断失败阈值：连续失败 ${activeRules.value.failureThreshold} 次触发熔断；` +
  `冷却时间：${activeRules.value.cooldownSeconds} 秒后由后端放开试探；` +
  `最大重试次数：${activeRules.value.maxRetries} 次`)

// 本地每秒刷新，让“剩余冷却时间”在两次后端推送之间也能平滑倒数
const now = ref(Date.now())
let timer: number | undefined
onMounted(() => { timer = window.setInterval(() => { now.value = Date.now() }, 500) })
onUnmounted(() => { if (timer) clearInterval(timer) })

function remaining(cb: CircuitBreaker): number {
  return Math.max(0, Math.ceil((cb.cooldownUntil - now.value / 1000)))
}
function stateClass(s: string): string {
  return s === 'HALF_OPEN' ? 'half_open' : s.toLowerCase()
}
function stateText(s: string): string {
  return s === 'HALF_OPEN' ? '试探中' : s === 'OPEN' ? '熔断中' : '正常'
}
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#f87171;font-size:12px;margin-bottom:6px}
.rules-summary{display:flex;gap:8px;flex-wrap:wrap;font-size:10px;color:#bb86fc;background:#bb86fc10;border:1px solid #bb86fc30;border-radius:4px;padding:4px 6px;margin-bottom:6px}
.cb-row{display:flex;gap:8px;align-items:center;padding:4px 6px;border-radius:4px;font-size:11px;margin:2px 0;flex-wrap:wrap}
.cb-row.open{background:#ef444415}
.cb-row.half_open{background:#fbbf2415}
.cb-task{color:#ccc;font-weight:600}.cb-state{font-weight:700}.cb-count{color:#888;font-size:10px}
.cb-cooldown{color:#f87171;font-size:10px;font-weight:700;font-family:monospace}
.open .cb-state{color:#ef4444}.closed .cb-state{color:#22c55e}.half_open .cb-state{color:#fbbf24}
.empty{color:#4a5568;font-size:11px}
</style>

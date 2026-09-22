import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo, ResilienceRules, RuleErrors, RuleLimits } from '@/types'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  // 公共熔断/重试规则。旧后端 / 无数据时由后端默认值兜底
  const rules = ref<ResilienceRules | null>(null)
  const ruleLimits = ref<RuleLimits | null>(null)

  let ws: WebSocket|null = null
  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onmessage = (e) => {
      try { const d = JSON.parse(e.data); execution.value = d }
      catch { /* ignore malformed frame */ }
    }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try { const { data } = await axios.post('/api/workflow', { name }) ; workflow.value = data }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      execution.value = data
    } finally { loading.value = false }
  }

  async function fetchRules() {
    const { data } = await axios.get('/api/rules')
    rules.value = data.rules
    ruleLimits.value = data.limits
    return data
  }

  // 返回逐字段错误；保存成功返回 null。旧设置不合规时后端会直接 400，不会落库
  async function saveRules(next: ResilienceRules): Promise<RuleErrors | null> {
    try {
      const { data } = await axios.put('/api/rules', next)
      rules.value = data.rules
      ruleLimits.value = data.limits
      return null
    } catch (e: any) {
      if (e?.response?.data?.errors) return e.response.data.errors as RuleErrors
      return { failureThreshold: e?.message || '保存失败，请稍后重试' }
    }
  }

  function disconnectWS() { ws?.close(); ws = null }
  return {
    loading, workflow, execution, wsConnected, workers, strategy,
    rules, ruleLimits, connectWS, createWorkflow, run, fetchRules, saveRules, disconnectWS
  }
})

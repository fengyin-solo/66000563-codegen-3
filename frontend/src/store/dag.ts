import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo, CircuitRules, RulesResponse, RuleValidationError } from '@/types'

export const DEFAULT_RULES: CircuitRules = { failureThreshold: 3, cooldownSeconds: 10, maxRetries: 3 }
export const RULE_LIMITS: Record<keyof CircuitRules, { min: number; max: number }> = {
  failureThreshold: { min: 1, max: 10 },
  cooldownSeconds: { min: 1, max: 300 },
  maxRetries: { min: 1, max: 10 },
}
export const RULE_LABELS: Record<keyof CircuitRules, string> = {
  failureThreshold: '熔断失败阈值',
  cooldownSeconds: '熔断冷却时间',
  maxRetries: '最大重试次数',
}

export class RulesValidationException extends Error {
  errors: RuleValidationError[]
  constructor(errors: RuleValidationError[]) {
    super(errors.map(e => e.message).join('；'))
    this.name = 'RulesValidationException'
    this.errors = errors
  }
}

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')
  // 公共规则：同一套设置对所有环节、新建与已有的工作流生效
  const rules = ref<CircuitRules>({ ...DEFAULT_RULES })

  let ws: WebSocket|null = null
  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onmessage = (e) => {
      try { const d = JSON.parse(e.data); execution.value = d }
      catch {}
    }
  }

  async function fetchRules() {
    const { data } = await axios.get<RulesResponse>('/api/rules')
    rules.value = { ...data.rules }
    return data
  }

  async function saveRules(next: CircuitRules) {
    try {
      const { data } = await axios.put('/api/rules', next)
      rules.value = { ...data.rules }
    } catch (err: any) {
      const errs = err?.response?.data?.detail?.errors as RuleValidationError[] | undefined
      if (errs) throw new RulesValidationException(errs)
      throw err
    }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try {
      const { data } = await axios.post('/api/workflow', { name })
      // 旧工作流缺字段/字段异常时按默认值兜底
      if (!data.rules) data.rules = { ...DEFAULT_RULES }
      workflow.value = data
    } finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      if (!data.rules) data.rules = { ...DEFAULT_RULES }
      execution.value = data
    } finally { loading.value = false }
  }

  function disconnectWS() { ws?.close(); ws = null }
  return {
    loading, workflow, execution, wsConnected, workers, strategy, rules,
    connectWS, fetchRules, saveRules, createWorkflow, run, disconnectWS
  }
})

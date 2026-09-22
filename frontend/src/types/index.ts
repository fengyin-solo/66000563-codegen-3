export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface CircuitRules { failureThreshold: number; cooldownSeconds: number; maxRetries: number }
export interface RuleLimits { min: number; max: number }
export interface RuleValidationError { field: string; message: string }
export interface RulesResponse {
  rules: CircuitRules
  defaults: CircuitRules
  limits: Record<keyof CircuitRules, RuleLimits>
  labels: Record<keyof CircuitRules, string>
}
export interface ExecutionInfo {
  workflow: DAGWorkflow
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  rules?: CircuitRules
  completed: boolean
  aborted?: boolean
}

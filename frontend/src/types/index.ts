export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface ResilienceRules { failureThreshold: number; cooldownSeconds: number; maxRetries: number }
export type RuleField = keyof ResilienceRules
export type RuleLimits = Record<RuleField, { min: number; max: number }>
export type RuleErrors = Partial<Record<RuleField, string>>
export interface RulesResponse { rules: ResilienceRules; limits: RuleLimits; defaults: ResilienceRules }
export interface ExecutionInfo {
  workflow: DAGWorkflow
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  rules?: ResilienceRules
  completed: boolean
}

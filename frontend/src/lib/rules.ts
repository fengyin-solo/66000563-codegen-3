import type { ResilienceRules, RuleErrors, RuleField, RuleLimits } from '@/types'

export const DEFAULT_LIMITS: RuleLimits = {
  failureThreshold: { min: 1, max: 10 },
  cooldownSeconds: { min: 1, max: 600 },
  maxRetries: { min: 0, max: 10 }
}

export const RULE_LABELS: Record<RuleField, string> = {
  failureThreshold: '熔断失败次数',
  cooldownSeconds: '冷却时间',
  maxRetries: '最大重试次数'
}

function isInt(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value)
}

/**
 * 校验熔断/重试规则。与后端 validate_rules 保持一致：
 *  - 三项均须为整数且落在合理区间内
 *  - failureThreshold 不能大于 maxRetries + 1，否则重试耗尽前熔断永远不会触发
 * 返回逐字段错误信息（未通过的项明确标出），全部合法时返回空对象。
 */
export function validateRules(rules: Partial<Record<RuleField, unknown>>, limits: RuleLimits = DEFAULT_LIMITS): RuleErrors {
  const errors: RuleErrors = {}
  const values = {} as ResilienceRules

  ;(Object.keys(limits) as RuleField[]).forEach((field) => {
    const value = rules[field]
    const { min, max } = limits[field]
    if (!isInt(value)) {
      errors[field] = `${RULE_LABELS[field]}必须是整数`
    } else if (value < min || value > max) {
      const unit = field === 'cooldownSeconds' ? ' 秒' : ' 次'
      errors[field] = `${RULE_LABELS[field]}超出合理范围（${min}~${max}${unit}）`
    } else {
      values[field] = value
    }
  })

  if (Object.keys(errors).length === 0 && values.failureThreshold > values.maxRetries + 1) {
    errors.failureThreshold = `熔断失败次数(${values.failureThreshold})不能大于最大重试次数+1(${values.maxRetries + 1})，否则重试耗尽前熔断永远无法触发`
  }
  return errors
}

export function isValid(rules: Partial<Record<RuleField, unknown>>, limits?: RuleLimits): boolean {
  return Object.keys(validateRules(rules, limits)).length === 0
}

<template>
  <el-dialog v-model="visible" title="⚙️ 熔断保护与重试规则" width="460px" @open="onOpen">
    <el-alert type="info" :closable="false" show-icon style="margin-bottom:14px">
      <template #title>
        同一套规则为所有环节公共设置，对新建与已有的工作流均生效；规则调整后，正在执行中的环节仍按调整前的约束走完，之后新启动的执行才使用新规则。
      </template>
    </el-alert>

    <el-form label-width="130px" class="rules-form" @submit.prevent>
      <el-form-item :label="labels.failureThreshold" :error="fieldErrors.failureThreshold">
        <el-input-number v-model="form.failureThreshold" :min="limits.failureThreshold.min"
          :max="limits.failureThreshold.max" :step="1" controls-position="right" style="width:150px"/>
        <span class="hint">连续出错达到此次数即熔断（{{ limits.failureThreshold.min }}~{{ limits.failureThreshold.max }} 次）</span>
      </el-form-item>

      <el-form-item :label="labels.cooldownSeconds" :error="fieldErrors.cooldownSeconds">
        <el-input-number v-model="form.cooldownSeconds" :min="limits.cooldownSeconds.min"
          :max="limits.cooldownSeconds.max" :step="5" controls-position="right" style="width:150px"/>
        <span class="hint">熔断后冷却多久再放开试探（{{ limits.cooldownSeconds.min }}~{{ limits.cooldownSeconds.max }} 秒）</span>
      </el-form-item>

      <el-form-item :label="labels.maxRetries" :error="fieldErrors.maxRetries">
        <el-input-number v-model="form.maxRetries" :min="limits.maxRetries.min"
          :max="limits.maxRetries.max" :step="1" controls-position="right" style="width:150px"/>
        <span class="hint">单个环节最多自动重试几次（{{ limits.maxRetries.min }}~{{ limits.maxRetries.max }} 次）</span>
      </el-form-item>
    </el-form>

    <div v-if="rootError" class="root-error">{{ rootError }}</div>

    <template #footer>
      <el-button @click="visible = false">取消</el-button>
      <el-button @click="onReset">恢复默认</el-button>
      <el-button type="primary" :loading="saving" @click="onSave">保存</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { reactive, ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import {
  useDAGStore, DEFAULT_RULES, RULE_LIMITS, RULE_LABELS, RulesValidationException
} from '../store/dag'
import type { CircuitRules } from '../types'

const emit = defineEmits<{ (e: 'saved', rules: CircuitRules): void }>()
const store = useDAGStore()
const visible = defineModel<boolean>('modelValue', { default: false })

const limits = RULE_LIMITS
const labels = RULE_LABELS
const form = reactive<CircuitRules>({ ...DEFAULT_RULES })
const serverErrors = ref<Record<string, string>>({})
const saving = ref(false)

function onOpen() {
  Object.assign(form, store.rules)
  serverErrors.value = {}
}

function onReset() {
  Object.assign(form, DEFAULT_RULES)
  serverErrors.value = {}
}

function localValidate(): Record<string, string> {
  const errs: Record<string, string> = {}
  for (const key of ['failureThreshold', 'cooldownSeconds', 'maxRetries'] as const) {
    const v = form[key]
    if (!Number.isInteger(v)) { errs[key] = `${labels[key]}必须是整数`; continue }
    const { min, max } = limits[key]
    if (v < min || v > max) errs[key] = `${labels[key]}超出合理范围，应为 ${min}~${max}`
  }
  if (errs.failureThreshold === undefined && errs.maxRetries === undefined
      && form.failureThreshold > form.maxRetries) {
    const msg = `熔断失败阈值(${form.failureThreshold})不能大于最大重试次数(${form.maxRetries})，熔断后需要重试名额放开试探，否则环节永远无法恢复`
    errs.failureThreshold = msg
    errs.maxRetries = msg
  }
  return errs
}

const localErrors = computed(() => localValidate())
const fieldErrors = computed<Record<string, string>>(() => ({ ...localErrors.value, ...serverErrors.value }))
const rootError = computed(() => serverErrors.value['_root'] || '')

async function onSave() {
  serverErrors.value = {}
  const errs = localValidate()
  if (Object.keys(errs).length) { serverErrors.value = errs; return }
  saving.value = true
  try {
    await store.saveRules({ ...form })
    ElMessage.success('熔断与重试规则已保存（运行中的环节继续按旧规则走完）')
    emit('saved', { ...form })
    visible.value = false
  } catch (e) {
    if (e instanceof RulesValidationException) {
      const byField: Record<string, string> = {}
      for (const x of e.errors) byField[x.field] = x.message
      serverErrors.value = byField
      ElMessage.error('规则校验未通过，请检查标红的配置项')
    } else {
      ElMessage.error('保存失败，请稍后重试')
    }
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.hint{color:#7a7a9a;font-size:11px;margin-left:10px}
.root-error{color:#f56c6c;font-size:12px;margin-top:4px}
:deep(.el-form-item__error){white-space:normal;line-height:1.4}
</style>

<template>
  <div class="panel">
    <div class="panel-head">
      <h4>🛡️ 熔断与重试规则</h4>
      <el-button size="small" link type="primary" @click="openEditor">编辑</el-button>
    </div>
    <div v-if="current" class="rule-summary">
      <div class="summary-row"><span>出错触发熔断</span><b>{{ current.failureThreshold }} 次</b></div>
      <div class="summary-row"><span>熔断冷却时间</span><b>{{ current.cooldownSeconds }} 秒</b></div>
      <div class="summary-row"><span>最多重试</span><b>{{ current.maxRetries }} 次</b></div>
    </div>
    <div v-else class="empty">规则加载中...</div>
    <p class="hint">同一套规则为各环节公共设置，对新建与已有工作流均生效；已在执行的工作流继续按调整前的约束走完。</p>

    <el-dialog v-model="dialogVisible" title="编辑熔断与重试规则" width="380px" append-to-body>
      <el-form label-position="top" class="rule-form" @submit.prevent>
        <el-form-item label="出错触发熔断（连续失败次数）" :error="formErrors.failureThreshold">
          <el-input-number v-model="draft.failureThreshold" :min="limits.failureThreshold.min"
            :max="limits.failureThreshold.max" :step="1" step-strictly controls-position="right"
            class="full" :class="{ invalid: formErrors.failureThreshold }" />
        </el-form-item>
        <el-form-item label="冷却时间（秒）" :error="formErrors.cooldownSeconds">
          <el-input-number v-model="draft.cooldownSeconds" :min="limits.cooldownSeconds.min"
            :max="limits.cooldownSeconds.max" :step="1" step-strictly controls-position="right"
            class="full" :class="{ invalid: formErrors.cooldownSeconds }" />
        </el-form-item>
        <el-form-item label="最多重试次数" :error="formErrors.maxRetries">
          <el-input-number v-model="draft.maxRetries" :min="limits.maxRetries.min"
            :max="limits.maxRetries.max" :step="1" step-strictly controls-position="right"
            class="full" :class="{ invalid: formErrors.maxRetries }" />
        </el-form-item>
      </el-form>
      <p class="range-hint">合理范围：阈值/重试 {{ limits.failureThreshold.min }}~{{ limits.failureThreshold.max }} 次，
        冷却 {{ limits.cooldownSeconds.min }}~{{ limits.cooldownSeconds.max }} 秒；
        熔断次数须 ≤ 最大重试 + 1。</p>
      <template #footer>
        <el-button @click="resetDefault">恢复默认</el-button>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" :disabled="!canSave" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useDAGStore } from '../store/dag'
import { validateRules, DEFAULT_LIMITS } from '../lib/rules'
import type { ResilienceRules, RuleErrors, RuleLimits } from '../types'

const store = useDAGStore()

const dialogVisible = ref(false)
const saving = ref(false)
const draft = reactive<ResilienceRules>({ failureThreshold: 3, cooldownSeconds: 5, maxRetries: 3 })
const serverErrors = ref<RuleErrors>({})

const current = computed(() => store.rules)
const limits = computed<RuleLimits>(() => store.ruleLimits || DEFAULT_LIMITS)

// 实时校验：取值超范围或彼此矛盾时，保存按钮不可用，并逐项指出问题
const formErrors = computed<RuleErrors>(() => ({
  ...validateRules(draft, limits.value),
  ...serverErrors.value
}))
const canSave = computed(() => Object.keys(formErrors.value).length === 0)

function openEditor() {
  const base = store.rules
  if (base) Object.assign(draft, base)
  serverErrors.value = {}
  dialogVisible.value = true
}

function resetDefault() {
  // 恢复默认（兜底）值：3 次失败熔断 / 冷却 5 秒 / 最多重试 3 次
  Object.assign(draft, { failureThreshold: 3, cooldownSeconds: 5, maxRetries: 3 })
  serverErrors.value = {}
}

async function save() {
  if (!canSave.value) return
  saving.value = true
  try {
    const errors = await store.saveRules({ ...draft })
    if (errors) {
      // 后端再次拦截：指出具体哪一项不合法
      serverErrors.value = errors
      ElMessage.error('规则未通过校验，未保存')
      return
    }
    ElMessage.success('规则已保存，对之后的执行生效')
    dialogVisible.value = false
  } finally {
    saving.value = false
  }
}

onMounted(() => { store.fetchRules().catch(() => { /* 后端不可用时保持默认展示 */ }) })
</script>

<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.panel-head h4{color:#60a5fa;font-size:12px}
.rule-summary{display:flex;flex-direction:column;gap:3px}
.summary-row{display:flex;justify-content:space-between;font-size:11px;color:#aaa}
.summary-row b{color:#e0e0e0;font-weight:600}
.hint{margin-top:6px;font-size:10px;color:#6b7280;line-height:1.5}
.range-hint{font-size:11px;color:#888;line-height:1.6;margin:0}
.full{width:100%}
:deep(.full .el-input-number){width:100%}
:deep(.invalid .el-input__wrapper){box-shadow:0 0 0 1px var(--el-color-danger) inset}
</style>

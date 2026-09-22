<template>
  <div class="panel">
    <h4>📜 执行日志</h4>
    <el-alert v-if="store.execution?.aborted" type="error" :closable="false" show-icon
      title="有环节重试耗尽后失败，其下游环节不再调度，本次工作流已终止"
      style="margin-bottom:6px;padding:4px 8px" />
    <el-alert v-else-if="store.execution?.completed" type="success" :closable="false" show-icon
      title="全部环节执行完成（失败计数已清零）"
      style="margin-bottom:6px;padding:4px 8px" />
    <div class="log-list">
      <div v-for="(l,i) in logs" :key="i" class="log-row" :class="l.status.toLowerCase()">
        <span class="l-status">{{ l.status }}</span>
        <span class="l-task">{{ l.taskId }}</span>
        <span class="l-msg">{{ l.message }}</span>
      </div>
      <div v-if="!logs.length" class="empty">等待执行...</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useDAGStore } from '../store/dag'
const store = useDAGStore()
const logs = computed(() => store.execution?.logs || [])
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a;flex:1;min-height:200px;display:flex;flex-direction:column}
.panel h4{color:#bb86fc;font-size:12px;margin-bottom:6px}
.log-list{max-height:280px;overflow-y:auto;font-size:10px;font-family:monospace}
.log-row{display:flex;gap:6px;padding:2px 4px;border-radius:2px;margin:1px 0}
.log-row.running{background:#3182ce15}.log-row.success{color:#38a169}.log-row.failed{color:#e53e3e;background:#e53e3e10}
.log-row.circuit_open{color:#f87171;background:#ef444415;font-weight:700}
.log-row.circuit_half_open{color:#fbbf24;background:#fbbf2412}
.l-status{font-weight:700;min-width:90px}.l-task{color:#888;min-width:70px}.l-msg{color:#ccc}.empty{color:#4a5568}
:deep(.el-alert__title){font-size:11px}
</style>

<script lang="ts">
  interface Props {
    matchPct: number;
    label?: string;
  }

  let { matchPct, label }: Props = $props();
</script>

<div class="progress-bar-container">
  {#if label}
    <span class="label">{label}</span>
  {/if}
  <div class="bar">
    <div class="bar-fill" style="width: {matchPct}%"></div>
  </div>
  <span class="pct">{matchPct.toFixed(1)}%</span>
</div>

<style>
  .progress-bar-container {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .label {
    font-size: 0.85em;
    min-width: 120px;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  }

  .bar {
    flex: 1;
    height: 8px;
    background: #e74c3c33;
    border-radius: 4px;
    overflow: hidden;
    min-width: 80px;
  }

  .bar-fill {
    height: 100%;
    background: #27ae60;
    border-radius: 4px;
    transition: width 0.3s ease;
  }

  .pct {
    font-size: 0.8em;
    min-width: 50px;
    text-align: right;
    color: #888;
  }
</style>

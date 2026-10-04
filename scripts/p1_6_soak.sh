#!/bin/zsh
set -euo pipefail

pattern='PipeTests.ConcurrentWritersReadersPreserveDataIntegrity|PipeTests.HighConcurrencyStressNoLossNoDuplication|PipelineBuilderTests.SubgraphMappingBridgesRuntimeDataFlow|PipelineBuilderTests.SubgraphLifecycleRequestStopJoinNoDeadlock'

start_epoch=$(date +%s)
end_epoch=$((start_epoch + 1800))
iter=0
first_rss=0
last_rss=0
max_rss=0
min_rss=0

while [[ $(date +%s) -lt $end_epoch ]]; do
  out=$(/usr/bin/time -l ctest --test-dir build -R "$pattern" --output-on-failure 2>&1) || {
    echo "$out"
    echo "SOAK_STATUS=FAILED"
    exit 1
  }

  rss=$(echo "$out" | grep 'maximum resident set size' | awk '{print $1}')
  if [[ -z "$rss" ]]; then
    rss=0
  fi

  if (( iter == 0 )); then
    first_rss=$rss
    min_rss=$rss
    max_rss=$rss
  fi
  if (( rss < min_rss )); then
    min_rss=$rss
  fi
  if (( rss > max_rss )); then
    max_rss=$rss
  fi

  last_rss=$rss
  iter=$((iter + 1))

  if (( iter % 25 == 0 )); then
    elapsed=$(( $(date +%s) - start_epoch ))
    echo "SOAK_PROGRESS iterations=$iter elapsed_sec=$elapsed latest_maxrss_kb=$rss"
  fi
done

duration=$(( $(date +%s) - start_epoch ))
echo "SOAK_STATUS=PASSED"
echo "SOAK_SUMMARY duration_sec=$duration iterations=$iter first_maxrss_kb=$first_rss last_maxrss_kb=$last_rss min_maxrss_kb=$min_rss max_maxrss_kb=$max_rss"

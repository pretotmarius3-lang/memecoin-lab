#!/bin/zsh
set -euo pipefail
cd "$HOME/memecoin_lab"
mkdir -p runtime_logs runtime_pids

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

if [[ -z "${ALCHEMY_API_KEY:-}" && -z "${ALCHEMY_SOLANA_RPC_URL:-}" ]]; then
  echo "[ERROR] ALCHEMY_API_KEY or ALCHEMY_SOLANA_RPC_URL missing"
  exit 1
fi

start_if_missing() {
  local name="$1" pattern="$2" cmd="$3" log="$4"
  if pgrep -f "$pattern" >/dev/null 2>&1; then
    local pid=$(pgrep -f "$pattern" | head -1)
    echo "[already] $name pid=$pid"
  else
    nohup zsh -lc "cd '$HOME/memecoin_lab'; set -a; source .env; set +a; $cmd" >> "$log" 2>&1 < /dev/null &
    local pid=$!
    echo $pid > "runtime_pids/${name}.pid"
    echo "[started] $name pid=$pid log=$log"
  fi
}

# Stop obsolete prospective providers that would compete for the durable queue.
pkill -f v514_hot_token_http_lane.py 2>/dev/null || true
pkill -f v515_durable_hot_lane.py 2>/dev/null || true
pkill -f v5151_low_usage_hot_lane.py 2>/dev/null || true
pkill -f v516_alchemy_priority_fetcher.py 2>/dev/null || true
pkill -f v517_alchemy_prospective_engine.py 2>/dev/null || true
pkill -f v5171_alchemy_prospective_engine.py 2>/dev/null || true

# Historical catch-up is intentionally OFF while prospective evidence is priority.
pkill -f v531_catchup_worker.py 2>/dev/null || true

start_if_missing "alchemy5172" "v5172_alchemy_turbo_engine.py" \
  "MEMECOIN_V517_BASE_SAMPLE_MOD=8 MEMECOIN_V517_HOT_TTL_S=180 MEMECOIN_V517_BASE_RPS=12 MEMECOIN_V517_MAX_RPS=30 MEMECOIN_V517_WORKERS=16 python3 -u v5172_alchemy_turbo_engine.py" \
  "runtime_logs/alchemy5172.log"

start_if_missing "decoder" "v52_decode_features.py" \
  "python3 -u v52_decode_features.py" \
  "runtime_logs/decoder.log"

start_if_missing "v64" "v64_next_fill_future_only_arena.py" \
  "python3 -u v64_next_fill_future_only_arena.py" \
  "runtime_logs/v64.log"

# Prefer newest dashboard if present; do not duplicate an existing dashboard on 8792.
if pgrep -f "v651_visual_command_center.py|v65_visual_command_center.py" >/dev/null 2>&1; then
  echo "[already] dashboard"
elif [[ -f v651_visual_command_center.py ]]; then
  start_if_missing "dashboard" "v651_visual_command_center.py" "python3 -u v651_visual_command_center.py" "runtime_logs/dashboard.log"
elif [[ -f v65_visual_command_center.py ]]; then
  start_if_missing "dashboard" "v65_visual_command_center.py" "python3 -u v65_visual_command_center.py" "runtime_logs/dashboard.log"
fi

echo
echo "=== TURBO STACK ==="
ps aux | grep -E "v5172_alchemy_turbo|v52_decode_features|v64_next_fill|v651_visual|v65_visual" | grep -v grep || true

echo
echo "Alchemy live log: tail -f runtime_logs/alchemy5172.log"
echo "V6.4 live log:    tail -f runtime_logs/v64.log"
echo "Dashboard:        http://127.0.0.1:8792"

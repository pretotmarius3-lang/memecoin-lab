#!/bin/zsh
set -u

ROOT="$HOME/memecoin_lab"
LOGDIR="$ROOT/runtime_logs"
PIDDIR="$ROOT/runtime_pids"
mkdir -p "$LOGDIR" "$PIDDIR"
cd "$ROOT" || exit 1

if [ -f "$ROOT/.env" ]; then
  set -a
  source "$ROOT/.env"
  set +a
fi

start_one() {
  name="$1"
  script="$2"
  logfile="$LOGDIR/$name.log"
  pidfile="$PIDDIR/$name.pid"

  if [ -f "$pidfile" ]; then
    oldpid=$(cat "$pidfile" 2>/dev/null || true)
    if [ -n "${oldpid:-}" ] && kill -0 "$oldpid" 2>/dev/null; then
      echo "[already running] $name pid=$oldpid"
      return 0
    fi
    rm -f "$pidfile"
  fi

  nohup python3 -u "$script" >> "$logfile" 2>&1 &
  pid=$!
  echo "$pid" > "$pidfile"
  sleep 0.5
  if kill -0 "$pid" 2>/dev/null; then
    echo "[started] $name pid=$pid log=$logfile"
  else
    echo "[FAILED] $name — see $logfile"
  fi
}

echo "============================================================"
echo "MEMECOIN LAB — START FULL STACK"
echo "============================================================"

start_one "live" "v511_live_priority_collector.py"
start_one "catchup" "v531_catchup_worker.py"
start_one "decoder" "v52_decode_features.py"
start_one "v61" "v61_economic_champion_consolidator.py"
start_one "v62" "v62_future_only_economic_champion_arena.py"
start_one "visual" "v51_visual_lab.py"

echo
echo "RUNNING PROCESSES"
for f in "$PIDDIR"/*.pid(N); do
  name=${f:t:r}
  pid=$(cat "$f" 2>/dev/null || true)
  if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
    echo "  $name  pid=$pid"
  fi
done

echo
echo "Logs: $LOGDIR"
echo "Stop all: ./stop_memecoin_stack.sh"

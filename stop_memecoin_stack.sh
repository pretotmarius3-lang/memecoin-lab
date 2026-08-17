#!/bin/zsh
ROOT="$HOME/memecoin_lab"
PIDDIR="$ROOT/runtime_pids"

echo "MEMECOIN LAB — STOP FULL STACK"

for f in "$PIDDIR"/*.pid(N); do
  name=${f:t:r}
  pid=$(cat "$f" 2>/dev/null || true)
  if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    echo "[stopped] $name pid=$pid"
  else
    echo "[not running] $name"
  fi
  rm -f "$f"
done

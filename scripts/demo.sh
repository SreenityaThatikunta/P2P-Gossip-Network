#!/usr/bin/env bash
# Spins up a full network locally (N seeds + M peers), lets it gossip,
# kills one peer to trigger two-level dead-node consensus, then reports.
#
#   ./scripts/demo.sh            # 3 seeds, 4 peers, 45s run
#   ./scripts/demo.sh 5 8 60     # 5 seeds, 8 peers, 60s run

set -euo pipefail

NUM_SEEDS=${1:-3}
NUM_PEERS=${2:-4}
RUNTIME=${3:-45}
SEED_BASE=5000
PEER_BASE=6000
IP=127.0.0.1

cd "$(dirname "$0")/.."

echo "==> Building"
make clean NUM_SEEDS="$NUM_SEEDS" >/dev/null
make >/dev/null

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "==> Starting $NUM_SEEDS seed nodes"
for i in $(seq 1 "$NUM_SEEDS"); do
  ./seed $((SEED_BASE + i)) >/dev/null 2>&1 &
  pids+=($!)
done
sleep 2

echo "==> Starting $NUM_PEERS peer nodes"
for i in $(seq 0 $((NUM_PEERS - 1))); do
  ./peer $((PEER_BASE + i)) >/dev/null 2>&1 &
  pids+=($!)
  sleep 0.5
done

echo "==> Gossiping for $((RUNTIME / 3))s"
sleep $((RUNTIME / 3))

victim_pid=${pids[$((NUM_SEEDS + NUM_PEERS - 1))]}
victim_port=$((PEER_BASE + NUM_PEERS - 1))
echo "==> Killing peer $victim_port (pid $victim_pid) to trigger dead-node consensus"
kill -9 "$victim_pid" 2>/dev/null || true

echo "==> Waiting $((RUNTIME * 2 / 3))s for detection + consensus"
sleep $((RUNTIME * 2 / 3))

echo
echo "==> Dead-node consensus as seen by the seeds"
grep -h "DEAD_\|REMOVE_PEER" seed_*_output.txt | sort | head -20 || echo "(none yet)"

echo
echo "==> Gossip delivered per peer"
for f in peer_*_output.txt; do
  printf '%-28s %s first-time gossip msgs\n' "$f" "$(grep -c 'first time' "$f" || true)"
done

echo
echo "==> Full logs: seed_<port>_output.txt / peer_<port>_output.txt"

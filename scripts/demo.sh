#!/usr/bin/env bash
# Spins up a full network locally (N seeds + M peers), lets it gossip,
# kills one peer to trigger two-level dead-node consensus, then reports.
#
#   ./scripts/demo.sh            # 3 seeds, 4 peers, 20s of gossip
#   ./scripts/demo.sh 5 8 30     # 5 seeds, 8 peers, 30s of gossip
#
# After the gossip phase one peer is killed and the script polls for the
# seed-level DEAD_COMMIT rather than sleeping a fixed amount.

set -euo pipefail

NUM_SEEDS=${1:-3}
NUM_PEERS=${2:-4}
RUNTIME=${3:-20}
DETECT_TIMEOUT=${DETECT_TIMEOUT:-120}
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

echo "==> Gossiping for ${RUNTIME}s"
sleep "$RUNTIME"

victim_pid=${pids[$((NUM_SEEDS + NUM_PEERS - 1))]}
victim_port=$((PEER_BASE + NUM_PEERS - 1))
echo "==> Killing peer $victim_port (pid $victim_pid) to trigger dead-node consensus"
kill -9 "$victim_pid" 2>/dev/null || true
killed_at=$(date +%s)

# Detection is quorum-gated: a peer with a direct connection to the victim
# suspects it within a second, but a peer that must time out its pings needs
# roughly 3 + 5*degree seconds per pass and three passes to cross the
# threshold. Poll for the commit instead of guessing a fixed wait.
echo "==> Waiting up to ${DETECT_TIMEOUT}s for detection + two-level consensus"
while :; do
  if grep -qh "DEAD_COMMIT" seed_*_output.txt 2>/dev/null; then
    echo "    consensus reached after $(($(date +%s) - killed_at))s"
    break
  fi
  if [ $(($(date +%s) - killed_at)) -ge "$DETECT_TIMEOUT" ]; then
    echo "    no consensus within ${DETECT_TIMEOUT}s — see the logs below"
    break
  fi
  sleep 2
done

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

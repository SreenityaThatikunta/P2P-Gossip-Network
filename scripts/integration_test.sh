#!/usr/bin/env bash
# Integration test: boots a real network, then asserts the three properties
# the protocol is supposed to guarantee.
#
#   1. every peer is admitted by a quorum of seeds
#   2. every peer receives every other live peer's gossip at least once
#   3. a killed peer is evicted by peer-level then seed-level consensus
#
#   ./scripts/integration_test.sh            # 3 seeds, 4 peers
#   ./scripts/integration_test.sh 5 6        # 5 seeds, 6 peers
#
# Env: GOSSIP_WAIT (default 25s), DETECT_TIMEOUT (default 180s).

set -uo pipefail

NUM_SEEDS=${1:-3}
NUM_PEERS=${2:-4}
GOSSIP_WAIT=${GOSSIP_WAIT:-25}
DETECT_TIMEOUT=${DETECT_TIMEOUT:-180}
SEED_BASE=5000
PEER_BASE=6000

cd "$(dirname "$0")/.."

failures=0
pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; failures=$((failures + 1)); }

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "==> Building"
make clean NUM_SEEDS="$NUM_SEEDS" >/dev/null || { echo "build config failed"; exit 1; }
make >/dev/null || { echo "build failed"; exit 1; }

echo "==> Starting $NUM_SEEDS seeds and $NUM_PEERS peers"
for i in $(seq 1 "$NUM_SEEDS"); do
  ./seed $((SEED_BASE + i)) >/dev/null 2>&1 &
  pids+=($!)
done
sleep 3
for i in $(seq 0 $((NUM_PEERS - 1))); do
  ./peer $((PEER_BASE + i)) >/dev/null 2>&1 &
  pids+=($!)
  sleep 0.5
done

echo "==> Gossiping for ${GOSSIP_WAIT}s"
sleep "$GOSSIP_WAIT"

victim_port=$((PEER_BASE + NUM_PEERS - 1))
victim_pid=${pids[$((NUM_SEEDS + NUM_PEERS - 1))]}

echo
echo "==> Assertion 1: every peer admitted by seed consensus"
for i in $(seq 0 $((NUM_PEERS - 1))); do
  port=$((PEER_BASE + i))
  if grep -qh "REG_COMMIT: Peer 127.0.0.1:$port registered" seed_*_output.txt 2>/dev/null; then
    pass "peer $port committed by seed consensus"
  else
    fail "peer $port never reached REG_COMMIT on any seed"
  fi
done

echo
echo "==> Assertion 2: gossip reaches every peer from every other peer"
for i in $(seq 0 $((NUM_PEERS - 1))); do
  port=$((PEER_BASE + i))
  log="peer_${port}_output.txt"
  [ -f "$log" ] || { fail "$log missing"; continue; }
  missing=""
  for j in $(seq 0 $((NUM_PEERS - 1))); do
    other=$((PEER_BASE + j))
    [ "$other" = "$port" ] && continue
    # message ids are timestamp:IP:port:msgNum — match on the origin port
    if ! grep -q "first time): [0-9]*:127\.0\.0\.1:$other:" "$log"; then
      missing="$missing $other"
    fi
  done
  if [ -z "$missing" ]; then
    pass "peer $port received gossip from all $((NUM_PEERS - 1)) others"
  else
    fail "peer $port received nothing from:$missing"
  fi
done

echo
echo "==> Killing peer $victim_port to trigger dead-node consensus"
kill -9 "$victim_pid" 2>/dev/null || true
killed_at=$(date +%s)

while :; do
  committed=$(grep -lh "DEAD_COMMIT.*127.0.0.1:$victim_port" seed_*_output.txt 2>/dev/null | wc -l | tr -d ' ')
  [ "${committed:-0}" -ge 1 ] && break
  if [ $(($(date +%s) - killed_at)) -ge "$DETECT_TIMEOUT" ]; then break; fi
  sleep 2
done
elapsed=$(($(date +%s) - killed_at))

echo
echo "==> Assertion 3: two-level consensus evicts the dead peer"
if grep -qh "SUSPICION: 127.0.0.1:$victim_port" peer_*_output.txt 2>/dev/null; then
  pass "peer-level suspicion raised"
else
  fail "no peer ever suspected $victim_port"
fi
if grep -qh "DEAD_PROPOSAL.*127.0.0.1:$victim_port" seed_*_output.txt 2>/dev/null; then
  pass "seed-level proposal issued"
else
  fail "no seed proposed removal of $victim_port"
fi
quorum=$(((NUM_SEEDS / 2) + 1))
committed=$(grep -l "DEAD_COMMIT.*127.0.0.1:$victim_port" seed_*_output.txt 2>/dev/null | wc -l | tr -d ' ')
if [ "${committed:-0}" -ge "$quorum" ]; then
  pass "DEAD_COMMIT on $committed/$NUM_SEEDS seeds (quorum $quorum) after ${elapsed}s"
else
  fail "DEAD_COMMIT on only ${committed:-0}/$NUM_SEEDS seeds (need $quorum) after ${elapsed}s"
fi

echo
if [ "$failures" -eq 0 ]; then
  echo "All assertions passed."
  exit 0
fi
echo "$failures assertion(s) failed. Logs:"
for f in seed_*_output.txt peer_*_output.txt; do
  echo "----- $f"; tail -25 "$f"
done
exit 1

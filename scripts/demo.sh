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
gossip_start=$(date +%s)
while :; do
  elapsed=$(($(date +%s) - gossip_start))
  [ "$elapsed" -ge "$RUNTIME" ] && break
  # grep exits 1 with no matches, which would trip `set -e` before the first
  # message lands, so swallow the status and keep the count
  delivered=$(cat peer_*_output.txt 2>/dev/null | grep -c "first time" || true)
  printf '\r    t=%-5s messages delivered across peers: %-5s' "${elapsed}s" "${delivered:-0}"
  sleep 4
done
delivered=$(cat peer_*_output.txt 2>/dev/null | grep -c "first time" || true)
printf '\r    t=%-5s messages delivered across peers: %-5s\n' "${RUNTIME}s" "${delivered:-0}"

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
# Every seed logs the same events within the same second, so sorting the
# merged logs just orders them alphabetically (commit before proposal).
# Follow one seed's own log instead: its line order is the protocol order.
# prefer a seed that proposed the removal: a follower's log has only the vote
# it cast and the commit it received, not the full sequence
consensus_log=$(grep -l "DEAD_PROPOSAL" seed_*_output.txt 2>/dev/null | head -1 || true)
[ -n "$consensus_log" ] || consensus_log=$(grep -l "DEAD_" seed_*_output.txt 2>/dev/null | head -1 || true)
if [ -n "$consensus_log" ]; then
  echo "==> Dead-node consensus as seen by ${consensus_log%_output.txt}"
  sed 's/^\[[^]]*\] //' "$consensus_log" \
    | grep "DEAD_\|REMOVE_PEER" \
    | awk '!seen[$0]++ { print "    " $0 }'
else
  echo "==> Dead-node consensus as seen by the seeds"
  echo "    (none)"
fi

echo
echo "==> Gossip delivered per peer"
for f in peer_*_output.txt; do
  printf '%-28s %s first-time gossip msgs\n' "$f" "$(grep -c 'first time' "$f" || true)"
done

echo
echo "==> Full logs: seed_<port>_output.txt / peer_<port>_output.txt"

# Gossip-Based P2P Network

A gossip-based peer-to-peer network with consensus-driven membership management,
power-law degree distribution, and two-level dead-node detection.

---

## File Structure

```
gossip_p2p/
├── common.h        # Shared constants, message tags, data structures, utilities
├── seed.cpp        # Seed node implementation
├── peer.cpp        # Peer node implementation
├── config.txt      # Seed node IP:Port list (shared by all nodes)
├── Makefile        # Build system
└── README.md       # This file
```

Output files are written automatically:
- `seed_<port>_output.txt` — seed node log
- `peer_<port>_output.txt` — peer node log

---

## Build

```bash
make          # builds both 'seed' and 'peer' binaries
make clean    # removes binaries and log files
```

Requirements: `g++` with C++17 support, Linux/macOS (POSIX sockets).

---

## Configuration

Edit `config.txt` to list all seed nodes:

```
127.0.0.1:5001
127.0.0.1:5002
127.0.0.1:5003
```

- One `IP:Port` per line (lines starting with `#` are comments).
- The same `config.txt` is used by **all** seed and peer nodes.
- For multi-machine deployments, replace `127.0.0.1` with actual IP addresses.

---

## Running (Local Testing)

Open separate terminals for each node. All commands assume the binary and
`config.txt` are in the same working directory.

### Step 1 — Start seed nodes (one per terminal)

```bash
./seed 5001   # Terminal 1

./seed 5002   # Terminal 2

./seed 5003   # Terminal 3
```

### Step 2 — Start peer nodes (one per terminal)

```bash
./peer 6000   # Terminal 4

./peer 6001   # Terminal 5

./peer 6002   # Terminal 6

./peer 6003   # Terminal 6
```

Peers register with a quorum of seeds, collect peer lists, select neighbors
following a power-law distribution, and start gossiping every 5 seconds.
Each peer generates up to 10 gossip messages.

### Step 3 — Simulate a peer failure

Kill one of the peer processes (`Ctrl+C` or `kill <pid>`). The remaining
peers will detect the failure via missed pings, run peer-level consensus,
and report the dead node to seed nodes. Seeds run their own consensus and
remove the peer from their Peer Lists.

---

## Running on Multiple Machines

1. Edit `config.txt` with the **actual IP addresses** of the seed machines.
2. Copy `config.txt`, `seed`, and `peer` binaries to all machines.
3. Set the `SELF_IP` environment variable on each machine if `127.0.0.1`
   would be resolved incorrectly:

```bash
export SELF_IP=192.168.1.10
./seed 5000
```

```bash
export SELF_IP=192.168.1.20
./peer 6000
```

---

## Protocol Overview

### Registration (Seed-Level Consensus)

```
Peer  →  Seed_A : REGISTER:IP:Port
Seed_A → Seed_B,C : REGISTER_PROPOSAL:IP:Port:SeedA_IP:SeedA_Port
Seed_B → Seed_A : REGISTER_VOTE:IP:Port:SeedB_IP:SeedB_Port:YES
          (majority reached)
Seed_A → Seed_B,C : REGISTER_COMMIT:IP:Port
Seed_A → Peer  : REGISTER_OK:IP:Port
Peer  → Seed_A : GET_PEER_LIST
Seed_A → Peer  : PEER_LIST:IP1:P1,IP2:P2,...
```

### Gossip Dissemination

```
Peer generates: <timestamp>:<IP>:<MsgNum>
Peer → all neighbors : GOSSIP:<payload>
Neighbor (first receipt): records in ML, forwards to all neighbors except sender
Neighbor (duplicate): silently drops
```

### Dead-Node Detection (Two-Level Consensus)

**Level 1 — Peer consensus:**
```
Peer_A pings Peer_X → no PONG (×3 missed)
Peer_A → all neighbors : SUSPICION:X_IP:X_Port:A_IP:A_Port
Peers collect suspicion reports; when majority confirm:
  → remove X from neighbor set
  → send Dead Node report to seeds
```

**Level 2 — Seed consensus:**
```
Peer_A → Seeds : Dead Node:X_IP:X_Port:timestamp:A_IP
Seed_1 → Seed_2,3 : DEAD_PROPOSAL:X_IP:X_Port:Seed1_IP:Seed1_Port
Seed_2 → Seed_1 : DEAD_VOTE:X_IP:X_Port:Seed2_IP:Seed2_Port:YES
          (majority reached)
Seed_1 → Seed_2,3 : DEAD_COMMIT:X_IP:X_Port
Seeds → all peers : REMOVE_PEER:X_IP:X_Port
```

---

## Power-Law Degree Distribution

Neighbor count `k` is sampled from a Zipf distribution (α = 1.5), and actual
neighbors are chosen via **preferential attachment** — nodes with higher
existing degree are more likely to be selected. This produces a power-law
(scale-free) degree distribution where a few hub nodes have many connections
and most nodes have few.

The topology is maintained as an in-memory adjacency list (`g_topology`)
updated on every neighbor connect/disconnect and dead-node removal.

---

## Security Analysis

| Attack | Mitigation |
|---|---|
| **False dead report** (single peer) | Peer-level majority required before reporting to seeds |
| **Colluding peers** | Seed-level majority independently confirms before removal |
| **Sybil registration** | Seeds exchange proposals/votes; single seed cannot unilaterally admit |
| **False suspicion flood** | Suspicion deduplicated per suspected node; quorum threshold |
| **Replay attacks** | Each message carries a timestamp; ML hash deduplication |

---

## Logging

All events are written to both **stdout** and an **output file**:

| Node | Output file |
|---|---|
| `seed 5001` | `seed_5001_output.txt` |
| `peer 6001` | `peer_6001_output.txt` |

Logged events include:
- Seed: registration proposals, consensus votes, commits, dead-node removals
- Peer: received peer lists, first-time gossip messages (with timestamps/sender IPs),
  confirmed dead-node reports

Ping/pong messages are **not** logged.

#!/usr/bin/env bash
# scripts/chaos.sh -- CLI wrapper for BoilerPulse's chaos/failure-injection
# admin server (internal/admin, spec §23/§34). Talks directly to each
# node's admin port (see configs/cluster/node-{1,2,3}.yaml's admin_addr) --
# not through the gateway -- so it keeps working even when the gateway
# itself is the thing you're testing against. Pairs with `make cluster`'s
# .cluster/*.pid convention: `restart` relaunches the node binary the same
# way `make cluster` originally started it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Deliberately not using bash associative arrays (declare -A, bash 4+):
# macOS ships bash 3.2 by default and plenty of dev machines never
# upgrade it, so this uses plain case statements to stay portable.

TOKEN="${BOILERPULSE_ADMIN_TOKEN:-}"
if [ -z "$TOKEN" ] && [ -f .cluster/admin.token ]; then
  TOKEN="$(cat .cluster/admin.token)"
fi
if [ -z "$TOKEN" ]; then
  echo "error: no admin token found. Set BOILERPULSE_ADMIN_TOKEN, or run 'make cluster' first (it writes .cluster/admin.token)." >&2
  exit 1
fi

usage() {
  cat <<USAGE
Usage: $(basename "$0") <command> [args]

Commands:
  status [node]          show fault + raft status (all 3 nodes, or just one)
  kill <node>             crash a node (ungraceful exit -- a real process death, not a graceful shutdown)
  restart <node>          relaunch a previously-killed node (same config, same port, picks up its own data)
  partition <node>        isolate a node from the rest of the cluster (both directions)
  heal <node>             clear a partition on one node
  latency <node> <ms>     add one-way RPC latency, in milliseconds
  drop <node> <rate>      drop a fraction (0.0-1.0) of RPCs to/from a node
  restore <node>          clear every injected fault on one node
  restore-all             clear every injected fault on every node

<node> is node-1, node-2, or node-3 (see configs/cluster/).

Examples:
  scripts/chaos.sh status
  scripts/chaos.sh kill node-2
  scripts/chaos.sh restart node-2
  scripts/chaos.sh partition node-1
  scripts/chaos.sh latency node-3 500
  scripts/chaos.sh restore-all
USAGE
  exit 1
}

admin_addr() {
  local node="$1"
  local port
  case "$node" in
    node-1) port=7080 ;;
    node-2) port=7081 ;;
    node-3) port=7082 ;;
    *)
      echo "error: unknown node '$node' (want node-1, node-2, or node-3)" >&2
      exit 1
      ;;
  esac
  echo "http://localhost:$port"
}

config_for() {
  local node="$1"
  case "$node" in
    node-1) echo "configs/cluster/node-1.yaml" ;;
    node-2) echo "configs/cluster/node-2.yaml" ;;
    node-3) echo "configs/cluster/node-3.yaml" ;;
    *)
      echo "error: unknown node '$node' (want node-1, node-2, or node-3)" >&2
      exit 1
      ;;
  esac
}

call() {
  local node="$1" method="$2" path="$3" body="${4:-}"
  local addr
  addr="$(admin_addr "$node")"
  if [ -n "$body" ]; then
    curl -sS -X "$method" "$addr$path" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$body"
  else
    curl -sS -X "$method" "$addr$path" -H "Authorization: Bearer $TOKEN"
  fi
  echo
}

cmd="${1:-}"
[ -z "$cmd" ] && usage

case "$cmd" in
  status)
    if [ -n "${2:-}" ]; then
      call "$2" GET /status
    else
      for node in node-1 node-2 node-3; do
        echo "== $node =="
        call "$node" GET /status
      done
    fi
    ;;
  kill)
    [ -n "${2:-}" ] || usage
    echo "killing $2..."
    call "$2" POST /kill
    ;;
  restart)
    [ -n "${2:-}" ] || usage
    node="$2"
    config="$(config_for "$node")"
    if [ ! -x ./bin/node ]; then
      echo "error: ./bin/node not built -- run 'make build' first" >&2
      exit 1
    fi
    mkdir -p .cluster
    BOILERPULSE_CONFIG="$config" BOILERPULSE_ADMIN_TOKEN="$TOKEN" ./bin/node > ".cluster/$node.log" 2>&1 &
    pid=$!
    echo "$pid" > ".cluster/$node.pid"
    echo "restarted $node (pid $pid), logs in .cluster/$node.log"
    ;;
  partition)
    [ -n "${2:-}" ] || usage
    call "$2" POST /fault '{"partitioned":true}'
    ;;
  heal)
    [ -n "${2:-}" ] || usage
    call "$2" POST /fault '{"partitioned":false}'
    ;;
  latency)
    if [ -z "${2:-}" ] || [ -z "${3:-}" ]; then usage; fi
    call "$2" POST /fault "{\"latency_ms\":$3}"
    ;;
  drop)
    if [ -z "${2:-}" ] || [ -z "${3:-}" ]; then usage; fi
    call "$2" POST /fault "{\"drop_rate\":$3}"
    ;;
  restore)
    [ -n "${2:-}" ] || usage
    call "$2" POST /restore
    ;;
  restore-all)
    for node in node-1 node-2 node-3; do
      call "$node" POST /restore
    done
    ;;
  *)
    usage
    ;;
esac

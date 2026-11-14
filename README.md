# Distributed auction

Three independent Go processes expose the same gRPC auction API. Two processes
must participate in each successful operation, allowing one process to stop.

## Start an auction on macOS

Stop all existing servers with Ctrl+C. Open four terminals in this project
directory: three for the server processes and one for client commands.

`go run ./cmd/server` compiles the current server code and starts it. Each terminal
runs a separate process. There is no separate build command to remember after code
changes. The server's `-id` selects its address from the shared membership list:

| Node ID | Address | Terminal |
| --- | --- | --- |
| 0 | localhost:50051 | 1 |
| 1 | localhost:50052 | 2 |
| 2 | localhost:50053 | 3 |

### Terminal 1: create the deadline and start node 0

Run the `date` command **once per fresh auction**, only in this terminal:

```sh
date -u -v+3M '+%Y-%m-%dT%H:%M:%SZ' > /tmp/mandatory5-deadline

go run ./cmd/server -id 0 -deadline "$(cat /tmp/mandatory5-deadline)"
```

The macOS `date` command saves a UTC timestamp three minutes ahead in the system's
`/tmp` folder, not the project's `tmp` folder. The countdown starts when this
command runs, not when all nodes are ready. `$(cat /tmp/mandatory5-deadline)` reads
that timestamp and passes it to the server. All nodes must use the same deadline.

### Terminal 2: start node 1

```sh
go run ./cmd/server -id 1 -deadline "$(cat /tmp/mandatory5-deadline)"
```

### Terminal 3: start node 2

```sh
go run ./cmd/server -id 2 -deadline "$(cat /tmp/mandatory5-deadline)"
```

Check that all three startup logs show the same deadline. Do not regenerate the
file during the auction: running processes retain the timestamp they loaded.
Each node ID must belong to exactly one process, and membership must match on all
nodes. The default membership is the ordered list shown above.

## Manual verification

The user confirmed this `go run` sequence works after the Result forwarding fix.
Run the steps in order. The pre-deadline bids must finish before the three minutes
expire; otherwise rejection is expected. Use a fresh auction if setup takes too long.

### Terminal 4: normal bidding

```sh
go run ./cmd/client -addr localhost:50053 bid alice 30
# Expected: success

go run ./cmd/client -addr localhost:50052 bid bob 20
# Expected: fail

go run ./cmd/client -addr localhost:50051 bid bob 30
# Expected: fail - equal bids are rejected.

go run ./cmd/client -addr localhost:50053 result
# Expected: Auction open. Highest bidder: alice, bid: 30
```

### Stop the leader

Press **Ctrl+C in Terminal 1 only**. Leave Terminals 2 and 3 running. In Terminal 4,
before the deadline:

```sh
go run ./cmd/client -addr localhost:50053 result
# Expected: Auction open. Highest bidder: alice, bid: 30

go run ./cmd/client -addr localhost:50052 bid bob 40
# Expected: success

go run ./cmd/client -addr localhost:50053 result
# Expected: Auction open. Highest bidder: bob, bid: 40
```

### Wait for expiry in Terminal 4

Paste the entire block and wait until **Terminal 4 prints DEADLINE PASSED**:

```sh
python3 - <<'PYWAIT'
from datetime import datetime, timezone
from pathlib import Path
import time

deadline = datetime.fromisoformat(
    Path("/tmp/mandatory5-deadline").read_text().strip().replace("Z", "+00:00")
)
seconds = max(0, (deadline - datetime.now(timezone.utc)).total_seconds())
print(f"Waiting {seconds:.0f} seconds...", flush=True)
time.sleep(seconds + 1)
print("DEADLINE PASSED", flush=True)
PYWAIT
```

This helper only waits for the remaining time; it does not extend the deadline or
close the auction itself. If the deadline is already past, it returns after about
one second. Python is only needed for this optional helper, not for the Go servers.
You can instead wait until the timestamp shown in the server startup logs.

After DEADLINE PASSED, run in Terminal 4:

```sh
go run ./cmd/client -addr localhost:50053 result
# Expected: Auction ended. Winner: bob, bid: 40

go run ./cmd/client -addr localhost:50053 bid alice 50
# Expected: fail

go run ./cmd/client -addr localhost:50052 result
# Expected: Auction ended. Winner: bob, bid: 40
```

### Loss of a majority

Press **Ctrl+C in Terminal 2**, leaving only node 2. In Terminal 4:

```sh
go run ./cmd/client -addr localhost:50053 result
# Expected: Unavailable error - fewer than two promises.

go run ./cmd/client -addr localhost:50053 bid alice 60
# Expected: exception - one node cannot confirm a bid.
```

A query must still obtain a majority after the auction ends. This scenario exceeds
the required single failure and checks that one node cannot confirm an answer alone.

### Follower failure in a fresh auction

Stop the remaining node. Repeat the startup steps with a new shared deadline,
then stop **node 2 in Terminal 3**. Leave nodes 0 and 1 running. In Terminal 4,
before expiry:

```sh
go run ./cmd/client -addr localhost:50052 bid alice 10
# Expected: success

go run ./cmd/client -addr localhost:50051 result
# Expected: Auction open. Highest bidder: alice, bid: 10
```

Leave crashed nodes stopped throughout a scenario. Stop all nodes before starting
a fresh auction. A client contacting a stopped node gets a connection error; use
a live address. A bid interrupted by a crash can have an unknown outcome, so query
the state before deciding whether to submit another bid. Uncertain bids are not
automatically retried, and exactly-once request tracking is not implemented.

### Requests from different clients

In a fresh auction, issue bids through different nodes in separate client terminals:

```sh
go run ./cmd/client -addr localhost:50052 bid alice 10
```

```sh
go run ./cmd/client -addr localhost:50053 bid bob 20
```

Then query either node. If both requests complete normally before the deadline,
Bob must lead with 20. Alice may succeed or fail depending on which bid was ordered
first. A timeout or interrupted proposal is reported as an exception, not a normal
bid rejection.

## How the coordination works

The first reachable node in the ordered membership is the preferred coordinator.
Other nodes forward requests to it. This is a routing preference, not a persistent
elected leader or the Bully algorithm. Failure detection happens on requests using
bounded Ping RPCs; a process may be briefly suspected if it is slow.

A successful Ping does not guarantee that the coordinator will finish the next
request. Result forwarding has a one-second deadline, within the overall
2.5-second server request budget. If forwarding returns Unavailable or
DeadlineExceeded while time remains, the receiving node runs the query's two
majority phases itself. It never substitutes a local snapshot for a confirmed
result. The client still defaults to a three-second timeout. Bids are not retried
this way because a missing response can conceal an accepted bid.

Safety comes from a two-phase, state-based CASPaxos protocol, rather than the Ping:

1. **Prepare:** the coordinator generates a unique ballot `(counter, node ID)`.
   Each replica promises to reject smaller ballots and returns its last accepted
   state. The coordinator needs two promises and selects the state with the highest
   accepted ballot among those replies.
2. **Accept:** it applies the bid or query to that recovered state, then sends the
   resulting snapshot with the ballot. It returns a confirmed response only after
   two replicas accept it. Even rejected bids register valid bidder IDs, so these
   state changes are replicated too.

Every pair of two-node majorities intersects. Recovery therefore encounters state
that preserves previously confirmed operations. Promises fence off outdated
proposals, including when different nodes temporarily act as coordinators.
An unsuccessful operation can have changed some replicas; its response is an
exception, never a claim that the bid definitely failed.

Queries use both phases too. They recover current state and confirm it on a
majority before returning. Once observed, auction closure is included in the
replicated state and cannot be undone by a replacement coordinator.

This implementation sends to all three replicas, sequentially, but requires only
two confirmations. A rejected prepare can learn the higher ballot and retry within
the request deadline, before applying the auction operation. Once Accept has been
sent for a bid, that bid is never retried automatically: its outcome could be unknown.
Queries can be repeated safely, including after a forwarding timeout, but every
successful result still requires majority confirmation.
Competing proposals or exhausted request deadlines may produce exceptions.
It does not perform background replication or repair. A lagging replica receives the current snapshot in later
operations when reachable.

Protocol reference: Denis Rystsov,
[CASPaxos: Replicated State Machines without logs, section 2.2](https://arxiv.org/html/1802.07000).

## Scope and assumptions

- Failure-stop: a stopped node stays stopped for that auction. State and ballot
  promises are in memory. **Do not restart an individual node into the running
  auction**; crash recovery would require durable promises and accepted state.
  Stop all nodes and generate a new deadline to start a fresh auction.
- All demonstration processes share the same host clock. Separate hosts need
  synchronized clocks for the shared wall-clock deadline.
- Internal RPC timeouts are 400 ms. Nonfailed nodes must respond within that bound
  for predictable availability; adjust it and the API/client timeouts for slower
  networks. gRPC uses reliable ordered TCP transport.
- Bidder IDs are client-supplied names, not authenticated accounts. Internal RPCs
  trust the configured nonmalicious nodes; this is a local assignment deployment.
- Nodes are separate OS processes. gRPC's internal goroutines handle concurrency
  within each process; replicas are not simulated as goroutines.

## Code map

- `auction/auction.go`: auction rules and snapshot conversion.
- `proto/auction.proto`: public and internal gRPC messages.
- `cmd/server/main.go`: configuration, connections, and server startup.
- `cmd/server/api.go`: preferred-coordinator selection, forwarding, Bid and Result.
- `cmd/server/consensus.go`: promises, accepted snapshots, and majority rounds.
- `cmd/client/main.go`: command-line bidding and queries.

Build with `go build ./...`. Regenerate protobuf bindings after changing the schema:

```sh
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/auction.proto
```

No automated tests are included, as requested.

# Distributed auction

A Go and gRPC auction system for the distributed systems assignment. It runs on
three server processes and can continue after one server stops.

## Start an auction on macOS

Stop all existing servers with Ctrl+C. Open four terminals in this project
directory: three for the server processes and one for client commands.

`go run` compiles and starts the current code. Nodes 0, 1 and 2 use ports
50051, 50052 and 50053.

### Terminal 1: create the deadline and start node 0

Run the `date` command **once per fresh auction**, only in this terminal:

```sh
date -u -v+3M '+%Y-%m-%dT%H:%M:%SZ' > /tmp/mandatory5-deadline

go run ./cmd/server -id 0 -deadline "$(cat /tmp/mandatory5-deadline)"
```

This sets the deadline three minutes from now. All nodes read the same timestamp
from `/tmp/mandatory5-deadline`. The three minutes start when you run `date`.

### Terminal 2: start node 1

```sh
go run ./cmd/server -id 1 -deadline "$(cat /tmp/mandatory5-deadline)"
```

### Terminal 3: start node 2

```sh
go run ./cmd/server -id 2 -deadline "$(cat /tmp/mandatory5-deadline)"
```

Check that all three servers print the same deadline. Do not recreate the deadline
file during an auction.

## Trying it out

Run these steps in order. Place the initial bids before the three minutes are up.

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

The script just waits until the saved deadline passes. It does not change the
auction. You can also wait yourself instead.

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

One node alone cannot confirm a result, even after the auction ends.

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

## How it works

Clients can contact any live node. Requests normally go to node 0. If it stops,
node 1 can take over. Before confirming an operation, the coordinator recovers the
latest state and gets agreement from at least two nodes. Queries also need two
nodes, so they do not return an outdated local result.

Replication uses CASPaxos. The protocol and correctness arguments are explained in
the [report](output/pdf/distributed-auction-report.md), including the source reference.

If forwarding a query times out, the receiving node can try to confirm the result
itself. Bids are not automatically retried: an exception can mean that the bid was
accepted but its response was lost.

## Limitations

- A stopped node must stay stopped for that auction. State is kept in memory.
  Stop all nodes and create a new deadline to start again.
- Use a live node's address after a crash; the client does not switch addresses.
- The examples run on one computer and share its clock. Separate computers need
  synchronized clocks and the same membership and deadline settings.
- Peer requests have a 400 ms timeout, so slow connections can cause exceptions.
- Bidder names are supplied by clients. There is no login or payment system.

## Files

- `auction/auction.go`: auction rules and snapshot conversion.
- `proto/auction.proto`: public and internal gRPC messages.
- `cmd/server/main.go`: configuration, connections, and server startup.
- `cmd/server/api.go`: preferred-coordinator selection, forwarding, Bid and Result.
- `cmd/server/consensus.go`: promises, accepted snapshots, and majority rounds.
- `cmd/client/main.go`: command-line bidding and queries.

To check that the project builds, run `go build ./...`. If you change the proto
file, regenerate the Go bindings:

```sh
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/auction.proto
```


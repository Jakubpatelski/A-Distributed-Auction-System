# Runtime verification

Nine temporary scenarios passed with the Go race detector (`go test -race -v ./cmd/server`).
The source was copied unchanged to `/tmp/mandatory5-check.YsxbWZ`; verification code
was added only to that temporary copy, not to the assignment project.
The adjacent `runtime-checks.log` contains the actual results.

The checks run three instances of the actual server through gRPC's in-memory
`bufconn` transport. Stopping an instance closes its gRPC server and listener.
This exercises handlers, forwarding, serialization, promises, majority recovery,
and real-clock expiry, but does not verify separate OS processes, TCP sockets,
terminal Ctrl+C handling, or the literal CLI commands. Local TCP binding is blocked
by the tool environment (`Operation not permitted`).

Passed scenarios:

1. The documented bid/result sequence, coordinator failure, deadline expiry,
   late-bid rejection, and loss of a majority.
2. Continued operation after a follower failure.
3. Alice's bid of 50 succeeds before expiry and is returned by Result.
4. An expired auction with no bids has no winner and rejects new bids.
5. Zero/negative amounts and empty IDs are rejected; a valid bidder is registered
   even if the amount is rejected.
6. Twenty concurrent bids through different nodes preserve the maximum amount.
7. Recovery adopts a partially accepted update after the coordinator stops.
8. Prepare retries after learning a higher ballot.
9. Replicas reject an outdated ballot and preserve the newer state.

No race detector findings were reported in this run.

During investigation, the user's deadline file contained `2026-11-14T16:08:09Z`,
while current UTC time was `2026-11-14T16:05:46Z`. Thus accepting 50 was expected
if the running processes had loaded that deadline. Their startup arguments could
not be inspected because process listing is also restricted. This is evidence for
an unexpired deadline, not proof of the active processes' configuration.

## Result forwarding timeout fix

A later fault-injection run reproduced a timeout when the coordinator answered
Ping and then stopped responding to subsequent RPCs. Before the fix, the forwarded
Result consumed the full 2.5-second server budget and failed with DeadlineExceeded,
even with two usable replicas. See `timeout-before.log`.

Result now gives forwarding one second. For Unavailable/DeadlineExceeded, if the
outer request still has time, the receiving node coordinates both majority phases
locally. The same injected failure then returned the correct closed-auction result
in approximately 1.8 seconds. No client timeout increase was needed.

All 11 scenarios in `timeout-after.log` passed with the race detector. Two further
safety checks in `timeout-safety.log` passed: the fallback cannot return a result
without a majority, and an uncertain bid is not retried. These are in-memory gRPC
checks, not new real-process/TCP demonstrations. The specific cause of the user's
original one-off timeout cannot be established from the available logs.

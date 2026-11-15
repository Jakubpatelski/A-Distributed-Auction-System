# Passive replication verification

Checked on 2026-11-15 after replacing CASPaxos with passive replication and Bully leader selection. Older verification files describe the previous implementation.

- `go build ./...`: passed.
- `go vet ./...`: passed.
- `git diff --check`: passed.
- Seven temporary gRPC scenarios, each run five times with the Go race detector: passed.

Scenarios: leader failure and continued bidding; backup failure; deadline enforcement after failover; refusal without a majority; concurrent bids; recovery of a partially replicated update and rejection of old-term updates; leader crash between Ping and forwarded Result.

The temporary checks used actual gRPC handlers over in-memory connections. No test files were added to the project. TCP binding is blocked in this environment (`Operation not permitted`), so this verification does not claim a separate-process TCP run. Use the README commands for that check.

The report PDF was left unchanged. Its Markdown source now describes the new protocol.

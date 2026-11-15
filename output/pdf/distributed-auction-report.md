# A Distributed Auction System

## Introduction

The system implements one auction using three independent Go server processes and gRPC. Clients can submit a bid or query the result through any live node. It uses leader-based passive replication, with Bully elections to replace a failed leader. The intended guarantees are linearizability and continued service after one failure-stop crash.

A client supplies a bidder ID alongside the amount. Its first nonempty ID registers the bidder, including when the amount is rejected. A successful bid must be positive and strictly higher than the current highest bid. Repeated bids use the same ID. Before a shared deadline, Result returns the highest bid and bidder; afterward it returns the final winner, or no winner if no bid was accepted. Bid returns success, fail for rejection under the auction rules, or exception for an unconfirmed outcome.

## Architecture

Nodes have fixed IDs 0, 1 and 2. Each runs in a separate process and knows all three addresses. Node 2 is the initial leader. Backups forward client requests to the leader, which serializes operations using a local gate. Only the leader executes the auction rules. It sends whole snapshots to backups through InstallSnapshot; backups store state rather than execute the operation. A snapshot contains registered bidders, highest bid and bidder, the common absolute deadline, and a closed flag.

Snapshots also carry a leadership term and a version. A term is a pair (counter, leader ID), ordered lexicographically. The version increases for each operation within a term. Each node remembers its highest promised term and rejects snapshots from older terms. The leader stores a new snapshot locally and sends it to both backups concurrently. A normal response requires acknowledgement from at least two of the three replicas, including itself. Result also confirms a snapshot on a majority, preventing an isolated former leader from returning a stale local result. Both Bid and Result check the deadline while executing and can set the closed flag. There is no separate timer operation: the fixed deadline determines when bidding ends, even if no request arrives at that moment.

Backups periodically Ping the leader. After suspected failure, a node sends Election messages to higher IDs. A higher node replies and takes responsibility for the election, or identifies itself if it is already the active leader. If no higher node responds, the candidate attempts promotion. This is the Bully selection rule: the highest responsive ID wins. With node 2 stopped, node 1 normally becomes leader and announces itself through Coordinator messages.

Bully alone does not protect auction state. Before becoming active, the candidate creates a higher term and sends Freeze to the replicas. A granted Freeze prevents further writes from older terms and returns the stored snapshot. The candidate must obtain two grants, choose the snapshot with the greatest (term, version), and install that recovered state under its new term on two replicas. Only then may it serve requests. Higher promises supersede an older candidate or leader. Repeated Coordinator announcements help backups learn the active leader.

Communication uses gRPC over TCP, with 350 ms peer timeouts and a 300 ms monitoring interval. The server request budget is five seconds; the client defaults to six seconds. Queries may retry after a forwarding failure. Bids are not automatically repeated after an uncertain response. Goroutines handle RPCs inside each process; the replicas themselves are separate processes.

## Correctness 1

A history is linearizable if its completed operations, together with an appropriate completion of unresolved operations, can be arranged into a legal sequential history that preserves real-time precedence. If A returns before B is invoked, A must precede B. Each operation therefore appears to take effect at an instant between invocation and response. Sequential consistency is weaker: it requires preservation of each client's program order, without requiring real-time order between clients.

Within one leadership term, the gate serializes operations. Each new snapshot extends the previous state, and backups never replace a newer snapshot with an older version. Normal responses require a majority. Even a rejected bid is confirmed this way, since registration and closure may change state. Queries use the same confirmation barrier and cannot simply expose a stale backup copy.

Across leadership changes, every two-node recovery quorum intersects every two-node confirmation quorum. The intersecting replica holds the confirmed snapshot or a later descendant. Freeze prevents older writes from changing the recovery quorum after its snapshots are collected. Selecting the greatest stored (term, version) and replicating it before activation therefore preserves confirmed history. This argument applies inductively to successive terms. An older leader cannot confirm a conflicting update after a recovery quorum is frozen: every possible majority contains a replica that rejects its term.

Consequently, confirmed operations form a consistent state history, and a later invocation cannot omit an earlier confirmed operation. Overlapping operations can be ordered according to that history. The protocol provides linearizability under its failure and clock assumptions, rather than merely sequential consistency. A request overlapping the deadline can be ordered at its rule evaluation before expiry; an operation invoked after expiry cannot accept a new bid. The deployment uses a common clock and identical deadline. Separate hosts require synchronized clocks for the same timed interpretation.

An exception means an unknown outcome, not a guaranteed no-op. A tentative operation may be retained during recovery or discarded if it was never confirmed. The abstract auction operation remains unresolved and may be included in the sequential history if its state is retained. Retrying creates a new operation; exactly-once request deduplication is not provided.

## Correctness 2

Without failures, the leader applies requests sequentially and copies state to backups before confirming them. Accepted bids strictly increase the highest amount, while rejected bids do not replace the winner. The shared deadline fixes the end of bidding, and replicated closure never reopens. Queries report state from the same confirmed history as bids.

With one failed backup, the leader and remaining backup can still confirm operations. With a failed leader, the two survivors elect the higher ID and perform recovery before serving requests. Every previously confirmed snapshot had two copies, so at least one surviving replica holds it or a descendant. Recovery preserves this state. If the leader crashes during an operation, its response can be lost; recovery may retain that operation, but cannot lose previously confirmed operations. A client connected to the stopped node must select a surviving address.

Progress assumes nonfailed RPCs and processing fit the configured timing bounds. After a crash, elections and state transfer take time, so an overlapping request may return an exception and require a later attempt. False suspicions can cause extra elections; term fencing and majority confirmation protect safety. With fewer than two available replicas, the service refuses normal responses rather than risk conflicting results.

The failure model is failure-stop: a stopped process must not restart within the same auction because snapshots and promises are held only in memory. All nodes are trusted and use identical membership and deadline configuration. These assumptions exclude Byzantine behavior and recovery from total state loss. Compilation and static checks passed, as did seven temporary in-memory gRPC scenarios run five times with the race detector. TCP binding was blocked in the verification environment; these checks support the reasoning but do not prove linearizability or replace checking separate processes in the deployment environment.

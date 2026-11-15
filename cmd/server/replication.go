package main

import (
	"context"
	"log"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"mandatory5/auction"
	pb "mandatory5/proto"
)

func (s *server) Ping(context.Context, *pb.Empty) (*pb.LeaderInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoLocked(), nil
}

// A candidate freezes old-leader writes BEFORE selecting a recovery snapshot.
func (s *server) Freeze(_ context.Context, req *pb.FreezeRequest) (*pb.FreezeReply, error) {
	if !s.validTerm(req.Term, req.DeadlineUnixNano) {
		return nil, status.Error(codes.InvalidArgument, "invalid term or mismatched deadline")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	granted := compareTerm(req.Term, s.promised) >= 0
	if granted {
		s.promised = copyTerm(req.Term)
		if compareTerm(req.Term, s.leader) > 0 {
			s.active = false
			s.leaderKnown = false
			s.signalLocked()
		}
	}
	return &pb.FreezeReply{Granted: granted, Promised: copyTerm(s.promised), State: copyState(s.state)}, nil
}

// Backups only store state. They never execute Auction.Bid or Auction.Result.
func (s *server) InstallSnapshot(_ context.Context, req *pb.InstallRequest) (*pb.ReplicaReply, error) {
	state := req.GetState()
	if state == nil || !s.validTerm(state.Term, state.DeadlineUnixNano) {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot or mismatched deadline")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if compareTerm(state.Term, s.promised) < 0 {
		return s.replyLocked(false), nil
	}
	if compareTerm(state.Term, s.state.Term) == 0 && state.Version == s.state.Version && !proto.Equal(state, s.state) {
		return nil, status.Error(codes.InvalidArgument, "conflicting snapshots for the same term and version")
	}
	s.promised = copyTerm(state.Term)
	if compareTerm(state.Term, s.leader) > 0 {
		s.active = false
		s.leaderKnown = false
		s.signalLocked()
	}
	if newer(state, s.state) {
		s.state = copyState(state)
		log.Printf("Node %d stored leader %d term %d version %d: bidder=%q bid=%d closed=%t", s.id, state.Term.LeaderId, state.Term.Number, state.Version, state.HighestBidder, state.HighestBid, state.Closed)
	}
	return s.replyLocked(true), nil
}

// The caller has already stored this snapshot locally. One backup acknowledgement
// makes two copies; a single unavailable backup does not prevent confirmation.
func (s *server) replicate(ctx context.Context, state *pb.Snapshot) error {
	replies := make(chan *pb.ReplicaReply, 2)
	s.contactPeers(ctx, func(ctx context.Context, _ uint32, p peer) {
		reply, err := p.replication.InstallSnapshot(ctx, &pb.InstallRequest{State: state})
		if err == nil {
			replies <- reply
		}
	})
	close(replies)
	acks := 1
	for reply := range replies {
		s.observePromise(reply.Promised)
		if reply.Accepted && compareTerm(reply.Promised, state.Term) == 0 && reply.StoredVersion >= state.Version {
			acks++
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if compareTerm(s.promised, state.Term) != 0 {
		return status.Error(codes.Unavailable, "leadership changed during replication")
	}
	if acks < 2 {
		return status.Error(codes.Unavailable, "cannot confirm state: fewer than two replicas acknowledged")
	}
	return nil
}

// Only the active leader calls this. Even queries copy/confirm the current state
// with a majority, so an isolated former leader cannot answer stale reads.
func (s *server) perform(ctx context.Context, operation func(*auction.Auction)) error {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	s.mu.Lock()
	if ctx.Err() != nil {
		s.mu.Unlock()
		return status.FromContextError(ctx.Err()).Err()
	}
	if !s.infoLocked().Ready {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "node is not the active leader")
	}
	a := auction.FromSnapshot(auction.Snapshot{
		Bidders: s.state.Bidders, HighestBid: s.state.HighestBid, HighestBidder: s.state.HighestBidder,
		Deadline: time.Unix(0, s.state.DeadlineUnixNano), Closed: s.state.Closed,
	})
	operation(a)
	snapshot := a.Snapshot()
	sort.Strings(snapshot.Bidders)
	state := &pb.Snapshot{
		Term: copyTerm(s.leader), Version: s.state.Version + 1, Bidders: snapshot.Bidders,
		HighestBid: snapshot.HighestBid, HighestBidder: snapshot.HighestBidder,
		DeadlineUnixNano: snapshot.Deadline.UnixNano(), Closed: snapshot.Closed,
	}
	// Keep tentative state if replication fails: exception means outcome unknown.
	s.state = copyState(state)
	s.mu.Unlock()
	if err := s.replicate(ctx, state); err != nil {
		return err
	}
	log.Printf("Leader %d confirmed term %d version %d", s.id, state.Term.Number, state.Version)
	return nil
}

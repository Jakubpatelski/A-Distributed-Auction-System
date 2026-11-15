package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "mandatory5/proto"
)

// Bully: a higher-ID node answers OK and takes responsibility for the election.
func (s *server) Election(_ context.Context, req *pb.ElectionRequest) (*pb.ElectionReply, error) {
	if req.CandidateId >= s.id || req.DeadlineUnixNano != s.deadline.UnixNano() {
		return nil, status.Error(codes.InvalidArgument, "elections must target a higher ID with the same deadline")
	}
	s.mu.Lock()
	info := s.infoLocked()
	s.mu.Unlock()
	if !info.Ready {
		s.startElection()
	}
	return &pb.ElectionReply{HigherAlive: true, Leader: info}, nil
}

func (s *server) learnCoordinator(term *pb.Term) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if compareTerm(term, s.promised) < 0 {
		return false
	}
	s.promised = copyTerm(term)
	s.leader = copyTerm(term)
	s.leaderKnown = true
	if term.LeaderId != s.id {
		s.active = false
	}
	s.signalLocked()
	return true
}

func (s *server) Coordinator(_ context.Context, req *pb.CoordinatorRequest) (*pb.ReplicaReply, error) {
	if !s.validTerm(req.Term, req.DeadlineUnixNano) {
		return nil, status.Error(codes.InvalidArgument, "invalid coordinator")
	}
	accepted := s.learnCoordinator(req.Term)
	// A higher-priority node that was slow to start can bully a lower leader out.
	if accepted && s.id > req.Term.LeaderId {
		s.startElection()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replyLocked(accepted), nil
}

func (s *server) announce(ctx context.Context, term *pb.Term) {
	s.contactPeers(ctx, func(ctx context.Context, _ uint32, p peer) {
		reply, err := p.replication.Coordinator(ctx, &pb.CoordinatorRequest{Term: term, DeadlineUnixNano: s.deadline.UnixNano()})
		if err == nil {
			s.observePromise(reply.Promised)
		}
	})
}

func (s *server) startElection() {
	s.mu.Lock()
	if s.electing || s.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	s.electing = true
	s.active = false
	s.leaderKnown = false
	s.signalLocked()
	s.mu.Unlock()
	go s.electionLoop()
}

func (s *server) electionLoop() {
	defer func() { s.mu.Lock(); s.electing = false; s.signalLocked(); s.mu.Unlock() }()
	for s.ctx.Err() == nil {
		answers := make(chan *pb.ElectionReply, 2)
		s.contactPeers(s.ctx, func(ctx context.Context, id uint32, p peer) {
			if id <= s.id {
				return
			}
			answer, err := p.replication.Election(ctx, &pb.ElectionRequest{CandidateId: s.id, DeadlineUnixNano: s.deadline.UnixNano()})
			if err == nil && answer.HigherAlive {
				answers <- answer
			}
		})
		close(answers)
		higherAlive := false
		for answer := range answers {
			higherAlive = true
			if info := answer.GetLeader(); info != nil && info.Ready && s.learnCoordinator(info.Term) {
				return
			}
		}
		if higherAlive {
			if s.waitForCoordinator(s.ctx) {
				return
			}
			continue // A higher node may have failed after replying OK.
		}
		if err := s.promote(s.ctx); err == nil {
			return
		} else {
			log.Printf("Node %d cannot become leader yet: %v", s.id, err)
		}
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *server) waitForCoordinator(ctx context.Context) bool {
	timer := time.NewTimer(electionWait)
	defer timer.Stop()
	for {
		s.mu.Lock()
		known := s.leaderKnown && s.leader.LeaderId > s.id && compareTerm(s.promised, s.leader) == 0
		changed := s.changed
		s.mu.Unlock()
		if known {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-changed:
		}
	}
}

// Winning Bully is not enough: recover state and fence the previous leader first.
func (s *server) promote(ctx context.Context) error {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	term := &pb.Term{Number: s.promised.Number + 1, LeaderId: s.id}
	s.mu.Unlock()
	req := &pb.FreezeRequest{Term: term, DeadlineUnixNano: s.deadline.UnixNano()}
	own, err := s.Freeze(ctx, req)
	if err != nil {
		return err
	}
	responses := make(chan *pb.FreezeReply, 3)
	responses <- own
	s.contactPeers(ctx, func(ctx context.Context, _ uint32, p peer) {
		reply, err := p.replication.Freeze(ctx, req)
		if err == nil {
			responses <- reply
		}
	})
	close(responses)
	var best *pb.Snapshot
	promises := 0
	for reply := range responses {
		s.observePromise(reply.Promised)
		if reply.Granted && compareTerm(reply.Promised, term) == 0 {
			promises++
			if best == nil || newer(reply.State, best) {
				best = reply.State
			}
		}
	}
	if promises < 2 {
		return status.Error(codes.Unavailable, "takeover needs two frozen replicas")
	}
	s.mu.Lock()
	if compareTerm(s.promised, term) != 0 {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "a newer takeover superseded this one")
	}
	recovered := copyState(best)
	recovered.Term = copyTerm(term)
	recovered.Version = 0
	s.state = copyState(recovered)
	s.mu.Unlock()
	if err := s.replicate(ctx, recovered); err != nil {
		return err
	}
	s.mu.Lock()
	if compareTerm(s.promised, term) != 0 {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "leadership changed during state transfer")
	}
	s.leader = copyTerm(term)
	s.leaderKnown = true
	s.active = true
	s.signalLocked()
	s.mu.Unlock()
	log.Printf("Node %d is leader for term %d; recovered bidder=%q bid=%d closed=%t", s.id, term.Number, recovered.HighestBidder, recovered.HighestBid, recovered.Closed)
	s.announce(ctx, term)
	return nil
}

// Backups periodically check the leader. The leader repeats its announcement so
// a backup that missed an earlier Coordinator message can learn the new term.
func (s *server) run() {
	ticker := time.NewTicker(monitorPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		info := s.infoLocked()
		electing := s.electing
		target := s.leader.LeaderId
		if !s.leaderKnown {
			target = s.promised.LeaderId
		}
		s.mu.Unlock()
		if info.Ready {
			s.announce(s.ctx, info.Term)
			continue
		}
		if electing {
			continue
		}
		if target != s.id {
			ctx, cancel := context.WithTimeout(s.ctx, peerTimeout)
			reply, err := s.peers[target].replication.Ping(ctx, &pb.Empty{})
			cancel()
			if err == nil && reply.Ready && s.learnCoordinator(reply.Term) {
				if s.id > reply.Term.LeaderId {
					s.startElection()
				}
				continue
			}
		}
		s.startElection()
	}
}

func (s *server) findLeader(ctx context.Context) (uint32, error) {
	for {
		s.mu.Lock()
		info := s.infoLocked()
		known := s.leaderKnown
		changed := s.changed
		s.mu.Unlock()
		if info.Ready {
			return s.id, nil
		}
		if known && info.Term.LeaderId != s.id {
			peerCtx, cancel := context.WithTimeout(ctx, peerTimeout)
			reply, err := s.peers[info.Term.LeaderId].replication.Ping(peerCtx, &pb.Empty{})
			cancel()
			if err == nil && reply.Ready && s.learnCoordinator(reply.Term) {
				return reply.Term.LeaderId, nil
			}
		}
		s.startElection()
		select {
		case <-ctx.Done():
			return 0, status.Error(codes.Unavailable, "no active leader with a majority before the request deadline")
		case <-changed:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

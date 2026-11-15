package main

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	pb "mandatory5/proto"
)

const (
	peerTimeout    = 350 * time.Millisecond
	monitorPeriod  = 300 * time.Millisecond
	electionWait   = time.Second
	requestTimeout = 5 * time.Second
)

type peer struct {
	api         pb.AuctionServiceClient
	replication pb.ReplicationServiceClient
}

type server struct {
	pb.UnimplementedAuctionServiceServer
	pb.UnimplementedReplicationServiceServer
	id       uint32
	deadline time.Time
	peers    []peer
	gate     chan struct{} // Serializes local auction operations and promotion.
	ctx      context.Context
	stop     context.CancelFunc

	mu          sync.Mutex // Never held across a network call.
	promised    *pb.Term   // Older leaders cannot install snapshots after this promise.
	leader      *pb.Term
	leaderKnown bool
	active      bool
	state       *pb.Snapshot
	electing    bool
	changed     chan struct{}
}

func newServer(id uint32, deadline time.Time) *server {
	ctx, cancel := context.WithCancel(context.Background())
	// Bully gives the highest ID priority, so node 2 is the initial leader.
	term := &pb.Term{Number: 0, LeaderId: 2}
	return &server{
		id: id, deadline: deadline, peers: make([]peer, 3), gate: make(chan struct{}, 1),
		ctx: ctx, stop: cancel, promised: copyTerm(term), leader: copyTerm(term),
		leaderKnown: true, active: id == 2, changed: make(chan struct{}),
		state: &pb.Snapshot{Term: copyTerm(term), DeadlineUnixNano: deadline.UnixNano()},
	}
}

func copyTerm(t *pb.Term) *pb.Term          { return proto.Clone(t).(*pb.Term) }
func copyState(s *pb.Snapshot) *pb.Snapshot { return proto.Clone(s).(*pb.Snapshot) }

func compareTerm(a, b *pb.Term) int {
	if a.GetNumber() < b.GetNumber() || (a.GetNumber() == b.GetNumber() && a.GetLeaderId() < b.GetLeaderId()) {
		return -1
	}
	if a.GetNumber() == b.GetNumber() && a.GetLeaderId() == b.GetLeaderId() {
		return 0
	}
	return 1
}

func newer(a, b *pb.Snapshot) bool {
	cmp := compareTerm(a.Term, b.Term)
	return cmp > 0 || (cmp == 0 && a.Version > b.Version)
}

func (s *server) validTerm(term *pb.Term, deadline int64) bool {
	return term != nil && term.LeaderId < 3 && deadline == s.deadline.UnixNano()
}

func (s *server) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *server) infoLocked() *pb.LeaderInfo {
	return &pb.LeaderInfo{Term: copyTerm(s.leader), Ready: s.active && s.leaderKnown && s.leader.LeaderId == s.id && compareTerm(s.promised, s.leader) == 0}
}

func (s *server) replyLocked(accepted bool) *pb.ReplicaReply {
	return &pb.ReplicaReply{Accepted: accepted, Promised: copyTerm(s.promised), StoredVersion: s.state.Version}
}

func (s *server) observePromise(term *pb.Term) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if compareTerm(term, s.promised) > 0 {
		s.promised = copyTerm(term)
		s.active = false
		s.leaderKnown = false
		s.signalLocked()
	}
}

// Every broadcast contacts both peers concurrently. This does not create replicas
// as goroutines: each replica is still a separately started server process.
func (s *server) contactPeers(ctx context.Context, call func(context.Context, uint32, peer)) {
	var wg sync.WaitGroup
	for i, p := range s.peers {
		if uint32(i) == s.id {
			continue
		}
		wg.Add(1)
		go func(id uint32, p peer) {
			defer wg.Done()
			peerCtx, cancel := context.WithTimeout(ctx, peerTimeout)
			defer cancel()
			call(peerCtx, id, p)
		}(uint32(i), p)
	}
	wg.Wait()
}

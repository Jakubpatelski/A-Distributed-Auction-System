package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"mandatory5/auction"
	auctionpb "mandatory5/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const peerTimeout = 400 * time.Millisecond

type peer struct {
	api         auctionpb.AuctionServiceClient
	replication auctionpb.ReplicationServiceClient
}

type server struct {
	auctionpb.UnimplementedAuctionServiceServer
	auctionpb.UnimplementedReplicationServiceServer
	id       uint32
	deadline time.Time
	peers    []peer

	// gate serializes proposals at this process, without blocking internal RPCs.
	gate    chan struct{}
	counter uint64

	// Each process is also an acceptor. Never hold mu during a network call.
	mu       sync.Mutex
	promised *auctionpb.Ballot
	accepted *auctionpb.Ballot
	state    *auctionpb.AuctionSnapshot
}

func compare(a, b *auctionpb.Ballot) int {
	if a.GetRound() < b.GetRound() || (a.GetRound() == b.GetRound() && a.GetNodeId() < b.GetNodeId()) {
		return -1
	}
	if a.GetRound() == b.GetRound() && a.GetNodeId() == b.GetNodeId() {
		return 0
	}
	return 1
}

func validBallot(b *auctionpb.Ballot) bool {
	return b != nil && b.GetRound() > 0 && b.GetNodeId() < 3
}

func (s *server) Ping(context.Context, *auctionpb.PingRequest) (*auctionpb.PingResponse, error) {
	return &auctionpb.PingResponse{}, nil
}

func (s *server) Prepare(_ context.Context, req *auctionpb.PrepareRequest) (*auctionpb.PrepareResponse, error) {
	if !validBallot(req.GetBallot()) {
		return nil, status.Error(codes.InvalidArgument, "invalid ballot")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	granted := compare(req.Ballot, s.promised) >= 0
	if granted {
		s.promised = proto.Clone(req.Ballot).(*auctionpb.Ballot)
	}
	// Clone so the local proposer cannot change the acceptor's state by accident.
	return proto.Clone(&auctionpb.PrepareResponse{
		Granted: granted, Promised: s.promised, Accepted: s.accepted, State: s.state,
	}).(*auctionpb.PrepareResponse), nil
}

func (s *server) Accept(_ context.Context, req *auctionpb.AcceptRequest) (*auctionpb.AcceptResponse, error) {
	if !validBallot(req.GetBallot()) || req.GetState() == nil || req.State.DeadlineUnixNano != s.deadline.UnixNano() {
		return nil, status.Error(codes.InvalidArgument, "invalid ballot, missing state, or mismatched deadline")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	granted := compare(req.Ballot, s.promised) >= 0
	if granted && compare(req.Ballot, s.accepted) == 0 && !proto.Equal(req.State, s.state) {
		return nil, status.Error(codes.InvalidArgument, "a ballot cannot accept two different states")
	}
	if granted {
		s.promised = proto.Clone(req.Ballot).(*auctionpb.Ballot)
		s.accepted = proto.Clone(req.Ballot).(*auctionpb.Ballot)
		s.state = proto.Clone(req.State).(*auctionpb.AuctionSnapshot)
		log.Printf("Accepted ballot (%d,%d): bidder=%q bid=%d closed=%t", req.Ballot.Round, req.Ballot.NodeId, req.State.HighestBidder, req.State.HighestBid, req.State.Closed)
	}
	return proto.Clone(&auctionpb.AcceptResponse{Granted: granted, Promised: s.promised}).(*auctionpb.AcceptResponse), nil
}

// change uses two majority rounds. A read also completes both rounds: reading
// one replica could return stale or only partly accepted state.
// This is the unoptimized CASPaxos state-transition protocol.
func (s *server) change(ctx context.Context, update func(*auction.Auction)) error {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ballot, recovered, err := s.prepareMajority(ctx)
	if err != nil {
		return err
	}
	a := auction.New(s.deadline)
	if recovered != nil {
		if recovered.DeadlineUnixNano != s.deadline.UnixNano() {
			return fmt.Errorf("nodes have different auction deadlines")
		}
		a = auction.FromSnapshot(auction.Snapshot{
			Bidders: recovered.Bidders, HighestBid: recovered.HighestBid,
			HighestBidder: recovered.HighestBidder,
			Deadline:      time.Unix(0, recovered.DeadlineUnixNano), Closed: recovered.Closed,
		})
	}
	update(a)
	snapshot := a.Snapshot()
	proposal := &auctionpb.AcceptRequest{
		Ballot: ballot,
		State: &auctionpb.AuctionSnapshot{
			Bidders: snapshot.Bidders, HighestBid: snapshot.HighestBid,
			HighestBidder: snapshot.HighestBidder, DeadlineUnixNano: snapshot.Deadline.UnixNano(),
			Closed: snapshot.Closed,
		},
	}
	acks := 0
	for i, p := range s.peers {
		var reply *auctionpb.AcceptResponse
		var err error
		if uint32(i) == s.id {
			reply, err = s.Accept(ctx, proposal)
		} else {
			peerCtx, cancel := context.WithTimeout(ctx, peerTimeout)
			reply, err = p.replication.Accept(peerCtx, proposal)
			cancel()
		}
		if err == nil {
			s.counter = max(s.counter, reply.GetPromised().GetRound())
			if reply.GetGranted() {
				acks++
			}
		}
	}
	if acks < 2 {
		// Some nodes may have stored the bid. Never report an ordinary rejection.
		return fmt.Errorf("accept received %d/2 required acknowledgements; outcome unknown", acks)
	}
	log.Printf("Node %d coordinated ballot (%d,%d) with %d acknowledgements", s.id, ballot.Round, ballot.NodeId, acks)
	return nil
}

// prepareMajority may retry a rejected ballot because no auction change has
// been submitted yet. The caller holds gate throughout this operation.
func (s *server) prepareMajority(ctx context.Context) (*auctionpb.Ballot, *auctionpb.AuctionSnapshot, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		s.mu.Lock()
		s.counter = max(s.counter, s.promised.GetRound()) + 1
		s.mu.Unlock()
		ballot := &auctionpb.Ballot{Round: s.counter, NodeId: s.id}
		var newest *auctionpb.Ballot
		var recovered *auctionpb.AuctionSnapshot
		promises := 0
		superseded := false
		for i, p := range s.peers {
			var reply *auctionpb.PrepareResponse
			var err error
			req := &auctionpb.PrepareRequest{Ballot: ballot}
			if uint32(i) == s.id {
				reply, err = s.Prepare(ctx, req)
			} else {
				peerCtx, cancel := context.WithTimeout(ctx, peerTimeout)
				reply, err = p.replication.Prepare(peerCtx, req)
				cancel()
			}
			if err != nil {
				continue
			}
			s.counter = max(s.counter, reply.GetPromised().GetRound())
			if compare(reply.GetPromised(), ballot) > 0 {
				superseded = true
			}
			if reply.GetGranted() {
				promises++
				if compare(reply.Accepted, newest) > 0 {
					newest, recovered = reply.Accepted, reply.State
				}
			}
		}
		if promises >= 2 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			return ballot, recovered, nil
		}
		if !superseded {
			return nil, nil, fmt.Errorf("prepare received %d/2 required promises", promises)
		}
		log.Printf("Ballot (%d,%d) superseded; preparing a higher ballot", ballot.Round, ballot.NodeId)
		// Yield briefly if coordinators are competing; respect the request deadline.
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

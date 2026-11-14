package main

import (
	"context"
	"log"
	"time"

	"mandatory5/auction"
	auctionpb "mandatory5/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Prefer the lowest reachable node ID. This routing rule reduces competing
// proposals; majority promises, rather than Ping, provide the safety guarantee.
// Failover is detected on the next request, without a background heartbeat.
func (s *server) coordinator(ctx context.Context) int {
	for i, p := range s.peers {
		if uint32(i) == s.id {
			return i
		}
		peerCtx, cancel := context.WithTimeout(ctx, peerTimeout)
		_, err := p.replication.Ping(peerCtx, &auctionpb.PingRequest{})
		cancel()
		if err == nil {
			return i
		}
	}
	return int(s.id)
}

func forwarded(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	return len(md.Get("auction-forwarded")) > 0
}

func (s *server) Bid(ctx context.Context, req *auctionpb.BidRequest) (*auctionpb.BidResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	if !forwarded(ctx) {
		if id := s.coordinator(ctx); uint32(id) != s.id {
			ctx = metadata.AppendToOutgoingContext(ctx, "auction-forwarded", "true")
			// Do not automatically resubmit an uncertain bid if this RPC fails.
			return s.peers[id].api.Bid(ctx, req)
		}
	}
	ack := auction.Exception
	err := s.change(ctx, func(a *auction.Auction) {
		ack = a.Bid(req.GetBidderId(), req.GetAmount(), time.Now())
	})
	if err != nil {
		log.Printf("Bid could not be confirmed: %v", err)
		return &auctionpb.BidResponse{Ack: auctionpb.Ack_ACK_EXCEPTION}, nil
	}
	if ack == auction.Success {
		return &auctionpb.BidResponse{Ack: auctionpb.Ack_ACK_SUCCESS}, nil
	}
	return &auctionpb.BidResponse{Ack: auctionpb.Ack_ACK_FAIL}, nil
}

func (s *server) Result(ctx context.Context, req *auctionpb.ResultRequest) (*auctionpb.ResultResponse, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	if !forwarded(ctx) {
		if id := s.coordinator(ctx); uint32(id) != s.id {
			// Ping can succeed immediately before the coordinator disappears.
			// Reserve time for this node to recover the result from a majority.
			forwardCtx, stopForward := context.WithTimeout(ctx, time.Second)
			forwardCtx = metadata.AppendToOutgoingContext(forwardCtx, "auction-forwarded", "true")
			response, err := s.peers[id].api.Result(forwardCtx, req)
			stopForward()
			if err == nil {
				return response, nil
			}
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
				return nil, err
			}
			log.Printf("Node %d: Result forwarding to node %d failed after %s: %v; recovering through a majority", s.id, id, time.Since(started), err)
			// A query may safely be repeated. Still use both consensus phases;
			// never return this node's potentially stale snapshot directly.
		}
	}
	var result auction.Outcome
	err := s.change(ctx, func(a *auction.Auction) { result = a.Result(time.Now()) })
	if err != nil {
		log.Printf("Node %d: Result could not be confirmed after %s: %v", s.id, time.Since(started), err)
		return nil, status.Errorf(codes.Unavailable, "cannot confirm result: %v", err)
	}
	return &auctionpb.ResultResponse{
		Over: result.Over, HighestBid: result.HighestBid, HighestBidder: result.HighestBidder,
	}, nil
}

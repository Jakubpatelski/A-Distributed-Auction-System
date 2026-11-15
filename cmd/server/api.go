package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"mandatory5/auction"
	pb "mandatory5/proto"
)

func forwarded(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	return len(md.Get("auction-forwarded")) > 0
}

func (s *server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	leader, err := s.findLeader(ctx)
	if err != nil {
		return &pb.BidResponse{Ack: pb.Ack_ACK_EXCEPTION}, nil
	}
	if leader != s.id {
		if forwarded(ctx) {
			return nil, status.Error(codes.Unavailable, "leader changed while forwarding")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, "auction-forwarded", "true")
		// Never automatically repeat a bid after losing its response.
		return s.peers[leader].api.Bid(ctx, req)
	}
	ack := auction.Exception
	err = s.perform(ctx, func(a *auction.Auction) { ack = a.Bid(req.GetBidderId(), req.GetAmount(), time.Now()) })
	if err != nil {
		log.Printf("Leader %d could not confirm bid: %v", s.id, err)
		return &pb.BidResponse{Ack: pb.Ack_ACK_EXCEPTION}, nil
	}
	if ack == auction.Success {
		return &pb.BidResponse{Ack: pb.Ack_ACK_SUCCESS}, nil
	}
	return &pb.BidResponse{Ack: pb.Ack_ACK_FAIL}, nil
}

func (s *server) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		leader, err := s.findLeader(ctx)
		if err != nil {
			return nil, err
		}
		if leader == s.id {
			var outcome auction.Outcome
			err = s.perform(ctx, func(a *auction.Auction) { outcome = a.Result(time.Now()) })
			if err != nil {
				return nil, err
			}
			return &pb.ResultResponse{Over: outcome.Over, HighestBid: outcome.HighestBid, HighestBidder: outcome.HighestBidder}, nil
		}
		if forwarded(ctx) {
			return nil, status.Error(codes.Unavailable, "leader changed while forwarding")
		}
		forwardCtx, stop := context.WithTimeout(ctx, time.Second)
		forwardCtx = metadata.AppendToOutgoingContext(forwardCtx, "auction-forwarded", "true")
		result, err := s.peers[leader].api.Result(forwardCtx, req)
		stop()
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return nil, err
		}
		log.Printf("Node %d: query through leader %d failed; checking leadership again: %v", s.id, leader, err)
		// Queries can be repeated, but only a recovered leader may answer them.
		s.startElection()
	}
	return nil, status.Error(codes.Unavailable, "could not confirm result after leader replacement")
}

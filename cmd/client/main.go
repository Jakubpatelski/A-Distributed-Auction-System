package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	auctionpb "mandatory5/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "localhost:50051", "server address")
	timeout := flag.Duration("timeout", 6*time.Second, "request timeout (includes leader election)")
	flag.Parse()
	args := flag.Args()
	usage := "usage: client [-addr localhost:50051] [-timeout 6s] bid <bidder-id> <amount> | result"
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	var bid *auctionpb.BidRequest
	switch args[0] {
	case "bid":
		if len(args) != 3 || args[1] == "" {
			return fmt.Errorf("%s", usage)
		}
		amount, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("amount must be an integer: %w", err)
		}
		bid = &auctionpb.BidRequest{BidderId: args[1], Amount: amount}
	case "result":
		if len(args) != 1 {
			return fmt.Errorf("%s", usage)
		}
	default:
		return fmt.Errorf("%s", usage)
	}

	// Plaintext transport is sufficient for our local assignment processes.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("exception: %w", err)
	}
	defer conn.Close()
	client := auctionpb.NewAuctionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if bid != nil {
		response, err := client.Bid(ctx, bid)
		if err != nil {
			return fmt.Errorf("exception: bid outcome is unknown: %w", err)
		}
		switch response.GetAck() {
		case auctionpb.Ack_ACK_SUCCESS:
			fmt.Println("success")
		case auctionpb.Ack_ACK_FAIL:
			fmt.Println("fail")
		default:
			return fmt.Errorf("exception: server could not confirm the bid")
		}
		return nil
	}

	response, err := client.Result(ctx, &auctionpb.ResultRequest{})
	if err != nil {
		return fmt.Errorf("exception: %w", err)
	}
	if response.GetOver() {
		if response.GetHighestBidder() == "" {
			fmt.Println("Auction ended without a winner.")
		} else {
			fmt.Printf("Auction ended. Winner: %s, bid: %d\n", response.GetHighestBidder(), response.GetHighestBid())
		}
	} else if response.GetHighestBidder() == "" {
		fmt.Println("Auction open. No accepted bids yet.")
	} else {
		fmt.Printf("Auction open. Highest bidder: %s, bid: %d\n", response.GetHighestBidder(), response.GetHighestBid())
	}
	return nil
}

package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	auctionpb "mandatory5/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.Int("id", 0, "node index in the members list: 0, 1, or 2")
	membersFlag := flag.String("members", "localhost:50051,localhost:50052,localhost:50053", "same ordered list of three addresses on every node")
	deadlineFlag := flag.String("deadline", "", "shared auction deadline in RFC3339 format (required)")
	flag.Parse()
	members := strings.Split(*membersFlag, ",")
	if len(members) != 3 || *id < 0 || *id >= 3 {
		log.Fatal("use exactly three members and an id from 0 to 2")
	}
	seen := make(map[string]bool)
	for i, addr := range members {
		members[i] = strings.TrimSpace(addr)
		if members[i] == "" || seen[members[i]] {
			log.Fatal("member addresses must be nonempty and unique")
		}
		seen[members[i]] = true
	}
	deadline, err := time.Parse(time.RFC3339, *deadlineFlag)
	if err != nil {
		log.Fatal("provide the same -deadline on every node, e.g. 2026-11-14T16:00:00Z")
	}
	s := &server{
		id: uint32(*id), deadline: deadline, gate: make(chan struct{}, 1),
		peers: make([]peer, len(members)),
	}
	for i, addr := range members {
		if i == *id {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		s.peers[i] = peer{
			api:         auctionpb.NewAuctionServiceClient(conn),
			replication: auctionpb.NewReplicationServiceClient(conn),
		}
	}
	listener, err := net.Listen("tcp", members[*id])
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	grpcServer := grpc.NewServer()
	auctionpb.RegisterAuctionServiceServer(grpcServer, s)
	auctionpb.RegisterReplicationServiceServer(grpcServer, s)
	log.Printf("Node %d listening on %s; shared deadline %s", *id, members[*id], deadline.Format(time.RFC3339))
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

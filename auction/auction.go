// Package auction implements the rules for one auction.
// Calls must be sequential; the networking layer will serialize requests later.
package auction

import "time"

// Snapshot is a copy of the state that another node needs to hold the auction.
type Snapshot struct {
	Bidders       []string
	HighestBid    int64
	HighestBidder string
	Deadline      time.Time
	Closed        bool
}

func (a *Auction) Snapshot() Snapshot {
	bidders := make([]string, 0, len(a.bidders))
	for id := range a.bidders {
		bidders = append(bidders, id)
	}
	return Snapshot{
		Bidders: bidders, HighestBid: a.highestBid, HighestBidder: a.highestBidder,
		Deadline: a.deadline, Closed: a.closed,
	}
}

func FromSnapshot(snapshot Snapshot) *Auction {
	a := New(snapshot.Deadline)
	for _, id := range snapshot.Bidders {
		a.bidders[id] = true
	}
	a.highestBid = snapshot.HighestBid
	a.highestBidder = snapshot.HighestBidder
	a.closed = snapshot.Closed
	return a
}

type Ack string

const (
	Success   Ack = "success"
	Fail      Ack = "fail"
	Exception Ack = "exception" // The distributed service cannot confirm the outcome.
)

// Outcome describes the current state, or the final result when Over is true.
// An empty HighestBidder means no bid has been accepted.
type Outcome struct {
	Over          bool
	HighestBid    int64
	HighestBidder string
}

type Auction struct {
	bidders       map[string]bool
	highestBid    int64
	highestBidder string
	deadline      time.Time
	closed        bool
}

// New creates an auction with a fixed deadline.
func New(deadline time.Time) *Auction {
	return &Auction{
		bidders:  make(map[string]bool),
		deadline: deadline,
	}
}

// Bid registers a nonempty bidder ID even when the bid is rejected.
// Bids must be positive, exceed the highest bid, and arrive before the deadline.
func (a *Auction) Bid(bidderID string, amount int64, now time.Time) Ack {
	a.closeIfExpired(now)
	if bidderID == "" {
		return Fail
	}
	a.bidders[bidderID] = true
	if a.closed || amount <= a.highestBid {
		return Fail
	}
	a.highestBid = amount
	a.highestBidder = bidderID
	return Success
}

// Result reports whether the deadline has been reached and the highest bid.
func (a *Auction) Result(now time.Time) Outcome {
	a.closeIfExpired(now)
	return Outcome{
		Over:          a.closed,
		HighestBid:    a.highestBid,
		HighestBidder: a.highestBidder,
	}
}

func (a *Auction) closeIfExpired(now time.Time) {
	if !now.Before(a.deadline) {
		a.closed = true
	}
}

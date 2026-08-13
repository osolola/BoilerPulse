package raft

import (
	"context"
	"math/rand"
	"time"
)

func randomDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func (n *Node) resetElectionTimerLocked() {
	n.electionResetAt = time.Now()
	n.electionTimeout = randomDuration(n.opts.MinElectionTimeout, n.opts.MaxElectionTimeout)
}

// startElectionLocked transitions to Candidate, votes for itself, and
// fans out RequestVote RPCs. It returns immediately; vote replies are
// processed asynchronously as they arrive.
func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.persistTermAndVoteLocked()
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()

	n.logger.Info("starting election", "term", term)

	votes := 1 // vote for self
	majority := n.majority()

	for _, peer := range n.peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout)
			defer cancel()

			reply, err := n.transport.SendRequestVote(ctx, peer, &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != term {
				return // stale reply: we've moved on since sending this request
			}
			if reply.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeaderLocked()
				}
			}
		}()
	}
}

// becomeLeaderLocked transitions to Leader and initializes per-peer
// replication state. It does not append a no-op entry to establish
// commitment in the new term immediately — the first Propose or heartbeat
// tick handles that naturally, which is simpler and sufficient here.
func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.currentTerm)

	lastIdx := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	n.lastHeartbeatSent = time.Time{} // force an immediate heartbeat on the next tick
}

// HandleRequestVote processes an incoming RequestVote RPC. Safe for
// concurrent use.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		reply.VoteGranted = false
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply.Term = n.currentTerm

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	logOK := args.LastLogTerm > n.lastLogTermLocked() ||
		(args.LastLogTerm == n.lastLogTermLocked() && args.LastLogIndex >= n.lastLogIndexLocked())

	if canVote && logOK {
		n.votedFor = args.CandidateID
		n.persistTermAndVoteLocked()
		n.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
	return reply
}

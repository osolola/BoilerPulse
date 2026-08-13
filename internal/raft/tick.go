package raft

import "time"

func (n *Node) run() {
	defer close(n.doneCh)
	ticker := time.NewTicker(n.opts.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	state := n.state
	electionDue := state != Leader && time.Since(n.electionResetAt) >= n.electionTimeout
	heartbeatDue := state == Leader && time.Since(n.lastHeartbeatSent) >= n.opts.HeartbeatInterval
	if electionDue {
		n.startElectionLocked()
	}
	if heartbeatDue {
		n.lastHeartbeatSent = time.Now()
	}
	peers := n.peers
	n.mu.Unlock()

	if heartbeatDue {
		for _, peer := range peers {
			n.triggerReplication(peer)
		}
	}
}

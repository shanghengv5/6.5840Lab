package raft

//	All Servers:
//
// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) commitIndexAboveLastApplied() {
	for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
		rf.applyStateMachine()
	}
}

func (rf *Raft) applyStateMachine() {
	msg := ApplyMsg{
		Command:      rf.Logs[rf.lastApplied+1].Command,
		CommandValid: true,
		CommandIndex: rf.lastApplied + 1,
	}
	DPrintf(dInfo, "S%d ApplyMsg %v", rf.me, msg)
	rf.applyCh <- msg

}

// • If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)

func (rf *Raft) aboveCurrentTerm(term int) (shouldReturn bool) {
	// Must use lock
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.initFollower()
		shouldReturn = true
	}
	return
}

// Followers
// • Respond to RPCs from candidates and leaders
// • If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate:
// convert to candidate

// granting vote to candidate: convert to candidate
func (rf *Raft) grantingVote(voteFor int) {
	rf.votedFor = voteFor
	rf.Convert(Candidate)
}

// Candidates:
// On conversion to candidate, start election:
// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
// • If votes received from majority of servers: become leader
// • If AppendEntries RPC received from new leader: convert to
// follower
// • If election timeout elapses: start new election

func (rf *Raft) startElection(fromState State) {
	// Repeat vote
	if rf.state != fromState {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.grantingVote(rf.me)
	rf.currentTerm++
	rf.voteCount = 1

	rf.broadcastRequestVote()
	DPrintf(dVote, "S%d fromState%s ", rf.me, fromState)
}

func (rf *Raft) voteMajorities() bool {
	return rf.voteCount >= rf.majority/2+1
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	DPrintf(dLeader, "S%d become a leader", rf.me)
	rf.Convert(Leader)
	// (Reinitialized after election)
	for server := range rf.peers {
		//for each server, index of the next log entry
		// to send to that server (initialized to leader
		// 	last log index + 1)
		rf.nextIndex[server] = rf.commitIndex + 1
		//for each server, index of highest log entry
		// known to be replicated on server
		// (initialized to 0, increases monotonically)
		rf.matchIndex[server] = 0
	}
	rf.broadcastAppendEntries()
}

// Leaders:
// • Upon election: send initial empty AppendEntries RPCs
// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)
// • If command received from client: append entry to local log,
// respond after entry applied to state machine (§5.3)
// • If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)
// • If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).

func (rf *Raft) getLastLogIndex() int {
	return len(rf.Logs) - 1
}

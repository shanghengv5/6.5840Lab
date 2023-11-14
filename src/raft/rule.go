package raft

//	All Servers:

// If RPC request or response contains term T > currentTerm:
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




// granting vote to candidate: convert to candidate
func (rf *Raft) grantingVote(voteFor int) {
	rf.votedFor = voteFor
	rf.Convert(Candidate)
}

// Upon election: send initial empty AppendEntries RPCs
func (rf *Raft) toLeader() {
	if rf.state != Candidate {
		return
	}
	DPrintf(dLeader, "S%d become a leader", rf.me)
	rf.Convert(Leader)
	rf.infoHeartbeat()
}

// On conversion to candidate, start election:
// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
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

	rf.InfoRequestVote()
	DPrintf(dVote, "S%d fromState%s ", rf.me, fromState)
}

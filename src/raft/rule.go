package raft

//	All Servers:
//
// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) commitIndexAboveLastApplied() {
	DPrintf(dApply, "S%d lastApplied%d commitIndex%d lastIncludedIndex%d", rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex)
	for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
		applyIndex := rf.lastApplied + 1
		if rf.getLogIndex(applyIndex) > 0 {
			rf.applyStateMachine(ApplyMsg{
				Command:      rf.getLogEntry(applyIndex).Command,
				CommandValid: true,
				CommandIndex: applyIndex,
			})
		}

	}
}

func (rf *Raft) refreshLastApplied(index int) bool {
	if rf.lastApplied > index {
		return false
	}
	rf.lastApplied = index
	if rf.commitIndex < rf.lastApplied {
		rf.commitIndex = index
	}
	return true
}

func (rf *Raft) SetLastIncludedIndex(index, term int, snapshot []byte) {
	//If the snapshot is oldest,return
	if rf.lastIncludedIndex >= index {
		return
	}
	DPrintf(dSnap, "S%d Index%d lastApplied%d commitIndex%d", rf.me, index, rf.lastApplied, rf.commitIndex)
	// SetNewSnapshot
	newHead := []LogEntry{{Term: 0}}
	rest := index + 1
	if rest <= rf.getLastLogIndex() {
		newHead = append(newHead, rf.getFractionLog(rest, -1)...)
	}
	rf.lastIncludedTerm = term
	rf.lastIncludedIndex = index
	rf.Logs = newHead
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	// If snapshot will update apply msg
	if rf.refreshLastApplied(rf.lastIncludedIndex) {
		rf.applyStateMachine(ApplyMsg{
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
		})
	}

}

func (rf *Raft) SetCommitIndex(index int) {
	rf.commitIndex = index
}

func (rf *Raft) applyStateMachine(msg ApplyMsg) {
	// DPrintf(dApply, "S%d lastApplied%d commitIndex%d Role:%s Msg%v", rf.me, rf.lastApplied, rf.commitIndex, rf.state, msg)
	rf.applyCh <- msg
}

// • If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) aboveCurrentTerm(term int) (shouldReturn bool) {
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
	defer rf.persist()

	rf.grantingVote(rf.me)
	rf.currentTerm++
	rf.voteCount = 1

	rf.broadcastRequestVote()
	// DPrintf(dVote, "S%d fromState %s start election", rf.me, fromState)
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
	rf.initLeaderVolatile()
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
	return len(rf.Logs) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	if rf.getLastLogIndex() == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.getLogEntry(rf.getLastLogIndex()).Term
}

func (rf *Raft) getLogLength() int {
	return rf.getLastLogIndex() + 1
}

func (rf *Raft) getFractionLog(front, back int) []LogEntry {
	if front == -1 {
		front = 0
	} else {
		front = rf.getLogIndex(front)
	}
	if back == -1 {
		back = len(rf.Logs)
	} else {
		back = rf.getLogIndex(back)
	}

	return rf.Logs[front:back]
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	return rf.Logs[rf.getLogIndex(index)]
}

func (rf *Raft) getLogIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) existsNSetCommitIndex() {
	for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
		voteCount := 0
		for _, mI := range rf.matchIndex {
			if mI <= rf.getLastLogIndex() &&
				mI >= N &&
				rf.getLogEntry(mI).Term == rf.currentTerm {
				// To eliminate problems like the one in Figure 8, Raft
				// never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
				// term are committed by counting replicas; once an entry
				// from the current term has been committed in this way,
				// then all prior entries are committed indirectly because
				// of the Log Matching Property. There are some situations
				// where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach
				// for simplicity
				voteCount++
			} else if mI >= N && mI == rf.lastIncludedIndex && rf.currentTerm == rf.lastIncludedTerm {
				voteCount++
			}
		}
		if voteCount >= rf.majority/2+1 {
			rf.SetCommitIndex(N)
		}
	}
}

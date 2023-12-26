package raft

type AppendEntriesArg struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// A log entry implement
type LogEntry struct {
	Term    int
	Command interface{}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || rf.aboveCurrentTerm(args.Term) {
		return
	}
	rf.followerRespond()

	DPrintf(dClient, "S%d => S%d %s AppendEntries lastApplied%d CommitIndex%d lastIncludedIndex%d PrevLogIndex%d PrevLogTerm%d  LastLogIndex%d EntriesLen%d", args.LeaderId, rf.me, rf.state, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, args.PrevLogIndex, args.PrevLogTerm, rf.getLastLogIndex(), len(args.Entries))

	// Non Snapshot
	if rf.getLogIndex(args.PrevLogIndex) >= 0 {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		if args.PrevLogIndex > rf.getLastLogIndex() {
			//follower's log is too short
			reply.XLen = rf.getLogLength()
			return
		}
		if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
			// term in the conflicting entry (if any)
			reply.XTerm = rf.getLogEntry(args.PrevLogIndex).Term
			// index of first entry with that term (if any)
			for reply.XIndex = args.PrevLogIndex; rf.getLogIndex(reply.XIndex) > 0 && rf.getLogEntry(reply.XIndex).Term == reply.XTerm; reply.XIndex-- {

			}
			reply.XIndex++
			return
		}

		//If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it (§5.3)
		rf.Logs = append(rf.getFractionLog(-1, args.PrevLogIndex+1), args.Entries...)

		// if rf.getLogLength() > newLogIndex {
		// 	rf.Logs = append(rf.getFractionLog(-1, newLogIndex), args.Entries...)
		// } else if rf.getLogLength() == newLogIndex {
		// 	// Append any new entries
		// 	rf.Logs = append(rf.Logs[:], args.Entries...)
		// }
		//  If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit >= rf.getLastLogIndex() {
				rf.SetCommitIndex(rf.getLastLogIndex())
			} else {
				rf.SetCommitIndex(args.LeaderCommit)
			}
		}
		reply.Success = true
	} else {
		DPrintf(dWarn, "Error Append rpc")
	}

}

// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)
func (rf *Raft) broadcastAppendEntries() {
	DPrintf(dAppend, "S%d lastIncludedIndex%d lastApplied%d commitIndex%d matchIndex%v nextIndex%v Term%d LastLogIndex%d LastLogTerm:%d", rf.me, rf.lastIncludedIndex, rf.lastApplied, rf.commitIndex, rf.matchIndex, rf.nextIndex, rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		args := AppendEntriesArg{
			LeaderId: rf.me,
		}
		rf.handleRpc(server, &args)
	}
}

func (rf *Raft) appendEntryRpc(server int, args *AppendEntriesArg) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// idempotent
	if reply.Term < rf.currentTerm || rf.state != Leader || args.Term != rf.currentTerm || rf.aboveCurrentTerm(reply.Term) {
		return
	}

	if reply.Success {
		// If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		DPrintf(dRefresh, "S%d => S%d index%d", rf.me, server, args.PrevLogIndex+len(args.Entries))
		rf.refreshMatchIndex(server, args.PrevLogIndex+len(args.Entries))
	} else {
		//Case 1: leader doesn't have XTerm:
		// nextIndex = XIndex
		// Case 2: leader has XTerm:
		//   nextIndex = leader's last entry for XTerm
		// Case 3: follower's log is too short:
		//   nextIndex = XLen
		if reply.XLen > 0 {
			rf.nextIndex[server] = reply.XLen
		} else {
			var i = rf.getLastLogIndex()
			for ; rf.getLogIndex(i) > 0 && rf.getLogEntry(i).Term != reply.XTerm; i-- {

			}
			if rf.getLogEntry(i).Term == reply.XTerm {
				rf.nextIndex[server] = i
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
		rf.handleRpc(server, args)
	}

}

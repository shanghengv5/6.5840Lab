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
	reply.Success = true
	reply.Term = args.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf(dClient, "S%d  leaderCommit%d commitIndex:%d", rf.me, args.LeaderCommit, rf.commitIndex)
	// DPrintf(dClient, "S%d Args:PrevLogIndex%d PrevLogTerm%d Logs%v rfCommitIndex%d rfLogs:%v", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.commitIndex, rf.Logs)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// If AppendEntries RPC received from new leader: convert to follower
	rf.currentTerm = args.Term
	rf.initFollower()
	rf.sendToChannel(rf.heartbeatCh, true)

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.Logs) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = len(rf.Logs)
		return
	}
	if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// term in the conflicting entry (if any)
		reply.XTerm = rf.Logs[args.PrevLogIndex].Term
		// index of first entry with that term (if any)
		for reply.XIndex = args.PrevLogIndex; reply.XIndex > 0 && rf.Logs[args.PrevLogIndex].Term == reply.XTerm; reply.XIndex-- {

		}
		reply.XIndex++
		return
	}

	newLogIndex := args.PrevLogIndex + 1

	//If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(rf.Logs) > newLogIndex {
		rf.Logs = append(rf.Logs[:newLogIndex], args.Entries...)
	} else if len(rf.Logs) == newLogIndex {
		// Append any new entries
		rf.Logs = append(rf.Logs, args.Entries...)
	}

	//  If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.LeaderCommit >= rf.commitIndex {
		if args.LeaderCommit > rf.getLastLogIndex() {
			rf.SetCommitIndex(rf.getLastLogIndex())
		} else {
			rf.SetCommitIndex(args.LeaderCommit)
		}
	}

}

// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)
func (rf *Raft) broadcastAppendEntries() {
	// DPrintf(dLeader, "S%d  CommitIndex:%d", rf.me, rf.commitIndex)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		args := AppendEntriesArg{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      make([]LogEntry, 0),
			PrevLogIndex: rf.getLastLogIndex(),
			PrevLogTerm:  rf.getLastLogTerm(),
		}
		rf.copyEntries(server, &args)
		go rf.appendEntryRpc(server, &args)

	}
}

func (rf *Raft) copyEntries(server int, args *AppendEntriesArg) {
	nextIndex := rf.nextIndex[server]
	if rf.getLastLogIndex() >= nextIndex {
		args.Entries = make([]LogEntry, len(rf.Logs[nextIndex:]))
		copy(args.Entries, rf.Logs[nextIndex:])
		args.PrevLogIndex = nextIndex - 1
		args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
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
	if reply.Term < rf.currentTerm || rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if rf.aboveCurrentTerm(reply.Term) {
		return
	}
	if reply.Success {
		// If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		rf.refreshMatchIndex(server, args.PrevLogIndex+len(args.Entries))
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)

		// if reply.XLen > 0 {
		// 	rf.nextIndex[server] = reply.XLen
		// } else {
		// 	var i = rf.getLastLogIndex()
		// 	for ; i > 0 && rf.Logs[i].Term != reply.XTerm; i-- {

		// 	}
		// 	if rf.Logs[i].Term == reply.XTerm {
		// 		rf.nextIndex[server] = i
		// 	} else {
		// 		rf.nextIndex[server] = reply.XIndex
		// 	}
		// }
		rf.nextIndex[server]--
		rf.copyEntries(server, args)
		go rf.appendEntryRpc(server, args)
		return
	}

}

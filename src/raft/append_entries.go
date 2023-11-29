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
	if args.PrevLogIndex >= len(rf.Logs) ||
		rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	newLogIndex := args.PrevLogIndex + 1

	DPrintf(dTrace, "S%d  args%v newLogIndex%d", rf.me, args, newLogIndex)
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
	if args.LeaderCommit > rf.commitIndex {
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
		}

		if rf.getLastLogIndex() >= rf.nextIndex[server] {
			args.Entries = make([]LogEntry, len(rf.Logs[rf.nextIndex[server]:]))
			copy(args.Entries, rf.Logs[rf.nextIndex[server]:])
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
		} else {
			args.Entries = make([]LogEntry, 0)
			args.PrevLogIndex = rf.getLastLogIndex()
			args.PrevLogTerm = rf.getLastLogTerm()
		}

		go rf.appendEntryRpc(server, &args)

	}
}

func (rf *Raft) appendEntryRpc(server int, args *AppendEntriesArg) {
	reply := AppendEntriesReply{}
	// DPrintf(dCommit, "ClientS%d LastLogIndex%d NextIndex%d  Logs%v ", server, rf.getLastLogIndex(), rf.nextIndex[server], rf.Logs)
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
		rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		rf.nextIndex[server]--
		args.Entries = make([]LogEntry, len(rf.Logs[rf.nextIndex[server]:]))
		copy(args.Entries, rf.Logs[rf.nextIndex[server]:])
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
		go rf.appendEntryRpc(server, args)
		return
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	N := args.LeaderCommit + 1
	if N > rf.commitIndex {
		voteCount := 0
		for _, mI := range rf.matchIndex {
			if mI <= rf.getLastLogIndex() &&
				mI >= N &&
				rf.Logs[mI].Term == rf.currentTerm {
				// To eliminate problems like the one in Figure 8, Raft
				// never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
				// term are committed by counting replicas; once an entry
				// from the current term has been committed in this way,
				// then all prior entries are committed indirectly because
				// of the Log Matching Property. There are some situations
				// where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach
				// for simplicity
				voteCount++
			}
		}
		if voteCount >= rf.majority/2+1 {
			rf.SetCommitIndex(N)
		}
	}

	// DPrintf(dLeader, "client %d args%v ", server, args)
}

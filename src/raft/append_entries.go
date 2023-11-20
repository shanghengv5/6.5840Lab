package raft

type AppendEntriesArg struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Logs                                      []LogEntry
	LeaderCommit                              int
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
	DPrintf(dClient, "Leader%d Term:%d Client%d Term%d %s\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.state.String())

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// If AppendEntries RPC received from new leader: convert to follower
	rf.currentTerm = args.Term
	rf.initFollower()

	rf.sendToChannel(rf.heartbeatCh, true)
	// it is a heartbeat
	if args.Logs == nil {
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.Logs) ||
		rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)

}

// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)
func (rf *Raft) broadcastAppendEntries() {
	lastLogIndex := rf.getLastLogIndex()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		DPrintf(dLeader, "S%d send => %d", rf.me, server)
		nextIndex := rf.nextIndex[server]
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		if lastLogIndex >= nextIndex {
			args := AppendEntriesArg{
				LeaderId:     rf.me,
				Term:         rf.currentTerm,
				LeaderCommit: rf.commitIndex,
				Logs:         rf.Logs[nextIndex:],
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.Logs[nextIndex-1].Term,
			}
			go rf.appendEntryRpc(server, &args)
		}

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
	// Ignore invalid response
	if reply.Term < rf.currentTerm || rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if rf.aboveCurrentTerm(reply.Term) {
		return
	}
	if reply.Success {
		// If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		rf.nextIndex[server] = args.PrevLogIndex + 1
		rf.matchIndex[server] = args.PrevLogIndex + 1
	} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		nextIndex := args.PrevLogIndex
		rf.nextIndex[server] = nextIndex

		args.Logs = args.Logs[nextIndex:]
		args.PrevLogIndex = nextIndex - 1
		args.PrevLogTerm = rf.Logs[nextIndex-1].Term
		go rf.appendEntryRpc(server, args)
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	N := args.LeaderCommit

	if N > rf.commitIndex {
		newNMaps := make(map[int]int, 0)
		for _, mI := range rf.matchIndex {
			if mI >= N && rf.Logs[mI].Term == args.Term {
				newNMaps[mI]++
			}
		}
		for newN, voteCount := range newNMaps {
			if voteCount >= rf.majority/2+1 && N > newN {
				N = newN
			}
		}
		
		rf.commitIndex = N

	}

}

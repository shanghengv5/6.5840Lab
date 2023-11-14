package raft

type AppendEntriesArg struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries                                   map[int]LogEntry
	LeaderCommit                              int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	if args.Entries == nil {
		return
	}
	entry, ok := rf.entries[args.PrevLogIndex]
	if !ok || entry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	newEntry := args.Entries[args.LeaderCommit]
	rf.entries[args.LeaderCommit] = newEntry
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}

}

// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts (§5.2)
func (rf *Raft) broadcastAppendEntries() {
	args := AppendEntriesArg{
		LeaderId: rf.me,
		Term:     rf.currentTerm,
		Entries:  nil,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		DPrintf(dLeader, "S%d send => %d", rf.me, server)
		go rf.appendEntryRpc(server, &args)
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

}

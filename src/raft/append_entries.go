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

// Invoked by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	reply.Success = true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("AppendEntries: Leader%d Term:%d, Follower%d Term:%d Role is %s\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.state.String())

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.toFollower(args.Term)
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

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	}

	rf.initChannel()
	rf.state = Leader
	DPrintf(dInfo, "S%d become a leader", rf.me)
	rf.infoHeartbeat()
}

// send a periodically heartbeat
func (rf *Raft) infoHeartbeat() {
	args := AppendEntriesArg{
		LeaderId: rf.me,
		Term:     rf.currentTerm,
		Entries:  nil,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendAppendEntry(server, &args)
	}

}

func (rf *Raft) toFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteCount = 0

	if state != Follower {
		rf.sendToChannel(rf.convertFollowerCh, true)
	}

}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArg) {
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

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		return
	}

}

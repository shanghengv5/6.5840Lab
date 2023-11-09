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
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	// fmt.Printf("AppendEntries: Leader%d Term:%d, Follower%d Term:%d Role is %s\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.role.String())

	if args.Term < reply.Term {
		reply.Success = false
		return
	}

	// it is a heartbeat
	if args.Entries == nil {
		rf.mu.Lock()
		rf.votedFor = -1
		rf.role = Followers
		rf.currentTerm = args.Term
		rf.commitIndex = args.LeaderCommit
		rf.mu.Unlock()

		rf.RaftElectionTimeout = false
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
	rf.RaftElectionTimeout = false
}

// send a periodically heartbeat
func (rf *Raft) sendHeartbeat() bool {
	rf.mu.Lock()
	state := rf.role
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	args := AppendEntriesArg{
		LeaderId: rf.me,
		Term:     curTerm,
		Entries:  nil,
	}
	return state == Leader && rf.sendAppendEntry(&args)
}

func (rf *Raft) sendAppendEntry(args *AppendEntriesArg) bool {
	// Yourself is always agree
	sum, support := 1, 1
	voteCh := make(voteCh, len(rf.peers)-1)
	for server := range rf.peers {
		reply := AppendEntriesReply{}
		if server == args.LeaderId {
			continue
		}
		sum, support := 0, 0
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		if ok {
			sum = 1
			if reply.Success {
				support = 1
			}
		}
		voteCh <- struct {
			Sum     int
			Support int
		}{sum, support}
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		ch := <-voteCh
		sum += ch.Sum
		support += ch.Support
	}
	return rf.halfVote(sum, support)
}

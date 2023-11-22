package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = true
	reply.Term = args.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("RequestVote... Sender%d term:%d,  Receiver%d  VotedFor: %d Term: %d state:%s Time: %s \n",
	// 	args.CandidateId, args.Term, rf.me, rf.votedFor, rf.currentTerm, rf.state.String(), time.Now().Format("15:04:05.000"))
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.aboveCurrentTerm(args.Term) {
		return
	}

	if rf.grantVoteCheck(args.CandidateId, args.LastLogIndex, args.LastLogTerm) {
		rf.grantingVote(args.CandidateId)
	}

}

// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) grantVoteCheck(candidateId, lastIndex, lastTerm int) bool {
	if rf.getLastLogIndex() > lastIndex {
		return false
	}
	if rf.Logs[lastIndex].Term != lastTerm {
		return false
	}
	return rf.votedFor == -1 || rf.votedFor == candidateId
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) requestVoteRpc(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}

	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	// DPrintf(dVote, "S%d VoteResult voteGranted%v voteCount:%d ok:%v", server, reply.VoteGranted, rf.voteCount, ok)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.state != Candidate || rf.currentTerm > reply.Term {
		return
	}
	if rf.aboveCurrentTerm(reply.Term) {
		return
	}

	// If votes received from majority of servers: become leader
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteMajorities() {
			rf.becomeLeader()
			return
		}
	}

}

// Send RequestVote RPCs to all other servers
func (rf *Raft) broadcastRequestVote() {
	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.requestVoteRpc(server, &arg)
	}
}

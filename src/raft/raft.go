package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Followers Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	maps := map[Role]string{
		Followers: "Followers",
		Candidate: "Candidate",
		Leader:    "Leader",
	}

	return maps[r]

}

// A log entry implement
type LogEntry struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	role Role

	// state a Raft server must maintain.
	RaftElectionTimeout int64

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	voteTimes   int64
	entries     map[int]LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.sendHeartbeat() && !rf.killed() {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) InitFollower() {
	rf.votedFor = -1
	rf.role = Followers
}

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
	fmt.Printf(
		"RequestVote: Candidate %d term:%d, lastLogIndex:%d, lastLogTerm:%d, Reply:%d,role: %s, votedFor: %d, commitIndex: %d, currentTerm: %d, Time: %s \n",
		args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.role.String(), rf.votedFor, rf.commitIndex, rf.currentTerm, time.Now().Format("15:04:05.000"))
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm || rf.role == Candidate {
		return
	}
	rf.AddElectionTime()
	// Agree vote
	if rf.votedFor == -1 && rf.commitIndex == args.LastLogIndex && rf.currentTerm == args.LastLogTerm {
		// A good Followers
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.votedFor = args.CandidateId
		rf.role = Followers
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startRequestVote() {
	rf.role = Candidate
	rf.votedFor = rf.me
	// Vote process
	arg := RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  rf.currentTerm, // No Log Now
	}
	sum, support := 1, 1
	response := make(map[int]string, 0)

	fmt.Printf("StartVote: Server%d, Term:%d try %d Times,Time:%s \n", rf.me, arg.Term, rf.voteTimes, time.Now().Format("15:04:05.000"))
	for server := range rf.peers {
		reply := RequestVoteReply{}
		if server == rf.me {
			continue
		}

		ok := rf.sendRequestVote(server, &arg, &reply)

		if ok {
			sum++
			if reply.VoteGranted {
				response[server] = "agree"
				support++
			} else {
				response[server] = "reject"
			}
		} else {
			response[server] = "lost connect"
		}

	}
	fmt.Printf("VoteResult: Server%d, Term:%d, Sum:%d, Support:%d, Response:%v, Time: %s\n", rf.me, rf.currentTerm, sum, support, response, time.Now().Format("15:04:05.000"))
	// vote success
	if rf.role == Candidate && rf.halfVote(support, sum) {
		rf.currentTerm++
		rf.role = Leader
		rf.voteTimes = 0

		fmt.Printf("Start Heartbeat: Server%d Term:%d,Time:%s \n", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
		// send first heartbeat
		for rf.sendHeartbeat() {
			rf.AddElectionTime()
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Printf("Stop Heartbeat: Server%d,Time:%s \n", rf.me, time.Now().Format("15:04:05.000"))
	}

	rf.InitFollower()
	rf.AddElectionTime()
}

type AppendEntriesArg struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries                                   map[int]LogEntry
	LeaderCommit                              int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AddElectionTime() {
	rf.RaftElectionTimeout = 50 + (rand.Int63() % 300)
}

func (rf *Raft) ClearElectionTime() {
	rf.RaftElectionTimeout = 0
}

// Invoked by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// it is a heartbeat
	if args.Entries == nil {
		fmt.Printf("AppendEntries: Leader%d Term:%d, Follower%d Term:%d Role is %s\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, rf.role.String())
		rf.votedFor = -1
		rf.role = Followers
		rf.currentTerm = args.Term
		rf.AddElectionTime()
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
	rf.AddElectionTime()

}

// send a periodically heartbeat
func (rf *Raft) sendHeartbeat() bool {
	args := AppendEntriesArg{
		Term:    rf.currentTerm,
		Entries: nil,
	}
	return rf.role == Leader && rf.sendAppendEntry(&args)
}

func (rf *Raft) sendAppendEntry(args *AppendEntriesArg) bool {
	// Yourself is always agree
	sum, confirm := 1, 1
	for server := range rf.peers {
		reply := AppendEntriesReply{}
		if server == rf.me {
			continue
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		if ok {
			sum++
			if reply.Success {
				confirm++
			}
		}
	}

	// fmt.Printf("%d server, term :%d, Heartbeat sum: %d, support: %d\n", rf.me, rf.currentTerm, sum, confirm)
	return rf.halfVote(confirm, sum)
}

func (rf *Raft) halfVote(confirm, sum int) (success bool) {
	if sum > 1 && confirm*2 > sum {
		success = true
	}

	return
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here (2A)
		for rf.RaftElectionTimeout > 0 {
			ms := rf.RaftElectionTimeout
			rf.ClearElectionTime()
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		// Start vote
		if rf.RaftElectionTimeout <= 0 {
			rf.startRequestVote()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	rf.AddElectionTime()

	// Your initialization code here (2A, 2B, 2C).
	rf.entries = make(map[int]LogEntry)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

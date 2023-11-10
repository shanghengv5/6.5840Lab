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
	RaftElectionTimeout bool

	HeartbeatCh chan int

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
	// fmt.Println("GetState...", rf.me)
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
	rf.mu.Lock()
	rf.role = Followers
	rf.votedFor = -1
	rf.mu.Unlock()
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
	rf.mu.Lock()
	curTerm := rf.currentTerm
	voteFor := rf.votedFor
	state := rf.role
	rf.mu.Unlock()

	// fmt.Printf("RequestVote: Candidate%d term:%d, args.LastLogIndex:%d args.LastLogTerm:%d Reply%d Role is %s VotedFor: %d Term: %d, commitIndex:%d Time: %s \n",
	// args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, rf.role.String(), rf.votedFor, curTerm, rf.commitIndex, time.Now().Format("15:04:05.000"))
	// Your code here (2A, 2B).
	if args.Term < curTerm || state != Followers {
		return
	}
	rf.RaftElectionTimeout = false

	// Agree vote
	if voteFor == -1 && rf.commitIndex == args.LastLogIndex && curTerm == args.LastLogTerm {
		// A good Followers
		reply.VoteGranted = true
		reply.Term = args.Term

		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.role = Followers
		rf.mu.Unlock()
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

type voteCh chan struct {
	Sum, Support int
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.votedFor = rf.me
	curTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	arg := RequestVoteArgs{
		Term:         curTerm + 1,
		CandidateId:  me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  curTerm, // No Log Now
	}
	rf.voteTimes++
	// fmt.Printf("StartVote: Server%d, Term:%d try %d Times,Time:%s \n", me, rf.currentTerm, rf.voteTimes, time.Now().Format("15:04:05.000"))
	sum, support := 1, 1
	voteCh := make(voteCh, len(rf.peers)-1)
	for server := range rf.peers {
		reply := RequestVoteReply{}
		if server == me {
			continue
		}

		go func(server int) {
			ok := rf.sendRequestVote(server, &arg, &reply)
			sum, support := 0, 0
			if ok {
				sum = 1
				if reply.VoteGranted {
					support = 1
				}
			}
			voteCh <- struct {
				Sum     int
				Support int
			}{sum, support}
		}(server)
	}

	for server := range rf.peers {
		if server == me {
			continue
		}
		ch := <-voteCh
		sum += ch.Sum
		support += ch.Support
	}
	// fmt.Printf("VoteResult: Server%d, Term:%d,  Sum:%d Support:%d Time: %s\n", me, rf.currentTerm, sum, support, time.Now().Format("15:04:05.000"))
	rf.mu.Lock()
	curRole := rf.role
	rf.mu.Unlock()

	// vote success
	if curRole == Candidate && rf.halfVote(sum, support) {
		rf.mu.Lock()
		rf.currentTerm++
		rf.role = Leader
		rf.votedFor = -1
		rf.mu.Unlock()

		fmt.Printf("Start heartbeat... Server%d Term:%d Time:%s \n", me, curTerm+1, time.Now().Format("15:04:05.000"))
		// send first heartbeat
		go func() {
			for rf.sendHeartbeat() {
				rf.RaftElectionTimeout = false
				// fmt.Printf("heartbeating...%d Time:%s\n", me, time.Now().Format("15:04:05.000"))s
				time.Sleep(100 * time.Millisecond)
			}
			rf.HeartbeatCh <- 0
			fmt.Printf("Stop heartbeat... Server%d Time:%s \n", me, time.Now().Format("15:04:05.000"))
		}()

		rf.voteTimes = 0
		<-rf.HeartbeatCh

	}
}

func (rf *Raft) halfVote(sum, support int) (success bool) {
	if sum > 1 && support*2 > sum {
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
		voteFor := rf.votedFor
		if rf.RaftElectionTimeout && voteFor == -1 {
			rf.startElection()
		} else {
			rf.RaftElectionTimeout = true
			rf.InitFollower()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

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

	rf.RaftElectionTimeout = false
	rf.HeartbeatCh = make(chan int)

	// Your initialization code here (2A, 2B, 2C).
	rf.entries = make(map[int]LogEntry)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

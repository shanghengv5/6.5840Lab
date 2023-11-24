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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const HEARTBEAT = 300

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	state State

	// state a Raft server must maintain.
	// Channel
	heartbeatCh        chan bool
	convertLeaderCh    chan bool
	convertFollowerCh  chan bool
	convertCandidateCh chan bool

	applyCh chan ApplyMsg

	majority  int
	voteCount int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	Logs        []LogEntry

	// Volatile state on all servers
	// index of highest log entry known to be
	// committed (initialized to 0, increases
	// monotonically)
	commitIndex int
	// index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)
	lastApplied int

	// Volatile state on leaders
	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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

func (rf *Raft) sendToChannel(ch chan bool, b bool) {
	select {
	case ch <- b:
	default:
	}
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.commitIndex
	term = rf.currentTerm
	isLeader = rf.state == Leader

	// Your code here (2B).
	if !isLeader {
		return
	}
	rf.Logs = append(rf.Logs, LogEntry{Term: rf.currentTerm, Command: command})
	//If command received from client: append entry to local log,
	// respond after entry applied to state machine
	index = rf.getLastLogIndex()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	DPrintf(dCommit, "S%d => commitIndex%d Logs%v", rf.me, rf.commitIndex, rf.Logs)
	return
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

func (rf *Raft) waitElectionTimeOut() time.Duration {
	ms := HEARTBEAT + 10 + (rand.Int63() % HEARTBEAT)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		rf.mu.Lock()
		state := rf.state
		rf.commitIndexAboveLastApplied()
		rf.mu.Unlock()
		switch state {
		case Follower:
			select {
			case <-rf.heartbeatCh:
			case <-rf.convertCandidateCh:
			case <-time.After(rf.waitElectionTimeOut()):
				// If election timeout elapses: start new election
				rf.startElection(state)
			}
		case Candidate:
			select {
			case <-rf.convertFollowerCh:
			case <-rf.convertLeaderCh:
			case <-time.After(rf.waitElectionTimeOut()):
				// If election timeout elapses: start new election
				rf.startElection(state)
			}
		case Leader:
			select {
			case <-rf.convertFollowerCh:
			case <-time.After(HEARTBEAT * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.majority = len(rf.peers)
	rf.Logs = append(rf.Logs, LogEntry{Term: 0})

	rf.initFollower()
	rf.initChannel()
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, rf.majority)
	rf.matchIndex = make([]int, rf.majority)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) initChannel() {
	rf.heartbeatCh = make(chan bool)
	rf.convertFollowerCh = make(chan bool)
	rf.convertLeaderCh = make(chan bool)
	rf.convertCandidateCh = make(chan bool)
}

func (rf *Raft) initFollower() {
	rf.Convert(Follower)
	rf.votedFor = -1
	rf.voteCount = 0
}

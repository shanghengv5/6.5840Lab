package raft

type Snapshot struct {
	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int
	// term of lastIncludedIndex
	LastIncludedTerm int

	StateMachineState interface{}
}
type InstallSnapshotArg struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderId int
	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int
	// term of lastIncludedIndex
	LastIncludedTerm int
	// byte offset where chunk is positioned in the
	// snapshot file
	Offset int
	// raw bytes of the snapshot chunk, starting at
	// offset
	Data []byte
	// true if this is the last chunk
	Done bool
}

type InstallSnapshotReply struct {
	// currentTerm, for leader to update itself
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	newLogs := []LogEntry{
		LogEntry{},
	}
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	if index+1 < rf.getLastLogIndex() {
		newLogs = append(newLogs, rf.getFractionLog(index+1, -1)...)
	}
	
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	rf.lastIncludedIndex = index
	rf.Logs = newLogs
	DPrintf(dSnap, "S%d index%d LogLen%d", rf.me, index, len(rf.Logs))
}

// 1. Reply immediately if term < currentTerm
// 2. Create new snapshot file if first chunk (offset is 0)
// 3. Write data into snapshot file at given offset
// 4. Reply and wait for more data chunks if done is false
// 5. Save snapshot file, discard any existing or partial snapshot
// with a smaller index
// 6. If existing log entry has same index and term as snapshot’s
// last included entry, retain log entries following it and reply
// 7. Discard the entire log
// 8. Reset state machine using snapshot contents (and load
// snapshot’s cluster configuration)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.aboveCurrentTerm(args.Term) {
		return
	}
	rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	newLogs := []LogEntry{
		LogEntry{},
	}
	if args.LastIncludedIndex+1 < rf.getLastLogIndex() {
		newLogs = append(newLogs, rf.getFractionLog(args.LastIncludedIndex+1, -1)...)
	}
	rf.Logs = newLogs
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

}

func (rf *Raft) installSnapshotRpc(server int, args *InstallSnapshotArg) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
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
	rf.refreshMatchIndex(server, rf.lastIncludedIndex)
}

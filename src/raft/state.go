package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	maps := map[State]string{
		Follower:  "Follower",
		Candidate: "Candidate",
		Leader:    "Leader",
	}
	return maps[s]
}

// Convert to new state
func (rf *Raft) Convert(state State) {
	if rf.state == state {
		return
	}
	rf.state = state
	switch state {
	case Follower:
		rf.sendToChannel(rf.convertFollowerCh, true)
	case Candidate:
		rf.sendToChannel(rf.convertCandidateCh, true)
	case Leader:
		rf.sendToChannel(rf.convertLeaderCh, true)
	}
}

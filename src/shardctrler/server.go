package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const LEADER_WAIT int64 = 200
const WAIT int64 = 10

const CHECK_WAIT int64 = 0

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	requestValid map[int64]map[int64]Op
	configs      []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Op      string
	Servers map[int][]string
	GIDS    []int
	Shard   int
	GID     int
	Num     int // desired config number
	Config  Config
	ClientHeader
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{
		Op:           "Join",
		Servers:      args.Servers,
		ClientHeader: args.ClientHeader,
	}
	DPrintf(dServer, "S(%d) %s Start RequestId(%d)", sc.me, cmd.Op, args.ClientId)
	respTime := WAIT
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		respTime = LEADER_WAIT
	} else {
		reply.WrongLeader = true
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		sc.mu.Lock()
		_, ok := sc.requestValid[args.ClientId][args.Seq]
		sc.mu.Unlock()
		if ok {
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{
		Op:           "Leave",
		GIDS:         args.GIDs,
		ClientHeader: args.ClientHeader,
	}
	// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
	respTime := WAIT
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		respTime = LEADER_WAIT
	} else {
		reply.WrongLeader = true
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		sc.mu.Lock()
		_, ok := sc.requestValid[args.ClientId][args.Seq]
		sc.mu.Unlock()
		if ok {
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{
		Op:           "Move",
		GID:          args.GID,
		Shard:        args.Shard,
		ClientHeader: args.ClientHeader,
	}
	// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
	respTime := WAIT
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		respTime = LEADER_WAIT
	} else {
		reply.WrongLeader = true
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		sc.mu.Lock()
		_, ok := sc.requestValid[args.ClientId][args.Seq]
		sc.mu.Unlock()
		if ok {
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{
		Op:           "Query",
		Num:          args.Num,
		ClientHeader: args.ClientHeader,
	}
	DPrintf(dServer, "S(%d) %s Start Seq(%d)", sc.me, cmd.Op, args.Seq)
	respTime := WAIT
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		respTime = LEADER_WAIT
	} else {
		reply.WrongLeader = true
	}
	DPrintf(dServer, "S(%d) %s Is Leader:%v Seq(%d)", sc.me, cmd.Op, isLeader, args.Seq)
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		sc.mu.Lock()
		op, ok := sc.requestValid[args.ClientId][args.Seq]
		sc.mu.Unlock()
		if ok {
			reply.Config = op.Config
			reply.WrongLeader = false
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestValid = make(map[int64]map[int64]Op)

	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		DPrintf(dApply, "S(%d) CommandIndex(%d) command(%v)", sc.me, m.CommandIndex, m.Command)
		if m.SnapshotValid {
			// sc.readSnapshot(m.Snapshot)
		} else if m.CommandValid {
			sc.mu.Lock()
			op, ok := m.Command.(Op)
			if !ok {
				panic("Not a op command")
			}
			// init seq map
			if _, ok := sc.requestValid[op.ClientId]; !ok {
				sc.requestValid[op.ClientId] = make(map[int64]Op)
			}
			_, repeatRequestOk := sc.requestValid[op.ClientId][op.Seq]
			// Repeat request don't calculate again
			if !repeatRequestOk {
				if op.Op == "Join" {
					cfg := sc.createNewConfig()
					// join
					for gid, servers := range op.Servers {
						cfg.Groups[gid] = servers
					}
					cfg.Shards = sc.distributeShards(cfg.Groups)
					sc.configs = append(sc.configs, cfg)
				} else if op.Op == "Leave" {
					cfg := sc.createNewConfig()
					// join
					for _, gid := range op.GIDS {
						delete(cfg.Groups, gid)
					}
					cfg.Shards = sc.distributeShards(cfg.Groups)
					sc.configs = append(sc.configs, cfg)
				} else if op.Op == "Move" {
					cfg := sc.createNewConfig()
					// Move
					cfg.Shards[op.Shard] = op.GID
					sc.configs = append(sc.configs, cfg)
				} else if op.Op == "Query" {
					if op.Num < len(sc.configs) {
						if op.Num == -1 {
							op.Config = sc.configs[len(sc.configs)-1]
						} else {
							op.Config = sc.configs[op.Num]
						}
					}
				}
				sc.requestValid[op.ClientId][op.Seq] = op
				for key, _ := range sc.requestValid[op.ClientId] {
					if key < op.Seq {
						delete(sc.requestValid[op.ClientId], key)
					}
				}

			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) createNewConfig() Config {
	prv := sc.configs[len(sc.configs)-1]
	cfg := Config{
		Num:    prv.Num + 1,
		Shards: prv.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range prv.Groups {
		cfg.Groups[gid] = servers
	}
	return cfg
}

func (sc *ShardCtrler) distributeShards(servers map[int][]string) [NShards]int {
	res := [NShards]int{}
	gids := []int{}
	for idx, _ := range servers {
		gids = append(gids, idx)
	}
	sort.Ints(gids)
	gidsLen := len(gids)
	if gidsLen == 0 {
		return res
	}
	for i := 0; i < NShards; i++ {
		res[i] = gids[i%gidsLen]
	}
	return res
}

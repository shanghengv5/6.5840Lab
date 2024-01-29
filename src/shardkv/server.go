package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const LEADER_WAIT int64 = 200
const WAIT int64 = 10

const CHECK_WAIT int64 = 0

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Op         string
	WrongGroup bool
	Server     string
	Data       map[string]string
	ClientHeader
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck          *shardctrler.Clerk
	requestValid map[int64]map[int64]Op
	data         map[string]string
	config       shardctrler.Config
	ClientHeader
}

func (ck *ShardKV) getHeader() ClientHeader {
	ck.Seq++
	return ClientHeader{
		ClientId: ck.ClientId,
		Seq:      ck.Seq,
	}
}

func (kv *ShardKV) Talk(args *TalkArgs, reply *TalkReply) {
	cmd := Op{
		Op:           "Talk",
		Data:         args.Data,
		Server:       args.Server,
		ClientHeader: args.ClientHeader,
	}

	respTime := WAIT
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		respTime = LEADER_WAIT
		reply.Err = ErrTimeout
	} else {
		reply.Err = ErrWrongLeader
	}

	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		kv.mu.Lock()
		op, ok := kv.requestValid[args.ClientId][args.Seq]
		kv.mu.Unlock()
		if ok {
			if op.WrongGroup {
				reply.Err = ErrWrongGroup
				return
			}
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (kv *ShardKV) checkWrongGroup(key string, argsServer string) bool {
	gid := kv.config.Shards[key2shard(key)]
	servers := kv.config.Groups[gid]
	b := true
	for _, server := range servers {
		if server == argsServer {
			return false
		}
	}
	return b
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Op:           "Get",
		Key:          args.Key,
		Server:       args.Server,
		ClientHeader: args.ClientHeader,
	}

	respTime := WAIT
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		reply.Err = ErrTimeout
		respTime = LEADER_WAIT
	} else {
		reply.Err = ErrWrongLeader
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		kv.mu.Lock()
		op, ok := kv.requestValid[args.ClientId][args.Seq]
		kv.mu.Unlock()
		if ok {
			if op.WrongGroup {
				reply.Err = ErrWrongGroup
				return
			}
			reply.Value = op.Value
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		Op:           args.Op,
		Key:          args.Key,
		Value:        args.Value,
		Server:       args.Server,
		ClientHeader: args.ClientHeader,
	}

	respTime := WAIT
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
		respTime = LEADER_WAIT
		reply.Err = ErrTimeout
	} else {
		reply.Err = ErrWrongLeader
	}

	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		kv.mu.Lock()
		op, ok := kv.requestValid[args.ClientId][args.Seq]
		kv.mu.Unlock()
		if ok {
			if op.WrongGroup {
				reply.Err = ErrWrongGroup
				return
			}
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.requestValid = make(map[int64]map[int64]Op)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ClientId = nrand()

	go kv.applier()
	go kv.refreshConfig()
	return kv
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any snapshot?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var kvData = make(map[string]string)
	var requestValid = make(map[int64]map[int64]Op)
	err1 := d.Decode(&index)
	err2 := d.Decode(&kvData)
	err3 := d.Decode(&requestValid)
	if err1 != nil ||
		err2 != nil ||
		err3 != nil {
		panic("read snapshot error")
	}
	kv.mu.Lock()
	kv.data = kvData
	kv.requestValid = requestValid
	kv.mu.Unlock()
}

func (kv *ShardKV) refreshConfig() {
	for {
		kv.mu.Lock()
		cfg := kv.mck.Query(-1)
		if cfg.Num > kv.config.Num {
			trans := make(map[int]bool)
			for shard, gid := range kv.config.Shards {
				// choose relative gid
				if gid == kv.gid && cfg.Shards[shard] != gid {
					trans[cfg.Shards[shard]] = false
				}
			}

			for gid, _ := range trans {
				servers := cfg.Groups[gid]
				for !trans[gid] {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply TalkReply
						args := TalkArgs{
							Op:     "Talk",
							Data:   make(map[string]string),
							Server: servers[si],
						}
						args.ClientHeader = kv.getHeader()
						for k, v := range kv.data {
							args.Data[k] = v
						}
						DPrintf(dServer, "S(%d) gid(%d) refresh servers%s data%v", kv.me, kv.gid, servers[si], args.Data)
						ok := srv.Call("ShardKV.Talk", &args, &reply)
						if ok && reply.Err == OK {
							trans[gid] = true
							break
						}
					}
				}
			}
			kv.config = cfg
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		// DPrintf(dApply, "S(%d) CommandIndex(%d) command(%v)", kv.me, m.CommandIndex, m.Command)
		if m.SnapshotValid {
			kv.readSnapshot(m.Snapshot)
		} else if m.CommandValid {
			kv.mu.Lock()
			op, ok := m.Command.(Op)
			if !ok {
				panic("Not a op command")
			}
			// init seq map
			if _, ok := kv.requestValid[op.ClientId]; !ok {
				// DPrintf(dApply, "S(%d) set ClientId%d", kv.me, op.ClientId)
				kv.requestValid[op.ClientId] = make(map[int64]Op)
			}
			_, repeatRequestOk := kv.requestValid[op.ClientId][op.Seq]
			// Repeat request don't calculate again
			if !repeatRequestOk {
				if kv.checkWrongGroup(op.Key, op.Server) {
					op.WrongGroup = true
				} else {
					op.WrongGroup = false
					// Same group
					if op.Op == "Get" {
						op.Value = kv.data[op.Key]
					} else if op.Op == "Put" {
						kv.data[op.Key] = op.Value
					} else if op.Op == "Append" {
						kv.data[op.Key] += op.Value
					} else if op.Op == "Talk" {
						kv.data = op.Data
					}
				}
				// DPrintf(dApply, "S(%d) %v Value(%s) repeat ClientId%d Seq%d", kv.me, op.Op, op.Value, op.ClientId, op.Seq)
				kv.requestValid[op.ClientId][op.Seq] = op
				for key, _ := range kv.requestValid[op.ClientId] {
					if key < op.Seq {
						delete(kv.requestValid[op.ClientId], key)
					}
				}
			}

			if kv.rf.Persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				e.Encode(kv.data)
				e.Encode(kv.requestValid)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()
		}
	}
}

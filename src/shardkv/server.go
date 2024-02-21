package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const LEADER_WAIT int64 = 200
const WAIT int64 = 10

const CHECK_WAIT int64 = 0

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
	requestValid map[int64]int64
	shardData    ShardData
	dead         int32

	CurConfig shardctrler.Config
	OldConfig shardctrler.Config // need shards to point pull servers data

	index2ReplyChan map[int]chan StartCommandReply
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) requestIsDone(cmd Op) bool {
	return kv.requestValid[cmd.ClientId] >= cmd.Seq
}

func (kv *ShardKV) checkIsRunning(shard int) bool {
	if data, ok := kv.shardData[shard]; ok && (data.State == Running || data.State == PullDone) && kv.CurConfig.Shards[shard] == kv.gid {
		return true
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Op:           "Get",
		Key:          args.Key,
		ClientHeader: args.ClientHeader,
		Type:         "Outside",
	}

	kv.mu.Lock()
	if !kv.checkIsRunning(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	r := kv.StartCommand(cmd)
	reply.Err = r.Err
	reply.Value = r.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		Op:           args.Op,
		Key:          args.Key,
		Value:        args.Value,
		ClientHeader: args.ClientHeader,
		Type:         "Outside",
	}
	kv.mu.Lock()
	if !kv.checkIsRunning(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.requestIsDone(cmd) {
		reply.Err = OK
		kv.mu.Unlock()
		return

	}
	kv.mu.Unlock()
	reply.Err = kv.StartCommand(cmd).Err

}

func (kv *ShardKV) StartCommand(cmd Op) (reply StartCommandReply) {
	respTime := LEADER_WAIT
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.NewReplyChan(index)
	kv.mu.Unlock()
	select {
	case reply = <-ch:
	case <-time.After(time.Duration(respTime) * time.Millisecond):
		reply.Err = ErrTimeout
	}
	// DPrintf(dServer, "(%d-%d) %s Key(%s) Value(%s) %s Index(%d) Seq(%d) isLeader(%v)", kv.gid, kv.me, cmd.Op, cmd.Key, reply.Value, reply.Err, index, cmd.Seq, isLeader)
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
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
	kv.shardData = NewShardData()
	kv.index2ReplyChan = make(map[int]chan StartCommandReply)
	kv.requestValid = make(map[int64]int64)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.refreshConfig()
	go kv.pullData()
	go kv.updatePullDone()
	return kv
}

func (kv *ShardKV) NewReplyChan(index int) (ch chan StartCommandReply) {
	ch = make(chan StartCommandReply, 1)
	kv.index2ReplyChan[index] = ch
	return
}

func (kv *ShardKV) sendReplyToChan(index int, reply StartCommandReply) {
	if ch, ok := kv.index2ReplyChan[index]; ok {
		select {
		case ch <- reply:
			return
		default:
		}
	}
}

func (kv *ShardKV) writeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(m.CommandIndex)
	e.Encode(kv.CurConfig)
	e.Encode(kv.OldConfig)
	e.Encode(kv.shardData)
	e.Encode(kv.requestValid)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any snapshot?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curConfig shardctrler.Config
	var oldConfig shardctrler.Config
	var kvData = ShardData{}
	var requestValid = make(map[int64]int64)
	err1 := d.Decode(&curConfig)
	err2 := d.Decode(&oldConfig)
	err3 := d.Decode(&kvData)
	err4 := d.Decode(&requestValid)
	if err1 != nil ||
		err2 != nil ||
		err3 != nil ||
		err4 != nil {
		panic("read snapshot error")
	}
	kv.mu.Lock()
	kv.CurConfig = curConfig
	kv.OldConfig = oldConfig
	kv.shardData = kvData
	kv.requestValid = requestValid
	kv.mu.Unlock()
}

// update shard data when new config refresh
func (kv *ShardKV) updateShardDataState(oldCfg, newCfg shardctrler.Config) {
	for s := 0; s < shardctrler.NShards; s++ {
		// Pull
		if oldCfg.Shards[s] != 0 && // 0 ignore
			newCfg.Shards[s] == kv.gid && // new cfg need this group
			oldCfg.Shards[s] != kv.gid { // old cfg doesnt exists
			kv.shardData.UpdateState(s, Pull)
		}
		// Share
		if oldCfg.Shards[s] != 0 && // 0 ignore
			newCfg.Shards[s] != kv.gid && // new cfg doesnt need this group
			oldCfg.Shards[s] == kv.gid { // old cfg  exists
			kv.shardData.UpdateState(s, Share)
		}
	}
}

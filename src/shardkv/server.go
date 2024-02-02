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
	shardData    ShardData

	CurConfig shardctrler.Config
	OldConfig shardctrler.Config // need shards to point pull servers data

	ClientHeader
}

func (kv *ShardKV) checkIsOk(shard int) bool {
	kv.mu.Lock()
	shardData := kv.shardData
	kv.mu.Unlock()
	for s, _ := range shardData {
		if s == shard && kv.CurConfig.Shards[shard] == kv.gid {
			return true
		}
	}
	// DPrintf(dServer, "S(%d) wrong gId%d", kv.me, kv.gid)
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Op:           "Get",
		Key:          args.Key,
		ClientHeader: args.ClientHeader,
	}

	if !kv.checkIsOk(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		return
	}
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
	}
	if !kv.checkIsOk(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = kv.StartCommand(cmd).Err
}

func (kv *ShardKV) StartCommand(cmd Op) (reply StartCommandReply) {
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
		op, ok := kv.requestValid[cmd.ClientId][cmd.Seq]
		kv.mu.Unlock()
		if ok {
			reply.Err = op.Err
			reply.Value = op.Value
			reply.ShardData = op.ShardData
			break
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
	return
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
	kv.shardData = NewShardData()

	kv.requestValid = make(map[int64]map[int64]Op)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ClientId = nrand()

	go kv.applier()
	go kv.refreshConfig()
	go kv.pullData()
	return kv
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any snapshot?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var kvData = ShardData{}
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
	kv.shardData = kvData
	kv.requestValid = requestValid
	kv.mu.Unlock()
}

func (kv *ShardKV) pullData() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		oldCfg := kv.OldConfig
		shardData := kv.shardData
		args := MigrateArgs{
			ClientHeader: kv.getHeader(),
			Op:           "Pull",
			OldConfig:    oldCfg,
		}
		gid2ShardIds := shardData.getGid2ShardIds(Pull, oldCfg)
		kv.mu.Unlock()

		for gid, shardIds := range gid2ShardIds {
			servers := oldCfg.Groups[gid]
			args.ShardIds = shardIds

			for _, server := range servers {
				go kv.migrateRpc(server, &args)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) refreshConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		curCfg := kv.CurConfig
		nextCfg := kv.mck.Query(kv.CurConfig.Num + 1)
		shardData := kv.shardData
		kv.mu.Unlock()
		isUpdate := true
		for _, kv := range shardData {
			if kv.State != Ok {
				isUpdate = false
				break
			}
		}
		if isUpdate && nextCfg.Num == curCfg.Num+1 {
			kv.StartCommand(Op{
				ClientHeader: kv.getHeader(),
				Op:           "Refresh",
				NextCfg:      nextCfg,
			})
		}
		time.Sleep(100 * time.Millisecond)
	}
}

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
				op.Err = OK
				if op.Op == "Get" {
					op.Value = kv.shardData.Get(key2shard(op.Key), op.Key)
				} else if op.Op == "Put" {
					kv.shardData.Put(key2shard(op.Key), op.Key, op.Value)
				} else if op.Op == "Append" {
					kv.shardData.Append(key2shard(op.Key), op.Key, op.Value)
				} else if op.Op == "Pull" {
					if op.OldConfig.Num == kv.OldConfig.Num {
						op.ShardData = ShardData{}
						for _, sid := range op.ShardIds {
							shardKv := kv.shardData[sid]
							op.ShardData.UpdateData(sid, shardKv.Data)
							if shardKv.State == Share {
								kv.shardData.UpdateState(sid, Ok)
							}
						}
					} else {
						op.Err = ErrConfigChange
					}
				} else if op.Op == "Refresh" {
					if op.NextCfg.Num == kv.CurConfig.Num+1 {
						// set ShardData state
						kv.OldConfig = kv.CurConfig
						kv.CurConfig = op.NextCfg
						kv.updateShardDataState(kv.OldConfig, kv.CurConfig)
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
				e.Encode(kv.shardData)
				e.Encode(kv.requestValid)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()
		}
	}
}

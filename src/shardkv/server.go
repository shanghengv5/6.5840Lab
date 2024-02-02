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
	requestValid map[int64]map[int64]StartCommandReply
	shardData    ShardData

	CurConfig shardctrler.Config
	OldConfig shardctrler.Config // need shards to point pull servers data

	index2ReplyChan map[int]chan StartCommandReply
	ClientHeader
}

func (kv *ShardKV) checkIsOk(shard int) bool {
	if data, ok := kv.shardData[shard]; ok && data.State == Ok && kv.CurConfig.Shards[shard] == kv.gid {
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
	if !kv.checkIsOk(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	r, ok := kv.requestValid[cmd.ClientId][cmd.Seq]
	if ok {
		reply.Err = r.Err
		reply.Value = r.Value
		DPrintf(dServer, "(%d-%d) respondByOld %s Key%s Value%s %s  Seq%d", kv.gid, kv.me, cmd.Op, cmd.Key, reply.Value, reply.Err, cmd.Seq)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	r = kv.StartCommand(cmd)
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
	if !kv.checkIsOk(key2shard(cmd.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	r, ok := kv.requestValid[cmd.ClientId][cmd.Seq]
	if ok {
		reply.Err = r.Err
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
	DPrintf(dServer, "(%d-%d) respond %s Key%s Value(%s) %s Index%d Seq%d isLeader%v", kv.gid, kv.me, cmd.Op, cmd.Key, reply.Value, reply.Err, index, cmd.Seq, isLeader)
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
	kv.index2ReplyChan = make(map[int]chan StartCommandReply)
	kv.requestValid = make(map[int64]map[int64]StartCommandReply)
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

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any snapshot?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	var kvData = ShardData{}
	var requestValid = make(map[int64]map[int64]StartCommandReply)
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
		if len(gid2ShardIds) > 0 {
			for gid, shardIds := range gid2ShardIds {
				servers := oldCfg.Groups[gid]
				args.ShardIds = shardIds
				for _, server := range servers {
					go kv.migrateRpc(server, &args)
				}
			}
			DPrintf(dMigrate, "%v PullData Rpc configNum%d (%v)", kv.gid, oldCfg.Num, gid2ShardIds)
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
		// DPrintf(dServer, "(%d)REFERSH CurConfigNum%d", kv.gid, kv.CurConfig.Num)
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

func (kv *ShardKV) applyInternal(op Op, reply *StartCommandReply) {
	// DPrintf(dApply, "Internal (%d-%d)%s) opConfigNum%d configNum%d", kv.gid, kv.me, op.Op, op.OldConfig.Num, kv.OldConfig.Num)
	if op.Op == "Pull" {
		if op.OldConfig.Num == kv.OldConfig.Num {
			reply.ShardData = ShardData{}
			for _, sid := range op.ShardIds {
				shardKv := kv.shardData[sid]
				reply.ShardData.UpdateData(sid, shardKv.Data)
				if shardKv.State == Share {
					kv.shardData.UpdateState(sid, Ok)
				}
			}
		} else {
			reply.Err = ErrConfigChange
		}
	} else if op.Op == "Refresh" {
		if op.NextCfg.Num == kv.CurConfig.Num+1 {
			// set ShardData state
			kv.OldConfig = kv.CurConfig
			kv.CurConfig = op.NextCfg
			kv.updateShardDataState(kv.OldConfig, kv.CurConfig)
		}
	} else if op.Op == "Sync" {
		if op.OldConfig.Num == kv.OldConfig.Num {
			for shard, data := range op.ShardData {
				kv.shardData.UpdateData(shard, data.Data)
				kv.shardData.UpdateState(shard, Ok)
			}
			kv.writeRequestValid(op.RequestValid)
		}
	}
}

func (kv *ShardKV) applyOutSide(op Op, reply *StartCommandReply) {
	// init seq map
	if _, ok := kv.requestValid[op.ClientId]; !ok {
		// DPrintf(dApply, "S(%d) set ClientId%d", kv.me, op.ClientId)
		kv.requestValid[op.ClientId] = make(map[int64]StartCommandReply)
	}
	_, repeatRequestOk := kv.requestValid[op.ClientId][op.Seq]
	// Repeat request don't calculate again
	if !repeatRequestOk {
		shard := key2shard(op.Key)
		if !kv.checkIsOk(shard) {
			reply.Err = ErrWrongGroup
			return
		}
		if op.Op == "Get" {
			reply.Value = kv.shardData.Get(shard, op.Key)
		} else if op.Op == "Put" {
			kv.shardData.Put(shard, op.Key, op.Value)
		} else if op.Op == "Append" {
			kv.shardData.Append(shard, op.Key, op.Value)
		}
		kv.requestValid[op.ClientId][op.Seq] = *reply

		for key, _ := range kv.requestValid[op.ClientId] {
			if key < op.Seq {
				delete(kv.requestValid[op.ClientId], key)
			}
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
			reply := StartCommandReply{Err: OK}
			if op.Type == "Outside" {
				kv.applyOutSide(op, &reply)
			} else {
				kv.applyInternal(op, &reply)
			}
			kv.sendReplyToChan(m.CommandIndex, reply)
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

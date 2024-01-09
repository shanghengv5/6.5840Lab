package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string
	applyLog map[int]interface{}
	term     int
}

func (kv *KVServer) checkLog(index int, oldOp Op) bool {
	if command, ok := kv.applyLog[index]; ok {
		if op, ok := command.(Op); ok {
			if op == oldOp {
				return true
			}
		}

	}

	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
	}
	oldOp := Op{
		Op:    "Get",
		Key:   args.Key,
		Value: "",
	}
	index, term, isLeader := kv.rf.Start(oldOp)
	t := time.Now()
	if isLeader && term >= kv.term {
		kv.term = term
		for time.Since(t).Seconds() < 10 {
			if kv.checkLog(index, oldOp) {
				reply.Err = ""
				reply.Value = kv.data[args.Key]
				return
			}
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldOp := Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	index, term, isLeader := kv.rf.Start(oldOp)
	t := time.Now()
	if isLeader && term >= kv.term {
		kv.term = term
		for time.Since(t).Seconds() < 10 {
			if kv.checkLog(index, oldOp) {
				reply.Err = ""
				kv.data[oldOp.Key] = oldOp.Value
				return
			}
		}
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.applyLog = make(map[int]interface{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			// err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
			kv.mu.Unlock()
		} else if m.CommandValid {
			kv.mu.Lock()
			kv.applyLog[m.CommandIndex] = m.Command
			kv.mu.Unlock()

			if (m.CommandIndex+1)%kv.maxraftstate == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []interface{}
				for j := 0; j <= m.CommandIndex; j++ {
					xlog = append(xlog, kv.applyLog[j])
				}
				e.Encode(xlog)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		}
	}
}

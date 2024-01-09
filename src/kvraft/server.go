package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

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
	Key       string
	Value     string
	Op        string
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string
	requestValid map[int64]bool
}

func (kv *KVServer) checkRequest(RequestId int64) int {
	if v, ok := kv.requestValid[RequestId]; ok {
		if v == false {
			return 0
		}
		return 1
	}
	return -1
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.checkRequest(args.RequestId) == -1 {
		oldOp := Op{
			Op:        "Get",
			Key:       args.Key,
			RequestId: args.RequestId,
		}
		kv.requestValid[args.RequestId] = false
		_, _, isLeader := kv.rf.Start(oldOp)
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
	} else if kv.checkRequest(args.RequestId) == 0 {
		reply.Err = ""
	} else {
		reply.Err = OK
		reply.Value = kv.data[args.Key]
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.checkRequest(args.RequestId) == -1 {
		oldOp := Op{
			Op:        args.Op,
			Key:       args.Key,
			RequestId: args.RequestId,
			Value:     args.Value,
		}
		kv.requestValid[args.RequestId] = false
		_, _, isLeader := kv.rf.Start(oldOp)
		if !isLeader {
			reply.Err = ErrWrongLeader
		}
	} else if kv.checkRequest(args.RequestId) == 0 {
		reply.Err = ""
	} else {
		reply.Err = OK
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
	kv.requestValid = make(map[int64]bool)
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
			op, ok := m.Command.(Op)
			if ok {
				kv.requestValid[op.RequestId] = true
				if op.Op != "GET" {
					kv.data[op.Key] += op.Value
				}
			}
			kv.mu.Unlock()

			if (m.CommandIndex+1)%kv.maxraftstate == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []interface{}
				// for j := 0; j <= m.CommandIndex; j++ {
				// 	xlog = append(xlog, kv.applyLog[j])
				// }
				e.Encode(xlog)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		}
	}
}

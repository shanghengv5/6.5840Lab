package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	RequestId int64
}

const WAIT int64 = 20

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string
	requestValid map[int64]Op

	requestCh map[int64]chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Op:        "Get",
		Key:       args.Key,
		RequestId: args.RequestId,
	}
	DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
	respTime := WAIT
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		reply.Err = ErrTimeout
		respTime = 1000
	} else {
		reply.Err = ErrWrongLeader
	}
	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		kv.mu.Lock()
		op, ok := kv.requestValid[args.RequestId]
		kv.mu.Unlock()
		if ok {
			reply.Value = op.Value
			reply.Err = OK
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
	}
	respTime := WAIT
	_, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		reply.Err = ErrTimeout
	} else {
		reply.Err = ErrWrongLeader
	}

	t := time.Now()
	for time.Since(t).Milliseconds() < respTime {
		kv.mu.Lock()
		_, ok := kv.requestValid[args.RequestId]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			return
		}
		time.Sleep(5 * time.Millisecond)
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
	kv.requestValid = make(map[int64]Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.requestCh = make(map[int64]chan bool)
	// You may need initialization code here.
	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for {
		// DPrintf(dApply, "S(%d) Start", kv.me)
		select {
		case m := <-kv.applyCh:
			DPrintf(dApply, "S(%d) CommandIndex(%d) command(%v)", kv.me, m.CommandIndex, m.Command)
			if m.SnapshotValid {
				// kv.mu.Lock()
				// err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
				// kv.mu.Unlock()
			} else if m.CommandValid {
				kv.mu.Lock()
				op, ok := m.Command.(Op)
				if !ok {
					panic("Not a op command")
				}
				_, repeatRequestOk := kv.requestValid[op.RequestId]
				// Repeat request don't calculate again
				if !repeatRequestOk {
					if op.Op == "Get" {
						op.Value = kv.data[op.Key]
					} else if op.Op == "Put" {
						kv.data[op.Key] = op.Value
					} else if op.Op == "Append" {
						kv.data[op.Key] += op.Value
					}
					kv.requestValid[op.RequestId] = op
				}
				kv.mu.Unlock()
				// if (m.CommandIndex+1)%kv.maxraftstate == 0 {
				// 	w := new(bytes.Buffer)
				// 	e := labgob.NewEncoder(w)
				// 	e.Encode(m.CommandIndex)
				// 	var xlog []interface{}
				// 	// for j := 0; j <= m.CommandIndex; j++ {
				// 	// 	xlog = append(xlog, kv.applyLog[j])
				// 	// }
				// 	e.Encode(xlog)
				// 	kv.rf.Snapshot(m.CommandIndex, w.Bytes())
				// }
			}
		case <-time.After(raft.HEARTBEAT * time.Millisecond):
			// DPrintf(dApply, "S(%d) Return", kv.me)
		}

	}

}

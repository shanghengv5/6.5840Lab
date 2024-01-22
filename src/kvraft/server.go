package kvraft

import (
	"bytes"
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
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      int64
}

const LEADER_WAIT int64 = 200
const WAIT int64 = 10

const CHECK_WAIT int64 = 0

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data         map[string]string
	requestValid map[int64]map[int64]Op

	applyLog []Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	// DPrintf(dServer, "S(%d) %s Start RequestId(%d)", kv.me, cmd.Op, args.RequestId)
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
			reply.Value = op.Value
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
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
		_, ok := kv.requestValid[args.ClientId][args.Seq]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
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
	kv.requestValid = make(map[int64]map[int64]Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyLog = []Op{Op{}}
	// You may need initialization code here.
	kv.readSnapshot(kv.rf.Persister.ReadSnapshot())
	go kv.applier()
	return kv
}

func (kv *KVServer) readSnapshot(data []byte) {
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

func (kv *KVServer) applier() {
	for {
		// DPrintf(dApply, "S(%d) Start", kv.me)
		select {
		case m := <-kv.applyCh:
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

					if op.Op == "Get" {
						op.Value = kv.data[op.Key]
					} else if op.Op == "Put" {
						kv.data[op.Key] = op.Value
					} else if op.Op == "Append" {
						kv.data[op.Key] += op.Value
					}
					DPrintf(dApply, "S(%d) %v Value(%s) repeat ClientId%d Seq%d", kv.me, op.Op, op.Value, op.ClientId, op.Seq)
					kv.requestValid[op.ClientId][op.Seq] = op
					for key, _ := range kv.requestValid[op.ClientId] {
						if key < op.Seq {
							delete(kv.requestValid[op.ClientId], key)
						}
					}
					// kv.applyLog = append(kv.applyLog, op)
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
			// case <-time.After(time.Duration(CHECK_WAIT) * time.Millisecond):
			// DPrintf(dApply, "S(%d) Return", kv.me)
		}

	}

}

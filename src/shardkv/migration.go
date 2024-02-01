package shardkv

import "time"

func (kv *ShardKV) getHeader() ClientHeader {
	kv.Seq++
	return ClientHeader{
		ClientId: kv.ClientId,
		Seq:      kv.Seq,
	}
}



func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	cmd := Op{
		Op:           "Pull",
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
			reply.Data = op.Data
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

func (kv *ShardKV) Push(args *PushArgs, reply *PushReply) {
	cmd := Op{
		Op:           "Push",
		Data:         args.Data,
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
		_, ok := kv.requestValid[args.ClientId][args.Seq]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			return
		}
		time.Sleep(time.Duration(CHECK_WAIT) * time.Millisecond)
	}
}

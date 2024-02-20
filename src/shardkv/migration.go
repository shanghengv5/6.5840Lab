package shardkv

func (kv *ShardKV) migrateRpc(server string, args *MigrateArgs) {
	srv := kv.make_end(server)
	var reply MigrateReply

	if ok := srv.Call("ShardKV."+args.Op, args, &reply); !ok || reply.Err != OK {

		return
	}
	DPrintf(dMigrate, "S(%d-%d) => (%s) (%s)  data(%v) ", kv.gid, kv.me, server, args.Op, reply.Data)
	if args.Op == "Pull" {
		kv.StartCommand(Op{
			Op:           "Sync",
			ShardData:    reply.Data,
			RequestValid: reply.RequestValid,
			Config:       args.Config,
		})
	} else if args.Op == "PullDone" {
		kv.StartCommand(Op{
			Op:       "PullDone",
			ShardIds: args.ShardIds,
			Config:   args.Config,
		})
	}

}

func (kv *ShardKV) updateRequestValid(op Op) {
	if kv.requestIsDone(op) {
		return
	}
	kv.requestValid[op.ClientId] = op.Seq
}

func writeRequestValid(src map[int64]int64, dst map[int64]int64) {
	for cId, seq := range src {
		if dst[cId] < seq {
			dst[cId] = seq
		}
	}
}

func (kv *ShardKV) Pull(args *MigrateArgs, reply *MigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	reply.RequestValid = make(map[int64]int64)
	if args.Config.Num == kv.CurConfig.Num {
		reply.Data = ShardData{}
		for _, sid := range args.ShardIds {
			shardKv := kv.shardData[sid]
			if shardKv.State == Share {
				reply.Data.UpdateData(sid, shardKv.Data)
				// kv.shardData.UpdateState(sid, Ok)
			}
		}
		writeRequestValid(kv.requestValid, reply.RequestValid)
	} else {
		reply.Err = ErrConfigChange
		DPrintf(dPull, "S(%d-%d) ConfigNum(%d)(%d)", kv.gid, kv.me, args.Config.Num, kv.CurConfig.Num)
	}
}

func (kv *ShardKV) PullDone(args *MigrateArgs, reply *MigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
	if args.Config.Num == kv.CurConfig.Num {
		for _, sid := range args.ShardIds {
			shardKv := kv.shardData[sid]
			if shardKv.State == Share {
				kv.shardData[sid] = NewKv()
			}
		}
	}

}

package shardkv

func (kv *ShardKV) migrateRpc(server string, args *MigrateArgs) {
	srv := kv.make_end(server)
	var reply MigrateReply
	if ok := srv.Call("ShardKV.Pull", args, &reply); !ok || reply.Err != OK {
		return
	}
	DPrintf(dMigrate, "S(%d-%d) => (%s) (%s) (%s) data(%v) ", kv.gid, kv.me, server, args.Op, args.Op, reply.Data)
	kv.StartCommand(Op{
		Op:           "Sync",
		ShardData:    reply.Data,
		RequestValid: reply.RequestValid,
		Config:       args.Config,
	})

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
			reply.Data.UpdateData(sid, shardKv.Data)
			if shardKv.State == Share {
				kv.shardData.UpdateState(sid, Ok)
			}
			writeRequestValid(kv.requestValid, reply.RequestValid)
		}
	} else {
		reply.Err = ErrConfigChange
		DPrintf(dPull, "S(%d-%d) ConfigNum(%d)(%d)", kv.gid, kv.me, args.Config.Num, kv.CurConfig.Num)
	}
}

package shardkv

func (kv *ShardKV) migrateRpc(server string, args *MigrateArgs) {
	srv := kv.make_end(server)
	var reply MigrateReply
	if ok := srv.Call("ShardKV."+args.Op, args, &reply); !ok || reply.Err != OK {
		return
	}
	DPrintf(dMigrate, "S(%d-%d)=>(%s) (%s) data(%v) shardIds(%v) ConfigNum(%d)", kv.gid, kv.me, server, args.Op, reply.Data, args.ShardIds, args.Config.Num)
	if args.Op == "Pull" {
		kv.StartCommand(Op{
			Op:           "FinishPull",
			ShardData:    reply.Data,
			RequestValid: reply.RequestValid,
			Config:       args.Config,
		})
	} else if args.Op == "PullDone" {
		kv.StartCommand(Op{
			Op:       "FinishPullDone",
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
	op := Op{
		Op:       "Pull",
		ShardIds: args.ShardIds,
		Config:   args.Config,
	}
	r := kv.StartCommand(op)
	reply.Err = r.Err
	reply.RequestValid = r.RequestValid
	reply.Data = r.ShardData
}

func (kv *ShardKV) PullDone(args *MigrateArgs, reply *MigrateReply) {
	op := Op{
		Op:       "PullDone",
		ShardIds: args.ShardIds,
		Config:   args.Config,
	}
	r := kv.StartCommand(op)
	reply.Err = r.Err
}

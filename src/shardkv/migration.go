package shardkv

func (kv *ShardKV) migrateRpc(server string, args *MigrateArgs) {
	srv := kv.make_end(server)
	var reply MigrateReply
	ok := srv.Call("ShardKV.Pull", args, &reply)
	DPrintf(dMigrate, "S(%d-%d) => (%s) (%s) data(%v) ok(%v) Err(%s) ", kv.gid, kv.me, server, args.Op, reply.Data, ok, reply.Err)
	if !ok || reply.Err != OK {
		return
	}

	if args.Op == "Pull" {
		kv.StartCommand(Op{
			Op:           "Sync",
			ShardData:    reply.Data,
			RequestValid: reply.RequestValid,
			OldConfig:    args.OldConfig,
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
	cmd := Op{
		Op:        "Pull",
		OldConfig: args.OldConfig,
		ShardIds:  args.ShardIds,
	}

	r := kv.StartCommand(cmd)
	reply.Data = r.ShardData
	reply.Err = r.Err
	reply.RequestValid = r.RequestValid
}

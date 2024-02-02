package shardkv

func (kv *ShardKV) getHeader() ClientHeader {
	kv.Seq++
	return ClientHeader{
		ClientId: kv.ClientId,
		Seq:      kv.Seq,
	}
}

func (kv *ShardKV) migrateRpc(server string, args *MigrateArgs) {
	srv := kv.make_end(server)
	var reply MigrateReply
	ok := srv.Call("ShardKV.Pull", args, &reply)
	if !ok {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.OldConfig.Num != kv.OldConfig.Num || reply.Err != OK {
		return
	}
	if args.Op == "Pull" {
		kv.StartCommand(Op{
			Op:           "Sync",
			ShardData:    reply.Data,
			RequestValid: reply.RequestValid,
		})
	}
	DPrintf(dMigrate, "S(%d-%d) =>(%s) %s data(%v)", kv.gid, kv.me, server, args.Op, reply.Data)
}

func (kv *ShardKV) writeRequestValid(reqValid map[int64]map[int64]Op) {
	for cId, Seqs := range reqValid {
		for seq, op := range Seqs {
			kv.requestValid[cId][seq] = op
		}
	}
}

func (kv *ShardKV) Pull(args *MigrateArgs, reply *MigrateReply) {
	cmd := Op{
		Op:           "Pull",
		OldConfig:    args.OldConfig,
		ClientHeader: args.ClientHeader,
		ShardIds:     args.ShardIds,
	}

	r := kv.StartCommand(cmd)
	reply.Data = r.ShardData
	reply.Err = r.Err
	reply.RequestValid = r.RequestValid
}

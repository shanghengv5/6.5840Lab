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
	DPrintf(dMigrate, "S(%d-%d) =>(%s) %s data(%v) ok%v Err(%s)", kv.gid, kv.me, server, args.Op, reply.Data, ok, reply.Err)
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

func (kv *ShardKV) writeRequestValid(reqValid map[int64]map[int64]StartCommandReply) {
	for cId, Seqs := range reqValid {
		for seq, reply := range Seqs {
			kv.requestValid[cId][seq] = reply
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

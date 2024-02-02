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

	if args.OldConfig.Num != kv.OldConfig.Num || reply.Err == ErrConfigChange {
		return
	}
	DPrintf(dMigrate, "%s reply%v", args.Op, reply)
	if args.Op == "Pull" {
		for shard, data := range reply.Data {
			kv.shardData.UpdateData(shard, data.Data)
			kv.shardData.UpdateState(shard, Ok)
		}
		kv.writeRequestValid(reply.RequestValid)
	}
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

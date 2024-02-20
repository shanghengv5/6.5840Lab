package shardkv

import "time"

func (kv *ShardKV) updatePullDone() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		oldCfg := kv.OldConfig
		shardData := kv.shardData
		args := MigrateArgs{
			Op:     "PullDone",
			Config: kv.CurConfig,
		}
		gid2ShardIds := shardData.getGid2ShardIds(PullDone, oldCfg)

		if len(gid2ShardIds) > 0 {
			for gid, shardIds := range gid2ShardIds {
				servers := oldCfg.Groups[gid]
				args.ShardIds = shardIds
				for _, server := range servers {
					go kv.migrateRpc(server, &args)
				}
				DPrintf(dRpc, "%v PullDone servers%v configNum%d (%v)", kv.gid, servers, args.Config.Num, gid2ShardIds)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pullData() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		oldCfg := kv.OldConfig
		shardData := kv.shardData
		args := MigrateArgs{
			Op:     "Pull",
			Config: kv.CurConfig,
		}
		gid2ShardIds := shardData.getGid2ShardIds(Pull, oldCfg)

		if len(gid2ShardIds) > 0 {
			for gid, shardIds := range gid2ShardIds {
				servers := oldCfg.Groups[gid]
				args.ShardIds = shardIds
				for _, server := range servers {
					go kv.migrateRpc(server, &args)
				}
				DPrintf(dRpc, "(%d-%d) Pull servers%v configNum%d (%v)", kv.gid, kv.me, servers, args.Config.Num, gid2ShardIds)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) refreshConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		curCfg := kv.CurConfig
		nextCfg := kv.mck.Query(kv.CurConfig.Num + 1)
		shardData := kv.shardData
		kv.mu.Unlock()
		isUpdate := true
		for _, kv := range shardData {
			if kv.State != Running {
				isUpdate = false
				break
			}
		}
		if isUpdate && nextCfg.Num == curCfg.Num+1 {
			kv.StartCommand(Op{
				Op:     "Refresh",
				Config: nextCfg,
			})
		}
		// DPrintf(dServer, "(%d)REFERSH CurConfigNum%d", kv.gid, kv.CurConfig.Num)
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		for m := range kv.applyCh {
			// DPrintf(dApply, "S(%d) CommandIndex(%d) command(%v)", kv.me, m.CommandIndex, m.Command)
			if m.SnapshotValid {
				kv.readSnapshot(m.Snapshot)
			} else if m.CommandValid {
				kv.mu.Lock()
				op, ok := m.Command.(Op)
				if !ok {
					panic("Not a op command")
				}
				reply := StartCommandReply{Err: OK, RequestValid: make(map[int64]int64)}
				if op.Type == "Outside" {
					kv.applyOutSide(op, &reply)
				} else {
					kv.applyInternal(op, &reply)
				}
				kv.sendReplyToChan(m.CommandIndex, reply)
				kv.writeSnapshot(m.CommandIndex)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) applyInternal(op Op, reply *StartCommandReply) {
	if op.Op == "Pull" {
		if op.Config.Num == kv.CurConfig.Num {
			reply.RequestValid = make(map[int64]int64)
			reply.ShardData = ShardData{}
			for _, sid := range op.ShardIds {
				shardKv := kv.shardData[sid]
				if shardKv.State == Share {
					reply.ShardData.UpdateData(sid, shardKv.Data)
				}
			}
			writeRequestValid(kv.requestValid, reply.RequestValid)
		} else {
			reply.Err = ErrConfigChange
			DPrintf(dPull, "S(%d-%d) ConfigNum(%d)(%d)", kv.gid, kv.me, op.Config.Num, kv.CurConfig.Num)
		}
	} else if op.Op == "FinishPull" {
		if op.Config.Num == kv.CurConfig.Num {
			for shard, data := range op.ShardData {
				kv.shardData.UpdateData(shard, data.Data)
				kv.shardData.UpdateState(shard, PullDone)
			}
			writeRequestValid(op.RequestValid, kv.requestValid)
		} else {
			reply.Err = ErrConfigChange
			DPrintf(dSync, "S(%d-%d) ConfigNum(%d)(%d) data%v", kv.gid, kv.me, op.Config.Num, kv.CurConfig.Num, op.ShardData)
		}
	} else if op.Op == "PullDone" {
		if op.Config.Num == kv.CurConfig.Num {
			for _, sid := range op.ShardIds {
				shardKv := kv.shardData[sid]
				if shardKv.State == Share {
					kv.shardData[sid] = NewKv()
				}
			}
		}
	} else if op.Op == "FinishPullDone" {
		if op.Config.Num == kv.CurConfig.Num {
			for _, shard := range op.ShardIds {
				kv.shardData.UpdateState(shard, Running)
			}
		}
	} else if op.Op == "Refresh" {
		if op.Config.Num == kv.CurConfig.Num+1 {
			// set ShardData state
			kv.OldConfig = kv.CurConfig
			kv.CurConfig = op.Config
			kv.updateShardDataState(kv.OldConfig, kv.CurConfig)
		}
	}
}

func (kv *ShardKV) applyOutSide(op Op, reply *StartCommandReply) {
	// init seq map
	// Repeat request don't calculate again
	shard := key2shard(op.Key)
	if !kv.checkIsRunning(shard) {
		reply.Err = ErrWrongGroup
		return
	}
	if op.Op == "Get" {
		reply.Value = kv.shardData.Get(shard, op.Key)
	} else if !kv.requestIsDone(op) {
		if op.Op == "Put" {
			kv.shardData.Put(shard, op.Key, op.Value)
		} else if op.Op == "Append" {
			kv.shardData.Append(shard, op.Key, op.Value)
		}
		kv.updateRequestValid(op)
	}
}

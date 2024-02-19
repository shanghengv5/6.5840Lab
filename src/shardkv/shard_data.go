package shardkv

import (
	"6.5840/shardctrler"
)

// shard => kvData
type ShardData map[int]Kv

type State int

const (
	Ok    State = iota
	Share       // Need share to other
	Pull        // Pull new shard
	Drop
)

func (s State) String() string {
	switch s {
	case Ok:
		return "Ok"
	case Share:
		return "Share"
	case Pull:
		return "Share"
	case Drop:
		return "Share"
	}

	return ""
}

type Kv struct {
	Data  map[string]string
	State State
}

func NewShardData() ShardData {
	shardData := ShardData{}
	for s := 0; s < shardctrler.NShards; s++ {
		shardData[s] = Kv{
			Data:  map[string]string{},
			State: Ok,
		}
	}
	return shardData
}

func (s ShardData) Get(shard int, key string) string {
	return s[shard].Data[key]
}

func (s ShardData) Put(shard int, key string, value string) {
	s[shard].Data[key] = value
}

func (s ShardData) Append(shard int, key string, value string) {
	s.Put(shard, key, s.Get(shard, key)+value)
}

func (s ShardData) UpdateData(shard int, data map[string]string) {
	kv := s[shard]
	newData := make(map[string]string)
	for k, v := range kv.Data {
		newData[k] = v
	}
	for k, v := range data {
		newData[k] = v
	}
	kv.Data = newData
	s[shard] = kv
}

func (s ShardData) UpdateState(shard int, state State) {
	kv := s[shard]
	kv.State = state
	s[shard] = kv
}

func (s ShardData) getGid2ShardIds(state State, oldCfg shardctrler.Config) (gid2ShardIds map[int][]int) {
	gid2ShardIds = make(map[int][]int)
	for shard, data := range s {
		if data.State != state {
			continue
		}
		gid := oldCfg.Shards[shard]
		gid2ShardIds[gid] = append(gid2ShardIds[gid], shard)
	}

	return
}

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

func (s ShardData) UpdateState(shard int, state State) {
	kv := s[shard]
	kv.State = state
	s[shard] = kv
}

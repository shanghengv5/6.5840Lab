package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              Err = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrTimeout          = "ErrTimeout"
	ErrConfigChange     = "ErrConfigChange"
)

type Err string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	Type  string
	ClientHeader
	OldConfig    shardctrler.Config
	NextCfg      shardctrler.Config
	RequestValid map[int64]map[int64]StartCommandReply
	ShardData    ShardData
	ShardIds     []int
	Err          Err
}

type StartCommandReply struct {
	Err
	Value        string
	ShardData    ShardData
	RequestValid map[int64]map[int64]StartCommandReply
}

type ClientHeader struct {
	ClientId int64
	Seq      int64
}

type MigrateArgs struct {
	Op string
	ClientHeader
	OldConfig shardctrler.Config
	ShardIds  []int
}

type MigrateReply struct {
	Data         ShardData
	Err          Err
	RequestValid map[int64]map[int64]StartCommandReply
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientHeader
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientHeader
}

type GetReply struct {
	Err   Err
	Value string
}

package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrTimeout      = "ErrTimeout"
	ErrConfigChange = "ErrConfigChange"
)

type Err string

type StartCommandReply struct {
	Err
	Value string
}

type ClientHeader struct {
	ClientId int64
	Seq      int64
}

type PushArgs struct {
	Op   string
	Data Kv
	ClientHeader
}

type PushReply struct {
	Err Err
}

type PullArgs struct {
	Op string
	ClientHeader
}

type PullReply struct {
	Data Kv
	Err  Err
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

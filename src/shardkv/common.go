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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ClientHeader struct {
	ClientId int64
	Seq      int64
}

type TalkArgs struct {
	Op     string
	Data   map[string]string
	Server string
	ClientHeader
}

type TalkReply struct {
	Data map[string]string
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
	Server string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientHeader
	Server string
}

type GetReply struct {
	Err   Err
	Value string
}

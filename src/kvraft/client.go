package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args, reply := GetArgs{
		Key:       key,
		RequestId: nrand(),
	}, GetReply{
		Err: "",
	}
	DPrintf(dClient, "Get Key%s RequestId%d", args.Key, args.RequestId)
	for reply.Err != OK {
		// You will have to modify this function.
		for server := range ck.servers {
			if ok := ck.servers[server].Call("KVServer.Get", &args, &reply); ok && reply.Err == OK {
				DPrintf(dRespond, "Get Key%s RequestId%d", args.Key, args.RequestId)
				return reply.Value
			}
		}
	}
	return ""
}

// func (ck *Clerk) GetRpc(server int, args *GetArgs, reply *GetReply) {
// 	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
// 	if !ok {

// 	}
// }

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args, reply := PutAppendArgs{
		Key:       key,
		Op:        op,
		Value:     value,
		RequestId: nrand(),
	}, PutAppendReply{
		Err: "",
	}
	DPrintf(dClient, "Put Key%s Value%s RequestId%d", args.Key, args.Value, args.RequestId)
	for reply.Err != OK {
		// You will have to modify this function.
		for server := range ck.servers {
			if ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply); ok && reply.Err == OK {
				DPrintf(dRespond, "Put Key%s Value%s RequestId%d", args.Key, args.Value, args.RequestId)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

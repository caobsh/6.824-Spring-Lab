package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int32
	clientID int64
	seq      int
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
	ck.clientID = nrand()
	ck.seq = 0
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seq++
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	reply := GetReply{
		Err: ErrWrongLeader,
	}

	ok := false
	for {
		leader := atomic.LoadInt32(&ck.leader)
		ok = ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if reply.Err != ErrWrongLeader && ok {
			break
		}
		leader = (leader + 1) % int32(len(ck.servers))
		atomic.StoreInt32(&ck.leader, leader)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	reply := PutAppendReply{
		Err: ErrWrongLeader,
	}

	ok := false
	for reply.Err == ErrWrongLeader || !ok {
		leader := atomic.LoadInt32(&ck.leader)
		ok = ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err != ErrWrongLeader && ok {
			break
		}
		leader = (leader + 1) % int32(len(ck.servers))
		atomic.StoreInt32(&ck.leader, leader)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

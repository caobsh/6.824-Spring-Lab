package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	data := make(map[string]string)
	dup := make(map[int64]int)
	if d.Decode(&data) != nil || d.Decode(&dup) != nil {
		DPrintf("{Node %d} fail to decode snapshot", kv.rf.Me())
	} else {
		kv.data = data
		kv.dup = dup
	}
}

func (kv *KVServer) needSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.dup)
	kv.rf.Snapshot(kv.commitIndex, w.Bytes())
}

func (kv *KVServer) getChan(index int) (chan *CommandResponse, bool) {
	indexChan, exist := kv.waitChan[index]
	if !exist {
		kv.waitChan[index] = make(chan *CommandResponse, 1)
		indexChan = kv.waitChan[index]
	}
	return indexChan, exist
}

func (kv *KVServer) helpget(command Op) (string, Err) {
	var Val string
	var Err Err
	if value, ok := kv.data[command.Key]; ok {
		Val, Err = value, OK
	} else {
		Val, Err = "", ErrNoKey
	}
	return Val, Err
}

func (kv *KVServer) applytomap(command Op) *CommandResponse {
	var Val string
	var Err Err
	switch command.Cmd {
	case "Get":
		Val, Err = kv.helpget(command)
	case "Put":
		kv.data[command.Key] = command.Value
		Err = OK
	case "Append":
		_, ok := kv.data[command.Key]
		if ok {
			kv.data[command.Key] += command.Value
		} else {
			kv.data[command.Key] = command.Value
		}
		Err = OK
	}
	return &CommandResponse{Value: Val, Err: Err}
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		DPrintf("{ Node %v } tries to apply msg %v", kv.rf.Me(), m)
		if m.CommandValid {
			op := m.Command.(Op)
			kv.mu.Lock()
			if kv.commitIndex >= m.CommandIndex {
				DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), m, kv.commitIndex)
				kv.mu.Unlock()
				continue
			}
			kv.commitIndex = m.CommandIndex
			var res *CommandResponse

			seqclientID, ok := kv.dup[op.ClientID]
			if !ok || op.Seq > seqclientID {
				kv.dup[op.ClientID] = op.Seq
				res = kv.applytomap(op)
			} else {
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), m, kv.dup[op.ClientID], op.ClientID)
				if op.Cmd == "Get" {
					val, err := kv.helpget(op)
					res = &CommandResponse{Err: err, Value: val}
				} else {
					res = &CommandResponse{Err: OK}
				}
			}

			if currentTerm, isleader := kv.rf.GetState(); isleader && currentTerm == m.CommandTerm {
				indexChan, exist := kv.waitChan[m.CommandIndex]
				if exist {
					indexChan <- res
				}
			}
			// take a snapshot
			if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
				kv.needSnapshot()
			}
			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.commitIndex = m.SnapshotIndex
				kv.readSnapshot(m.Snapshot)
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", m))
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer DPrintf("{Node %v} processes getrequest %v with getresponse %v", kv.rf.Me(), args, reply)

	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Cmd:      "Get",
		Key:      args.Key,
	}
	// submit
	index, _, isleader := kv.rf.Start(op)
	if !isleader || kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	indexChan, _ := kv.getChan(index)
	kv.mu.Unlock()

	select {
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	case commitOp := <-indexChan:
		kv.mu.Lock()
		reply.Err, reply.Value = commitOp.Err, commitOp.Value
		kv.mu.Unlock()
	}
	go func() {
		kv.mu.Lock()
		delete(kv.waitChan, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer DPrintf("{Node %v} processes updaterequest %v with updateresponse %v", kv.rf.Me(), args, reply)
	kv.mu.Lock()
	if id, ok := kv.dup[args.ClientID]; ok && id >= args.Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Cmd:      args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader || kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	indexChan, _ := kv.getChan(index)
	kv.mu.Unlock()

	select {
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	case commitOp := <-indexChan:
		kv.mu.Lock()
		reply.Err = commitOp.Err
		kv.mu.Unlock()
	}

	go func() {
		kv.mu.Lock()
		delete(kv.waitChan, index)
		kv.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	DPrintf("{Node %v} has been killed", kv.rf.Me())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// println("New KV")
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.

	kv.data = make(map[string]string)
	kv.dup = make(map[int64]int)
	kv.waitChan = make(map[int]chan *CommandResponse)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.applier()
	DPrintf("{Node %v} has started", kv.rf.Me())

	return kv
}

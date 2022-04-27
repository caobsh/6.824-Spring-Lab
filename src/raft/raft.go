package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.base { // should have =?
		return
	}
	rf.log = rf.log[index-rf.base:]
	rf.base = index
	rf.snapshot = snapshot
	if rf.lastApplied < rf.base {
		rf.lastApplied = rf.base
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.role = 0
			rf.currentTerm = reply.Term
			rf.persist()
		}
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	if rf.nextIndex[server]-rf.base > len(rf.log) {
		err_str := fmt.Sprintf("sendInstallSnapshot server %d Entries: %v Base: %v Next: %v, My: %v", rf.me, rf.log, rf.base, rf.nextIndex[server], rf.nextIndex[rf.me])
		panic(err_str)
	}
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	switch {
	case args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.base:
		return
	case args.Term > rf.currentTerm:
		rf.role = 0
		rf.currentTerm = args.Term
	case args.Term == rf.currentTerm:
		rf.role = 0
	}
	rf.lastTick = time.Now()

	// msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	// go func() { rf.applyCh <- msg }()

	if len(rf.log) > args.LastIncludedIndex-rf.base && rf.log[args.LastIncludedIndex-rf.base].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.base:]
	} else {
		rf.log = append([]LogEntry{}, LogEntry{Term: args.LastIncludedTerm})
	}
	rf.base = args.LastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	go func(snapshot []byte) {
		rf.mu.Lock()
		if rf.lastApplied >= args.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = args.LastIncludedIndex
		rf.mu.Unlock()
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex}
	}(args.Data)
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	voteReceived int
	currentTerm  int
	votedFor     int
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	role         int // 0 follower, 1 candidate, 2 leader

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	applyBool bool

	// time
	lastTick time.Time
	elapsed  int64
	beatFreq int64

	// 2D
	base     int
	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == 2
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("fail to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2D).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var base int
	var snapshot []byte
	if err := d.Decode(&base); err != nil {
		panic(err)
	} else {
		rf.base = base
		rf.snapshot = snapshot
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Id           string
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Xbase   int
	Success bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.role != 1 {
		return
	}
	if reply.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.role = 0
			rf.currentTerm = reply.Term
			rf.persist()
		}
		return
	}
	if reply.VoteGranted {
		rf.voteReceived++
		if rf.voteReceived*2 > len(rf.peers) {
			rf.role = 2
			rf.lastTick = time.Now()
			for index, _ := range rf.nextIndex {
				rf.nextIndex[index] = len(rf.log) + rf.base
				rf.matchIndex[index] = 0
			}
			go rf.heartbeats()
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 0
		rf.votedFor = -1
	}
	lastIndex := len(rf.log) - 1
	lastTerm := -1
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	switch {
	case args.Term < rf.currentTerm:
		return
	case args.Term > rf.currentTerm:
		rf.currentTerm = args.Term
		rf.persist()
		rf.role = 0
		rf.votedFor = -1
	case args.Term == rf.currentTerm:
		rf.role = 0
	}

	// when this would happen? outdated appendentries
	if rf.base > args.PrevLogIndex {
		// fmt.Println("outdated: "+args.Id, rf.me)
		reply.Xbase = rf.base
		return
	}
	rf.lastTick = time.Now()

	if len(rf.log)+rf.base <= args.PrevLogIndex {
		reply.XLen = len(rf.log) + rf.base
		return
	} else if rf.log[args.PrevLogIndex-rf.base].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex-rf.base].Term
		i := args.PrevLogIndex
		for i-rf.base >= 0 && rf.log[i-rf.base].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i + 1
		return
	}

	reply.Success = true
	// Falt here, If the follower has all the entries the leader sent, the follower MUST NOT truncate its log
	// index := args.PrevLogIndex + 1 - rf.base
	// for _, entry := range args.Entries {
	// 	if index >= len(rf.log) {
	// 		rf.log = append(rf.log, entry)
	// 	} else {
	// 		rf.log[index] = entry
	// 	}
	// 	index++
	// }
	// rf.log = append([]LogEntry(nil), rf.log[:index]...)
	i := 0
	for ; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 >= len(rf.log)+rf.base {
			break
		} else if args.Entries[i].Term == rf.log[args.PrevLogIndex+i+1-rf.base].Term {
			continue
		}
		DPrintf("server %d Entries!: %v Log: %v Prev: %d Curr: %d, Base: %d, Index: %d", rf.me, args.Entries, rf.log, args.PrevLogIndex, i, rf.base, args.PrevLogIndex+i+1-rf.base-1)
		rf.log = rf.log[:args.PrevLogIndex+i+1-rf.base] // shabi cao
		break
	}
	for j := i; j < len(args.Entries); j++ {
		rf.log = append(rf.log, args.Entries[j])
		DPrintf("server %d append: %d", rf.me, len(rf.log)+rf.base-1)
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.base+len(rf.log)-1)
		rf.applyBool = true
		rf.applyCond.Broadcast()
		// DPrintf("Follower [%d]: Commit index [%d]", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) heartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != 2 {
			rf.mu.Unlock()
			return
		}
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			if rf.base >= rf.nextIndex[index] {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.base,
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.snapshot,
				}
				go rf.sendInstallSnapshot(index, &args)
				continue
			}
			entries := []LogEntry{}
			if rf.nextIndex[index]-rf.base < len(rf.log) {
				entries = append(entries, rf.log[rf.nextIndex[index]-rf.base:]...)
			}
			prvlogindex := rf.nextIndex[index] - rf.base - 1
			if rf.nextIndex[index]-rf.base > len(rf.log) {
				err_str := fmt.Sprintf("--- server %d Entries: %v Base: %v Next: %v, My: %v", rf.me, rf.log, rf.base, rf.nextIndex[index], rf.nextIndex[rf.me])
				panic(err_str)
			}
			prvlogterm := 0
			if prvlogindex >= 0 {
				prvlogterm = rf.log[prvlogindex].Term
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: prvlogindex + rf.base,
				PrevLogTerm:  prvlogterm,
				LeaderCommit: rf.commitIndex,
				// Id:           randstring(20),
			}
			go rf.sendHeartBeat(index, &args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.beatFreq) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{
		Term:    -1,
		XLen:    -1,
		Xbase:   -1,
		Success: false,
	}
	// if len(args.Entries) > 0 {
	// 	DPrintf(args.Id)
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.role = 0
			rf.currentTerm = reply.Term
			rf.persist()
		}
		return
	}
	if !reply.Success { // change to term TODO, what if outdated?
		// rf.nextIndex[server] = args.PrevLogIndex
		// k := 0
		if reply.Xbase != -1 {
			// k = 1
		} else if reply.XLen != -1 { // log is shorter than rf.nextIndex[server]
			// k = 2
			rf.nextIndex[server] = reply.XLen
		} else if reply.XIndex-rf.base <= 0 { // no next log in leader
			// k = 3
			rf.nextIndex[server] = rf.base
		} else if rf.log[reply.XIndex-rf.base].Term != reply.XTerm && rf.log[reply.XIndex-rf.base-1].Term != reply.XTerm { // no term
			// k = 4
			rf.nextIndex[server] = reply.XIndex
		} else { // with term, point to the final index of that term
			i := reply.XIndex
			for ; i-rf.base < len(rf.log) && rf.log[i-rf.base].Term == reply.XTerm; i++ {
			}
			if reply.XIndex-rf.base < len(rf.log) {
				i--
			}
			rf.nextIndex[server] = i
			// k = 5
		}
		if rf.nextIndex[server]-rf.base > len(rf.log) {
			err_str := fmt.Sprintf(args.Id+" sendHeartBeat1: server %d Entries: %v Base: %v Next: %v, My: %v", rf.me, rf.log, rf.base, rf.nextIndex[server], rf.nextIndex[rf.me])
			panic(err_str)
		}
		return
	}
	// DPrintf(args.Id+"  [%d]: commit index [%d][%d][%d]", rf.me, rf.commitIndex, len(args.Entries), args.PrevLogIndex)
	rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	if rf.nextIndex[server]-rf.base > len(rf.log) {
		err_str := fmt.Sprintf("sendHeartBeat2: server %d Entries: %v Base: %v Next: %v, My: %v", rf.me, rf.log, rf.base, rf.nextIndex[server], rf.nextIndex[rf.me])
		panic(err_str)
	}
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)
	N := matches[(len(rf.peers)-1)/2]
	if N > rf.commitIndex && rf.log[N-rf.base].Term == rf.currentTerm {
		rf.commitIndex = N
		// DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
		rf.applyBool = true
		rf.applyCond.Broadcast()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == 2

	if isLeader {
		index = len(rf.log) + rf.base
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		DPrintf("\n<Index: %d, command: %d> start on leader %d\n", len(rf.log)-1, command, rf.me)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
	}

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		tickNow := time.Now()
		if rf.role == 2 {
			rf.lastTick = tickNow
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		elapsed := tickNow.Sub(rf.lastTick)
		if elapsed.Milliseconds() < rf.elapsed {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		rf.lastTick = tickNow
		rf.elapsed = rand.Int63n(200) + rf.beatFreq*3
		rf.mu.Unlock()
		go rf.election()
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteReceived = 1
	rf.role = 1
	rf.persist()
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.role != 1 {
			rf.mu.Unlock()
			break
		}
		lastIndex := len(rf.log) - 1
		lastTerm := -1
		if lastIndex >= 0 {
			lastTerm = rf.log[lastIndex].Term
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		reply := RequestVoteReply{}
		rf.mu.Unlock()
		go rf.sendRequestVote(index, &args, &reply)
	}
}

func (rf *Raft) sendMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("server %d admit %d. %v\n\n", rf.me, rf.lastApplied, rf.log)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.base].Command, CommandIndex: rf.lastApplied}
			rf.mu.Unlock()
			//OR will cause deadlock(In 2D, since Snapshot() need to hold rf.mu)!
			rf.applyCh <- msg
			rf.mu.Lock()
			DPrintf("admitted\n")
		} else {
			rf.applyCond.Wait()
		}
	}
	// for rf.killed() == false {
	// 	rf.mu.Lock()
	// 	for !rf.applyBool {
	// 		rf.applyCond.Wait()
	// 	}
	// 	// DPrintf("Free wait in SendMsg")
	// 	entries := rf.log
	// 	commit := rf.commitIndex
	// 	applied := rf.lastApplied + 1
	// 	rf.applyBool = false
	// 	rf.mu.Unlock()
	// 	for i := applied; i <= commit; i++ {
	// 		rf.applyCh <- ApplyMsg{CommandValid: true, Command: entries[i-rf.base].Command, CommandIndex: i}
	// 		rf.mu.Lock()
	// 		if rf.lastApplied > i {
	// 			i = rf.lastApplied
	// 		} else {
	// 			rf.lastApplied = i
	// 		}
	// 		rf.mu.Unlock()
	// 		DPrintf("server %d admit %d.\n\n", rf.me, rf.lastApplied)
	// 	}
	// }
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.votedFor = -1
	rf.lastTick = time.Now()
	rf.beatFreq = 100
	rf.elapsed = rand.Int63n(200) + rf.beatFreq*3
	rf.role = 0

	// 2D
	rf.base = 0
	rf.readSnapshot(persister.ReadSnapshot())

	// 2B
	rf.log = append([]LogEntry{}, LogEntry{Term: 0})
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1 + rf.base
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = rf.base
	rf.lastApplied = rf.base
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyBool = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendMsg()

	return rf
}

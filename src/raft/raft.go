package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
	// reply.Term = rf.currentTerm
	// if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
	// 	(len(rf.log) == 0 || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
	// 		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
	// 	rf.votedFor = args.CandidateId
	// 	rf.lastTick = time.Now()
	// 	reply.VoteGranted = true
	// 	return
	// }
	// reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	// Id           string
}

type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term    int
	XTerm   int
	XIndex  int
	XLen    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	defer rf.persist()
	// to follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 0
		rf.votedFor = -1
	}
	rf.lastTick = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true
	if len(rf.log) <= args.PrevLogIndex {
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	} else if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i >= 0 && rf.log[i].Term == rf.log[args.PrevLogIndex].Term {
			i--
		}
		reply.XIndex = i + 1
		reply.Success = false
		return
	}
	// should check whether larger than commitIndex?
	index := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if index >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[index] = entry
		}
		index++
	}
	rf.log = rf.log[:index]
	if args.LeaderCommit > rf.commitIndex {
		minV := args.LeaderCommit
		if args.LeaderCommit > index-1 {
			minV = index - 1
		}
		rf.commitIndex = minV
		rf.applyBool = true
		rf.applyCond.Broadcast()
		// DPrintf("Follower [%d]: Commit index [%d]", rf.me, rf.commitIndex)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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
				rf.nextIndex[index] = len(rf.log)
				rf.matchIndex[index] = -1
			}
			go rf.heartsbeats()
		}
	}
}

func (rf *Raft) heartsbeats() {
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
			entries := []LogEntry{}
			if rf.nextIndex[index] < len(rf.log) {
				entries = rf.log[rf.nextIndex[index]:]
			}
			prvlogindex := rf.nextIndex[index] - 1
			prvlogterm := -1
			if prvlogindex >= 0 {
				prvlogterm = rf.log[prvlogindex].Term
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: prvlogindex,
				PrevLogTerm:  prvlogterm,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendHeartBeat(index, &args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.beatFreq) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{
		Term: -1,
		XLen: -1,
	}
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
	if !reply.Success { // change to term
		// rf.nextIndex[server] = args.PrevLogIndex
		if reply.XLen != -1 {
			rf.nextIndex[server] = reply.XLen
		} else if reply.XIndex > 0 && rf.log[reply.XIndex].Term != reply.XTerm && rf.log[reply.XIndex-1].Term != reply.XTerm {
			rf.nextIndex[server] = reply.XIndex
		} else if reply.XIndex == 0 && len(rf.log) > 0 && rf.log[reply.XIndex].Term != reply.XTerm {
			rf.nextIndex[server] = reply.XIndex
		} else {
			i := reply.XIndex
			for ; i < len(rf.log) && rf.log[i].Term == reply.XTerm; i++ {
			}
			if i > 0 {
				i--
			}
			rf.nextIndex[server] = i
		}
		return
	}
	rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)
	N := matches[(len(rf.peers)-1)/2]
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
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
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.persist()
	}

	// Your code here (2B).

	return index + 1, term, isLeader
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
	for rf.killed() == false {
		rf.mu.Lock()
		for !rf.applyBool {
			rf.applyCond.Wait()
		}
		// DPrintf("Free wait in SendMsg")
		entries := rf.log
		commit := rf.commitIndex
		applied := rf.lastApplied + 1
		rf.applyBool = false
		rf.mu.Unlock()
		for i := applied; i <= commit; i++ {
			rf.applyCh <- ApplyMsg{true, entries[i].Command, i + 1}
			rf.mu.Lock()
			rf.lastApplied = i
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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

	// 2B
	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyBool = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendMsg()

	return rf
}

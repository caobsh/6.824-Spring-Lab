package raft

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		role:           0,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(randTime()),
		heartbeatTimer: time.NewTimer(stableTime()),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())

	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) changeRole(role int) {
	if rf.role == role {
		return
	}
	DPrintf("{Node %d} changes role from %d to %d in term %d", rf.me, rf.role, role, rf.currentTerm)
	rf.role = role
	switch role {
	case 0:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTime())
	case 1: // candidate
	case 2:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(stableTime())
	}
}

func (rf *Raft) getLastLog() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

func (rf *Raft) getFirstLog() *LogEntry {
	return &rf.log[0]
}

func (rf *Raft) codeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.codeState())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	rf.lastApplied, rf.commitIndex = rf.log[0].Index, rf.log[0].Index
}

// election && request vote
func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteReceived = 1
	rf.changeRole(1)
	rf.persist()

	lastLog := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(index, &args, &reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.role != 1 || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.changeRole(0)
		rf.persist()
	} else if reply.VoteGranted {
		rf.voteReceived++
		if rf.voteReceived*2 > len(rf.peers) {
			rf.changeRole(2)
			rf.HeartBeat() // should include or will have posiblity to fail
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(0)
		rf.votedFor = -1
	}
	lastLog := rf.getLastLog()
	reply.Term = rf.currentTerm
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	// should reset timeout? yes
	rf.electionTimer.Reset(randTime())
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	defer func() { reply.Term = rf.currentTerm }()
	defer rf.persist()

	switch {
	case args.Term < rf.currentTerm:
		reply.Success = false
		return
	case args.Term > rf.currentTerm:
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.changeRole(0)
	rf.electionTimer.Reset(randTime())

	// when this would happen? outdated
	if rf.getFirstLog().Index > args.PrevLogIndex {
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		reply.Xbase, reply.Success = -1, false
		return
	}

	if rf.getLastLog().Index < args.PrevLogIndex {
		reply.XTerm, reply.Success = -1, false
		reply.XIndex = rf.getLastLog().Index + 1
		return
	} else if rf.log[args.PrevLogIndex-rf.getFirstLog().Index].Term != args.PrevLogTerm {
		reply.XTerm, reply.Success = rf.log[args.PrevLogIndex-rf.getFirstLog().Index].Term, false
		i := args.PrevLogIndex - 1
		for i >= rf.getFirstLog().Index && rf.log[i-rf.getFirstLog().Index].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i + 1 // mark
		return
	}

	reply.Success = true
	i := 0
	for ; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 > rf.getLastLog().Index {
			break
		} else if args.Entries[i].Term == rf.log[args.PrevLogIndex+i+1-rf.getFirstLog().Index].Term {
			continue
		}
		rf.log = append([]LogEntry{}, rf.log[:args.PrevLogIndex+i+1-rf.getFirstLog().Index]...) // shabi cao
		break
	}
	for j := i; j < len(args.Entries); j++ {
		rf.log = append(rf.log, args.Entries[j])
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) HeartBeat() {
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		if rf.getFirstLog().Index >= rf.nextIndex[index] {
			// snapshot
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.getFirstLog().Index,
				LastIncludedTerm:  rf.getFirstLog().Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.sendInstallSnapshot(index, &args)
			continue
		} else {
			// command
			var entries []LogEntry
			if rf.nextIndex[index] <= rf.getLastLog().Index {
				entries = make([]LogEntry, len(rf.log[rf.nextIndex[index]-rf.getFirstLog().Index:]))
				copy(entries, rf.log[rf.nextIndex[index]-rf.getFirstLog().Index:])
			} else {
				entries = []LogEntry{}
			}
			prvlogindex := rf.nextIndex[index] - 1
			prvlogterm := rf.log[prvlogindex-rf.getFirstLog().Index].Term
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: prvlogindex,
				PrevLogTerm:  prvlogterm,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(index, &args, &reply)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// fmt.Println("here", reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.role != 2 {
		return
	}

	if reply.Term != rf.currentTerm || args.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm, rf.votedFor = reply.Term, -1
			rf.changeRole(0)
			rf.persist()
		}
		return
	}

	if !reply.Success { // change to term TODO, what if outdated?
		if reply.Xbase != -1 {
			rf.nextIndex[server] = reply.XIndex
			if reply.XTerm != -1 {
				firstIndex := rf.getFirstLog().Index
				for i := args.PrevLogIndex; i >= firstIndex; i-- {
					if rf.log[i-firstIndex].Term == reply.XTerm {
						rf.nextIndex[server] = i + 1
						break
					}
				}
			}
		}
	} else {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		matches := make([]int, len(rf.peers))
		copy(matches, rf.matchIndex)
		sort.Ints(matches)
		N := matches[(len(rf.peers)-1)/2]
		if N > rf.commitIndex {
			if rf.log[N-rf.getFirstLog().Index].Term == rf.currentTerm {
				DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d, commandindex : %d", rf.me, rf.commitIndex, N, rf.matchIndex, rf.currentTerm, rf.log[N-rf.getFirstLog().Index].Index)
				rf.commitIndex = N
				rf.applyCond.Broadcast()
			} else {
				DPrintf("{Node %d} can not advance commitIndex from %d to %d, because the term of newCommitIndex %d is not equal to currentTerm %d, commandindex : %d", rf.me, rf.commitIndex, N, rf.log[N-rf.getFirstLog().Index].Term, rf.currentTerm, rf.log[N-rf.getFirstLog().Index].Index)
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, args)
	if !ok || args.Term != rf.currentTerm || rf.role != 2 {
		return
	}
	if reply.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.currentTerm, rf.votedFor = reply.Term, -1
			rf.changeRole(0)
			rf.persist()
		}
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	defer func() { reply.Term = rf.currentTerm }()
	switch {
	case args.Term < rf.currentTerm:
		return
	case args.Term > rf.currentTerm:
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.changeRole(0)
	rf.electionTimer.Reset(randTime())

	// Outdated
	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex}
	}()
}

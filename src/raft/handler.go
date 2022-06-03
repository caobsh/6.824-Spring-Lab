package raft

import (
	"sync/atomic"
)

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		entries := make([]LogEntry, len(rf.log[lastIncludedIndex-rf.getFirstLog().Index:]))
		copy(entries, rf.log[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.log = entries
	}
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	data := rf.codeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)

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
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex { // should have =?
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	newlog := make([]LogEntry, len(rf.log[index-snapshotIndex:]))
	copy(newlog, rf.log[index-snapshotIndex:])
	rf.log = newlog

	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	data := rf.codeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server believes it is the leader.
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

func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
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
		index = rf.getLastLog().Index + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: term, Index: index})
		DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, rf.getLastLog(), rf.currentTerm)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.HeartBeat()
	}

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Me() int {
	return rf.me
}

package raft

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			//DPrintf("server %d admit %d. %v\n\n", rf.me, rf.lastApplied, rf.log)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.getFirstLog().Index].Command, CommandIndex: rf.lastApplied, CommandTerm: rf.currentTerm}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			DPrintf("{Node %v} applies entries %v, %v-%v in term %v", rf.me, msg.Command, rf.lastApplied, rf.commitIndex, rf.currentTerm)
		} else {
			rf.applyCond.Wait()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.electionTimer.Reset(randTime())
			go rf.election()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.role == 2 {
				rf.HeartBeat()
				rf.heartbeatTimer.Reset(stableTime())
			}
			rf.mu.Unlock()
		}
	}
}

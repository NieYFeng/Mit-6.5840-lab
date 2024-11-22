package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// 当每个 Raft 节点意识到连续的日志条目已提交时，
// 该节点应通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试器）发送一个 ApplyMsg。
// 将 CommandValid 设置为 true 以指示 ApplyMsg 包含一个新提交的日志条目。
//
// 在第 3D 部分中，你将需要在 applyCh 上发送其他类型的消息（例如，
// 快照），但对于这些其他用途，将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimeout()
}

func (rf *Raft) initLeaderState() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i)
		}
	}

	DPrintf("Raft %d: Initialized as Leader for term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(rand.Intn(200)+300) * time.Millisecond)
}

func (rf *Raft) judgeTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().After(rf.electionTimeout)
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	localLastLogIndex := len(rf.log) - 1
	localLastLogTerm := rf.log[localLastLogIndex].Term
	if lastLogTerm != localLastLogTerm {
		return lastLogTerm > localLastLogTerm
	}
	return lastLogIndex >= localLastLogIndex
}

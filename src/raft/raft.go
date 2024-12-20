package raft

// go test -run 3A
// 这是 Raft 必须向服务（或测试器）暴露的 API 的大纲。
// 参见下面每个函数的注释以获取更多详细信息。
//
// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始对一个新的日志条目达成一致。
// rf.GetState() (term, isLeader)
//   询问 Raft 当前的任期，以及它是否认为自己是领导者。
// ApplyMsg
//   每当一个新的条目被提交到日志中，每个 Raft 节点
//   都应该通过传递给 Make() 的 applyCh 向服务（或测试器）
//   发送一个 ApplyMsg。
//   在相同的服务器上，将 CommandValid 设置为 true 以指示 ApplyMsg 包含一个新提交的日志条目。

import (
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.RWMutex        // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	state             int                 // 当前节点的状态
	lastHeard         time.Time           // 上次收到心跳的时间
	electionTimeout   time.Time           // 选举超时时间
	currentTerm       int                 // 当前节点的任期号
	votedFor          int                 // 当前节点投票给了谁
	log               []LogEntry          // 日志条目
	commitIndex       int                 // 已知的已提交的最大的日志条目的索引
	lastHeartbeatSent time.Time           // 上次发送心跳的时间
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// LogEntry 表示 Raft 日志条目
type LogEntry struct {
	Term    int         // 日志条目的任期号
	Command interface{} // 代表任意类型的值
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // 是否投票给了 CandidateId
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //前一个日志的索引
	PrevLogTerm  int //前一个日志的任期号
	Entries      []LogEntry
	LeaderCommit int //控制 提交日志 的进度
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果领导者的任期小于当前任期，拒绝请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 更新心跳时间
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	rf.state = Follower
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm // 将更新了的任期传给主结点
	}

	// 更新 commitIndex（领导者提交的日志索引可能比当前高）
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("Raft %d: Rejecting vote request from %d due to lower term", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = time.Now()
	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("Raft %d: Failed to send RequestVote to %d (network issue)", rf.me, server)
	} else {
		DPrintf("Raft %d: Sent RequestVote to %d", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := len(rf.log) - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			DPrintf("Raft %d: Downgraded to follower due to higher term from %d", rf.me, server)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower, Candidate:
			if rf.judgeTimeout() {
				rf.startElection()
			}
		case Leader:
			rf.mu.Lock()
			if time.Since(rf.lastHeartbeatSent) >= 150*time.Millisecond {
				rf.lastHeartbeatSent = time.Now()
				rf.mu.Unlock()
				rf.initLeaderState()
			} else {
				rf.mu.Unlock()
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	var voteCount int32 = 1
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(i, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					} else if reply.VoteGranted && rf.currentTerm == args.Term && rf.state == Candidate {
						atomic.AddInt32(&voteCount, 1)
						if int(voteCount) > len(rf.peers)/2 && rf.state == Candidate {
							rf.state = Leader
							rf.lastHeartbeatSent = time.Now()
							rf.initLeaderState()
						}
					}
				}
			}(i)
		}
	}
}

// 服务或测试器希望创建一个 Raft 服务器。所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。
// persister 是一个保存这个服务器持久化状态的地方，并且最初持有最近保存的状态（如果有的话）。
// applyCh 是一个通道，Raft 应该通过它向服务（或测试器）发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

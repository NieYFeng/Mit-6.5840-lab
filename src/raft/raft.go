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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	state             int                 // 当前节点的状态
	lastHeard         time.Time           // 上次收到心跳的时间
	electionTimeout   time.Duration       // 选举超时时间
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的任期小于当前任期，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("Raft %d: AppendEntries rejected due to stale term from leader %d", rf.me, args.LeaderId)
		return
	}

	// 如果请求的任期大于当前任期，更新当前任期并转换为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		DPrintf("Raft %d: Updated term to %d and converted to Follower due to AppendEntries from leader %d", rf.me, args.Term, args.LeaderId)
		return
	}

	// 处理日志复制逻辑
	// 如果日志索引或任期不匹配，返回失败
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			DPrintf("Raft %d: AppendEntries rejected due to log inconsistency at index %d from leader %d", rf.me, args.PrevLogIndex, args.LeaderId)
			return
		}
	}

	// 删除与新条目冲突的现有条目
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.log = rf.log[:args.PrevLogIndex+1] // 删除冲突条目及之后的所有条目
			DPrintf("Raft %d: Log truncated at index %d due to term conflict from leader %d", rf.me, args.PrevLogIndex, args.LeaderId)
		}
	}

	// 附加新的日志条目（若存在）
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		DPrintf("Raft %d: Appended new log entries from leader %d", rf.me, args.LeaderId)
	}

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("Raft %d: Updated commitIndex to %d from leader %d", rf.me, rf.commitIndex, args.LeaderId)
	}

	// 最后设置回复
	rf.lastHeard = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("Raft %d: AppendEntries succeeded from leader %d", rf.me, args.LeaderId)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	DPrintf("Raft %d RequestVote from %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		reply.VoteGranted = false
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// 发送 RequestVote RPC 给服务器的示例代码。
// server 是 rf.peers[] 中目标服务器的索引。
// args 中包含 RPC 参数。
// 用 RPC 回复填充 *reply，因此调用者应传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与
// 处理函数中声明的参数类型相同（包括它们是否是指针）。
//
// labrpc 包模拟了一个有损网络，其中服务器
// 可能无法访问，请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果在超时间隔内收到回复，
// Call() 返回 true；否则返回 false。因此 Call() 可能不会立即返回。
// false 返回值可能是由于服务器宕机、无法访问的活跃服务器、
// 丢失的请求或丢失的回复引起的。
//
// Call() 保证返回（可能会有延迟）*除非*服务器端的处理函数不返回。
// 因此不需要在 Call() 周围实现自己的超时。
//
// 有关更多详细信息，请查看 ../labrpc/labrpc.go 中的注释。
//
// 如果你在使 RPC 工作时遇到问题，请检查你是否
// 将通过 RPC 传递的结构体中的所有字段名都大写，
// 并且调用者传递的是回复结构体的地址（&），而不是结构体本身。

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	var prevLogTerm int
	var prevLogIndex int
	if len(rf.log) > 0 {
		prevLogTerm = rf.log[len(rf.log)-1].Term
		prevLogIndex = len(rf.log) - 1
	} else {
		prevLogTerm = 0   // 没有日志时的默认 Term
		prevLogIndex = -1 // 没有日志时的默认索引
	}

	// 构造 AppendEntries 请求
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
	if ok {
		if reply.Success {
			DPrintf("Raft %d AppendEntries to %d success", rf.me, server)
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
	} else {
		DPrintf("Raft %d AppendEntries to %d failed", rf.me, server)
		rf.state = Follower
		rf.votedFor = -1
	}

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		DPrintf("Raft %d ticker,state: %v,time.Now().Sub(rf.lastHeard):%v", rf.me, rf.state, time.Now().Sub(rf.lastHeard))
		rf.mu.Lock()
		// 触发选举
		if rf.state == Follower && time.Since(rf.lastHeard) > rf.electionTimeout {
			DPrintf("Raft %d is Follower", rf.me)
			rf.state = Candidate
			rf.mu.Unlock()
		} else if rf.state == Leader {
			now := time.Now()
			DPrintf("Raft %d is Leader", rf.me)
			if now.Sub(rf.lastHeartbeatSent) >= time.Duration(100)*time.Millisecond {
				for i := range rf.peers {
					if i != rf.me {
						go rf.sendAppendEntries(i)
					}
				}
				// 记录最后一次心跳的时间
				rf.lastHeartbeatSent = now
			}
			rf.mu.Unlock()
		} else if rf.state == Candidate && time.Since(rf.lastHeard) > rf.electionTimeout {
			rf.currentTerm++
			rf.votedFor = rf.me
			LastLogIndex := -1
			LastLogTerm := -1
			if len(rf.log) > 0 {
				LastLogIndex = len(rf.log) - 1
				LastLogTerm = rf.log[LastLogIndex].Term
			}
			rf.mu.Unlock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,        // 当前节点的 ID
				LastLogIndex: LastLogIndex, // 假设的日志索引
				LastLogTerm:  LastLogTerm,  // 假设的日志任期号
			}
			cnt := 1
			// 使用 sync.WaitGroup 等待所有请求完成
			var wg sync.WaitGroup
			for i := range rf.peers {
				if i != rf.me {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						reply := RequestVoteReply{}
						rf.sendRequestVote(i, &args, &reply)
						if reply.VoteGranted {
							rf.mu.Lock()
							cnt++
							rf.mu.Unlock()
						}
					}(i)
				}
			}
			// 等待所有的 goroutine 完成
			wg.Wait()
			DPrintf("Raft %d get %d votes", rf.me, cnt)
			// 在所有请求完成后判断是否获得多数票
			if cnt > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.state = Leader
				rf.mu.Unlock()
			} else {
				rf.state = Follower
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.electionTimeout = time.Duration(rand.Intn(150)+150) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

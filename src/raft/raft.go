package raft

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	state           int                 // 当前节点的状态
	lastHeard       time.Time           // 上次收到心跳的时间
	electionTimeout time.Duration       // 选举超时时间
	currentTerm     int                 // 当前节点的任期号
	votedFor        int                 // 当前节点投票给了谁
	log             []LogEntry          // 日志条目
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
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	term        int
	voteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

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
		rf.mu.Lock()
		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.state == Follower && time.Now().Sub(rf.lastHeard) > 150*time.Millisecond {
			rf.state = Candidate
			rf.lastHeard = time.Now()
			rf.currentTerm++
			rf.votedFor = rf.me
			args := RequestVoteArgs{
				term:         rf.currentTerm,
				candidateId:  rf.me,                      // 当前节点的 ID
				lastLogIndex: len(rf.log) - 1,            // 假设的日志索引
				lastLogTerm:  rf.log[len(rf.log)-1].Term, // 假设的日志任期号
			}
			reply := RequestVoteReply{}
			for i := range rf.peers {
				if i != rf.me { // 不向自己发送请求
					go rf.sendRequestVote(i, &args, &reply)
				}
			}

		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.initialize()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) initialize() {
	// 使用 rand 生成 150 毫秒到 300 毫秒之间的随机时间
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.lastHeard = time.Now()
}

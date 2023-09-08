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
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"Raft/labgob"
	"Raft/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Leader    = 0
	Follower  = 1
	Candidate = 2
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm      int
	VotedFor         int
	Log              []LogEntry
	CommitedIndex    int
	LastAppliedIndex int
	NextIndex        []int
	MatchIndex       []int

	ApplyCh                 chan ApplyMsg
	State                   int8
	HeartBeatTimeoutPeriod  int
	ElectionTimeoutPeriod   int
	LastHeartBeatTime       time.Time
	LastElectionTimeoutTime time.Time

	// 2B
	ApplyCond      sync.Cond
	ReplicatorCond []*sync.Cond
}

type AppendEntriesArgs struct {
	LeaderTerm          int
	LeaderId            int
	PrevLogIndex        int
	PrevLogTerm         int
	Entries             []LogEntry
	LeaderCommitedIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 2B
	PrevLogIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
	return term, isleader
}

func (rf *Raft) ConvertId2State(id int8) string {
	switch id {
	case 0:
		return "Leader"
	case 1:
		return "Follower"
	case 2:
		return "Candidate"
	default:
		return "none"
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.VotedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VotedFor int
	var Log []LogEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&Log) != nil || d.Decode(&VotedFor) != nil {
		DPrintf("Node{%v} failed readPersist", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log
		rf.VotedFor = VotedFor
	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

// example RequestVote RPC handler.
// 该方法用于 Follower 处理接收自 Candidate 的投票请求
// 即 sendRequestVote 中的 rf.peers[server].Call("Raft.RequestVote", args, reply) 调用
// 通过 reply 参数返回相关信息
//
// 投票规则如下：
// 1.每个服务器在给定的任期内最多给一个Candidate投票
// 2. If a server receives a request with a stale term number, it rejects the request.(§5.1)
// 3. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
// 4. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.CurrentTerm > args.CandidateTerm || (rf.CurrentTerm == args.CandidateTerm && rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) {
		reply.CurrentTerm = rf.CurrentTerm
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if rf.CurrentTerm < args.CandidateTerm {
		rf.CurrentTerm = args.CandidateTerm
		rf.VotedFor = -1
		rf.ToFollower()

		// 2C
		rf.persist()
	}

	// 候选人的日志至少与接收者的日志一样最新，否则不授予投票
	// 根据(term, index) 来判断谁的日志更新
	if args.LastLogTerm < rf.GetLastLog().Term {
		reply.CurrentTerm = rf.CurrentTerm
		return
	}
	if args.LastLogTerm == rf.GetLastLog().Term && args.LastLogIndex < rf.GetLastLog().Index {
		reply.CurrentTerm = rf.CurrentTerm
		return
	}

	rf.VotedFor = args.CandidateId
	rf.ResetElectionTimer()
	rf.ToFollower()
	rf.persist()
	reply.CurrentTerm = rf.CurrentTerm
	reply.VoteGranted = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 对于实验2A部分，AppendEntries方法不需要实现Figure2部分的所有功能，
// 可以暂时不用考虑与日志有关的部分，即 AppendEntriesArgs 中的 Entries 字段为空
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.PrevLogIndex = args.PrevLogIndex
	DPrintf("reply.PrevLogIndex=%v\n", reply.PrevLogIndex)

	// 2A
	reply.Success = false
	// 若Leaderd的任期小于Follower，则直接拒绝这次RPC
	if args.LeaderTerm < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}

	// 若当前节点的任期小于Leader发来的任期，则说明当前节点的任期已经过期，同步Follower节点状态
	if args.LeaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.LeaderTerm
		rf.VotedFor = -1
		// 2C rf.persist()
		rf.persist()
	}
	// 一个隐式的条件，rf.CurrentTerm == args.LeaderTerm
	// 执行到这，一定要将当前身份转变为Follower
	rf.ToFollower()
	rf.ResetElectionTimer()
	// 2A

	// 2B
	if args.PrevLogIndex != 0 {
		// 日志不匹配
		// args.PrevLogIndex > rf.GetLastLog().Index为true，对应于figure 7中的(a)和(b)
		//
		// rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm 说明，在Follower的日志中，
		// 从args.PrevLogIndex或args.PrevLogIndex前面的某个位置开始往后，Follower的Log Entries与Leader中的Log Entries冲突，
		// 返回false，Leader需要回退rf.NextIndex[peer]，找到哪个冲突的位置，
		// 将从冲突的位置开始的Log Entries发送给Follower，覆盖Follower中冲突的Log Entries.
		if args.PrevLogIndex > rf.GetLastLog().Index || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = args.LeaderTerm

			// 对应于 figure 7 中的 Follower(d)
			if args.PrevLogIndex <= rf.GetLastLog().Index {
				// rf.log[args.PrevLogIndx:] = make([]LogEntry) 和 rf.Log = rf.Log[:args.PrevLogIndex] 的区别？？
				rf.Log = rf.Log[:args.PrevLogIndex]
				rf.persist()
			}
			return
		}
	} else {
		reply.Success = false
		reply.Term = args.LeaderTerm
		// DPrintf("args.PrevLogIndex = 0")
	}

	// 执行到这，说明通过了日志的一致性检查，Follower开始追加日志
	// 补充：准确来讲，通过一致性检查找到了Leader和Follower日志一致的最高索引，
	// 接下来Leader就可以进行日志覆盖了

	if !rf.CheckFollowerLog(args.PrevLogIndex, args.Entries) { //youwenti
		rf.Log = rf.Log[:args.PrevLogIndex+1]
		rf.Log = append(rf.Log, args.Entries...)
		rf.persist()
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 注意：不存在 Follower 的 CommittedIndex 大于 Leader 的 CommittedIndex 的情况。
	rf.CurrentTerm = args.LeaderTerm
	if args.LeaderCommitedIndex > rf.CommitedIndex {
		rf.CommitedIndex = int(math.Min(float64(args.LeaderCommitedIndex), float64(rf.GetLastLog().Index)))
		// 说明 Leader's.log[rf.CommitedIndex:LeaderCommittedIndex+1]中的日志需要复制到 Follower 的本地日志中
		// 因此 Leader 需要重复调用 AppendEntries RPC，以最终确定Follower的CommittedIndex与Leader的CommittedIndex一致
		// 因此需要一个后台协程来监测并重复发起这种情况下的 AppendEntries RPC，
		// 直到Follower的CommittedIndex与Leader的CommittedIndex一致，或者是将Follower中还未提交的Log Entry提交。
		rf.ApplyCond.Signal()
	}
	rf.persist()
	reply.Term = args.LeaderTerm
	reply.Success = true
}

// 采用一种相对朴素暴力的方法来检查Follower日志中prevLogIndex位置后的Log Entries和请求复制的日志logEntries之前的一致性。
// 只要任一位置出现Log Entry不一致，就返回false，然后执行Leader的强制覆盖操作。
// @TODO: 一个可以优化的方向为：返回开始不一致的Log Entry的索引下标，只从不一致的位置开始覆盖，减小复制的开销。
func (rf *Raft) CheckFollowerLog(prevLogIndex int, logEntries []LogEntry) bool {
	logs := rf.Log[prevLogIndex+1:]
	if len(logs) == 0 {
		return false
	}
	if len(logs) > len(logEntries) {
		for i := 0; i < len(logEntries); i++ {
			if logs[i].Index != logEntries[i].Index && logs[i].Term != logEntries[i].Term {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 1.Leader收到命令，追加到本地日志中。
// 2.并发地向其它的节点发送 AppendEntries RPC，等待节点响应。
// 3.Follower节点收到 AppendEntries RPC，进行日志的一致性检验；
//
//	a.若检验通过，将AppendEntries中携带的Log Entry追加到Follower本地日志中，并告知Leader我复制了你发来的Log Entry。（响应成功）
//	b.若检验失败，则携带一些信息（）告知Leader，你发来的Log Entry和我本地的Log Entry有冲突。（响应失败）
//
// 4.Leader收到大多数节点的成功响应后，更新 committed index。已提交的（committed）Log Entry就可以安全地应用于状态机了。
//
//	需要注意的是，Leader更新了committed index，但是Follower还没有更新 committedIndex。
//	因此 Leader 在更新了 committed Index 后，需要通过再次发送心跳包给 Follower节点，将committed index作为 AppendEntrie RPC 的 leaderCommitt 参数发送。
//	并且需要收到所有的 Follower 节点对该心跳包的响应，若没有收到该节点的响应，则会一致重复发送直至收到成功响应。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		// 先由Leader将Log Entry复制到本地日志
		return -1, -1, false
	}
	DPrintf("{Node %v}'s state is %s\n", rf.me, rf.ConvertId2State(rf.State))
	// 1.接收客户端发送地命令，并生成对应的 Log Entry，然后添加到 Leader 中的本地日志中
	newLogEntry := LogEntry{rf.CurrentTerm, len(rf.Log), command}
	DPrintf("{Node %v} receive a new commodidx[%v] to replicate in term %v", rf.me, newLogEntry, rf.CurrentTerm)
	rf.Log = append(rf.Log, newLogEntry)
	// 将 Log Entry 写入日志后，保存持久化状态，2C中实现
	rf.persist()

	// -----------
	//
	for i := range rf.peers {
		rf.NextIndex[i] = rf.GetLastLog().Index
	}
	// matchIndex只有在Follower正确响应了AppendEntries RPC时才进行更新。
	// 应该更新为 matchIndex = prevLogIndex +len(entries[])
	rf.MatchIndex[rf.me] = rf.GetLastLog().Index
	// -----------

	// Leader 并发的向服务器中的其它节点发送 AppendEntries RPC，要求其它节点将该Log Entry添加到它们的本地日志中。
	rf.BroadcastAppendEntries(false)
	// 一次 AppendEntries RPC 和 一次心跳包类似，需要重置心跳包发送定时器
	rf.ResetHeart()

	index = newLogEntry.Index
	term = rf.CurrentTerm
	isLeader = true
	return index, term, isLeader
}

func (rf *Raft) BroadcastAppendEntries(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			DPrintf("Node(%v) send heartbeat to the Node(%v)", rf.me, peer)
			go rf.ReplicateOneBound(peer)
		} else {
			DPrintf("Node(%v) start to replicate to the Node(%v)", rf.me, peer)
			rf.ReplicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) ReplicateLogEntriresTo(peer int) {
	rf.ReplicatorCond[peer].L.Lock()
	defer rf.ReplicatorCond[peer].L.Unlock()
	for !rf.killed() {
		// 若日志不需要进行复制，则阻塞等待，等待被唤醒。
		for !rf.CheckLogReplication(peer) {
			rf.ReplicatorCond[peer].Wait()
		}
		// 在ReplicateOneBound函数内部调用 AppendEntries RPC，进行日志复制，
		// 若RPC因网络故障等原因调用失败，则Leader中的日志没有和该Follower节点完全一致，则CheckLogReplication检测再次失效，
		// 再次发起AppendEntries RPC调用，直到ReplicatorCond检测有效，进入阻塞，再次等待被唤醒。
		rf.ReplicateOneBound(peer)
	}
}

func (rf *Raft) CheckLogReplication(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 注意理解这里的两个判断条件：
	// 1. 只有Leader才被允许发送AppendEntries RPC
	// 2. rf.MatchIndex[peer]表示，Leader已知rf.peers[perr]节点上日志的最高索引
	// 因此 rf.MatchIndex[peer] < rf.GetLastLog().Index 说明当前时刻，Leader中存在还未复制到Follower的Log Entry，因此返回true。
	return rf.State == Leader && rf.MatchIndex[peer] < rf.GetLastLog().Index
}

func (rf *Raft) ReplicateOneBound(peer int) {
	rf.mu.Lock()
	if rf.State != Leader {
		DPrintf("Node %v is not Leader\n", rf.me)
		rf.mu.Unlock()
		return
	}
	// 获取这次AppendEntries RPC中，peer节点日志中从prevLogIndex索引处开始复制Log Entries.
	prevLogIndex := rf.NextIndex[peer] - 1
	if prevLogIndex < 0 {
		DPrintf("Node(%v)'s send to peer(%v)'s prevLogIndex(%v) is 0", rf.me, peer, prevLogIndex)
		prevLogIndex = 0
	}

	// 生成AppendEntriesArgs参数
	args := AppendEntriesArgs{
		LeaderTerm:   rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Log[prevLogIndex].Term,
		// @TODO:
		// 此处，Entries = rf.Log[rf.MatchIndex[peer]+1] 应该是等价的，
		// 因为 rf.MatchIndex[peer] = rf.NextIndex[peer] - 1， 待验证
		Entries:             rf.Log[prevLogIndex+1:],
		LeaderCommitedIndex: rf.CommitedIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		rf.ProcessAppendReply(peer, args, reply)
		rf.mu.Unlock()
	} else {
		DPrintf("Node(%v) send AppendEntrie RPC fail\n", rf.me)
	}
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

func (rf *Raft) ElectionIsTimeout() bool {
	if rf.State == Leader {
		return false
	}
	return time.Now().After(rf.LastElectionTimeoutTime.Add(time.Millisecond * time.Duration(rf.ElectionTimeoutPeriod)))
}

func (rf *Raft) GetLastLog() LogEntry {
	return rf.Log[len(rf.Log)-1]
}

func (rf *Raft) ToLeader() {
	rf.State = Leader
	// Volatile state on leaders (figure 2)
	for peer := range rf.peers {
		rf.NextIndex[peer] = rf.GetLastLog().Index + 1
		if peer == rf.me {
			rf.MatchIndex[peer] = rf.NextIndex[peer] - 1
		} else {
			rf.MatchIndex[peer] = 0
		}
	}
}

func (rf *Raft) ToFollower() {
	rf.State = Follower
}

func (rf *Raft) ProcessAppendReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	reply.PrevLogIndex = int(math.Min(float64(reply.PrevLogIndex), float64(rf.NextIndex[peer])))
	// DPrintf("reply.PrevLogIndex=%v\n", reply.PrevLogIndex)

	// 需要注意的是，Leader 向 Follower 发送 AppendEntries RPC（包括心跳包）后需要接收Follower的响应。
	// 这是一个有来有回的过程，Leader 需要根据 Follower 的响应来判断 Follower 是否正确收到了Leader发送的AppendEntries RPC（包括心跳包）。
	// Leader收到Follower的响应后，首先需要判断的是，发送日志时的Leader的任期和现在Leader的任期是否一致。
	// 任期不一致说明，在发送AppendEntries RPC之后，收到Follower响应之前，Leader发生了变更,直接丢这个响应。
	if rf.State != Leader || rf.CurrentTerm != args.LeaderTerm {
		return
	}

	// rf.State == Leader && rf.CurrentTerm == args.LeaderTerm
	// 发现更大term的candidate, 转变为follwer
	if !reply.Success && reply.Term > args.LeaderTerm {
		rf.ToFollower()
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		return
	}
	// if !reply.Success && reply.Term < arg.LeaderTerm

	// 日志不一致，回退 rf.NextIndex 进行重试
	if !reply.Success && reply.Term == args.LeaderTerm {
		DPrintf("Node(%v)'s peer(%v), prevLogIndex is %v\n", rf.me, peer, rf.NextIndex[peer]-1)
		// 根据论文中第7页右下方和第8页右上方的优化技巧，Leader减小nextIndex一次性越过该冲突任期的所有日志条目。
		rf.NextIndex[peer] = rf.GetPrevTermLogEntryIndex(reply.PrevLogIndex)
		if rf.NextIndex[peer] < rf.MatchIndex[peer]+1 {
			rf.NextIndex[peer] = rf.MatchIndex[peer] + 1
		}
		return
	}

	if reply.Success {
		DPrintf("success.")
		rf.NextIndex[peer] = reply.PrevLogIndex + 1 + len(args.Entries)
		rf.MatchIndex[peer] = int(math.Max(float64(rf.NextIndex[peer]-1), float64(rf.MatchIndex[peer])))
		// rf.MatchIndex[peer] = rf.NextIndex[peer] - 1

		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		matchIndex := make([]int, 0)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i {
				matchIndex = append(matchIndex, rf.MatchIndex[i])
			}
		}
		matchIndex = append(matchIndex, rf.GetLastLog().Index)
		sort.Ints(matchIndex)
		commitIndex := matchIndex[len(matchIndex)/2]
		if commitIndex > rf.CommitedIndex && rf.Log[commitIndex].Term == rf.CurrentTerm {
			rf.CommitedIndex = commitIndex
			rf.ApplyCond.Signal()
		}
	} else {
		rf.ToFollower()
	}
}

func (rf *Raft) GetPrevTermLogEntryIndex(prevLogIndex int) int {
	prevTerm := rf.Log[prevLogIndex].Term
	for i := prevLogIndex - 1; i >= 0; i-- {
		if rf.Log[i].Term != prevTerm {
			return rf.Log[i].Index + 1
		}
	}
	return 1
}

// 开始一次选举的过程如下：
// 1. 增加 CurrentTerm，Follower 转换为 Candidate
// 2. 投票给自己
// 3. 并发地向集群中地其他服务器发送 RequestVote RPC
// 4. 重置选举定时器
func (rf *Raft) ElectionAction() {
	rf.CurrentTerm++
	rf.State = Candidate
	rf.VotedFor = rf.me
	rf.persist()
	votedCnt := 1

	args := RequestVoteArgs{
		CandidateTerm: rf.CurrentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  rf.GetLastLog().Index,
		LastLogTerm:   rf.GetLastLog().Term,
	}
	// 并发的发送 RequestVote RPC
	for i := range rf.peers {
		if i != rf.me {
			go func(idx int) {
				rf.mu.Lock()
				// 这一步的判断很重要
				// 目的是为了防止在这个 goroutine 执行前，rf.State 因某些原因导致状态发生变化.
				// 在测试的时候，可以尝试把下面这个判断条件注释掉，看看测试结果
				// 实验2A中，没有该条件判断能正常通过。在实验2B中，也正常通过。
				// 我猜测，在测试中，Candidate过期的时间粒度不够细，需要查看测试代码进行验证。
				if rf.State != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(idx, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 根据 sendRequestVote 的返回 reply 判断投票情况
					if rf.CurrentTerm == args.CandidateTerm && rf.State == Candidate {
						// 思考：上面这条判断语句是否必要？
						// 考虑这样一种情况，在当前Candidate并发地向集群中其他服务器发送RequestVote RPC的过程中，
						// Leader服务器回复正常，向该Candidate发送心跳包，心跳包中携带的Term大于rf.CurrentTerm，
						// 则该Candidate需要将状态转换为 Follower，因此该Candidate后续收到的来自其他服务器的响应就需要在处理了。
						if reply.VoteGranted {
							votedCnt++
							if votedCnt > len(rf.peers)/2 {
								// 超过半数投票给该 Candidate
								// Candidate -> Leader，并发地向其他服务器发送心跳包，维护Leader身份
								rf.ToLeader()
								// 并发发送心跳包
								rf.BroadcastAppendEntries(true)
								// 重置心跳包定时器
								rf.ResetHeart()
							}
							// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
							// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
						} else if rf.CurrentTerm < reply.CurrentTerm {
							rf.ToFollower()
							rf.CurrentTerm = reply.CurrentTerm
							rf.VotedFor = -1
							rf.persist()
						}
					}
					// else {}
				}
			}(i)
		}
	}
}

func (rf *Raft) ResetHeart() {
	rf.LastHeartBeatTime = time.Now()
}

func (rf *Raft) HeartBeatIsTimeout() bool {
	return time.Now().After(rf.LastHeartBeatTime.Add(time.Duration(rf.HeartBeatTimeoutPeriod) * time.Millisecond))
}

func (rf *Raft) Heart() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.HeartBeatIsTimeout() {
			if rf.State == Leader {
				rf.BroadcastAppendEntries(true)
				rf.ResetHeart()
			}
		}
		rf.mu.Unlock()

		t := 30 + (rand.Int63() % 5)
		time.Sleep(time.Duration(t) * time.Millisecond)
		// time.Sleep(time.Duration(rf.HeartBeatTimeoutPeriod) * time.Millisecond)
	}
}

func (rf *Raft) ResetElectionTimer() {
	rf.ElectionTimeoutPeriod = 150 + rand.Int()%150
	rf.LastElectionTimeoutTime = time.Now()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.ElectionIsTimeout() {
			// 超时时间已过，尝试选举
			rf.ElectionAction()
			rf.ResetElectionTimer()
		}
		rf.mu.Unlock()

		// 定时检测是否发选举超时
		t := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(t) * time.Millisecond)
	}
}

// 后台线程，将已提交（committed）但还未应用到状态机的命令应用到状态机中
func (rf *Raft) ApplyLogEntries() {
	for !rf.killed() {
		rf.mu.Lock()

		// rf.LastAppliedIndex < rf.CommitedIndex && rf.GetLastLog().Index+1 > rf.CommitedIndex
		//
		// for rf.LastAppliedIndex >= rf.CommitedIndex || rf.GetLastLog().Index+1 <= rf.CommitedIndex {
		// 	// DPrintf("wait to signal.")
		// 	rf.ApplyCond.Wait()
		// 	rf.CommitedIndex = int(math.Min(float64(rf.CommitedIndex), float64(rf.GetLastLog().Index)))
		// }

		// 当没有新的已提交的（committed）Log Entry 可以应用于状态机时，进入下面的循环后，
		// 会被 Wait() 阻塞，然后等待
		for rf.LastAppliedIndex >= rf.CommitedIndex {
			// DPrintf("wait to signal.")
			rf.ApplyCond.Wait()
			rf.CommitedIndex = int(math.Min(float64(rf.CommitedIndex), float64(rf.GetLastLog().Index)))
		}

		DPrintf("Node(%v)'s CommittedIndex=%v, LastAppliedIndex=%v\n", rf.me, rf.CommitedIndex, rf.LastAppliedIndex)
		entries := make([]LogEntry, rf.CommitedIndex-rf.LastAppliedIndex)
		copy(entries, rf.Log[rf.LastAppliedIndex+1:rf.CommitedIndex+1])
		committedIndex := rf.CommitedIndex
		rf.mu.Unlock()
		DPrintf("Node(%v) apply Log to state machine\n", rf.me)
		for _, entry := range entries {
			rf.ApplyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		rf.LastAppliedIndex = int(math.Max(float64(rf.LastAppliedIndex), float64(committedIndex)))
		DPrintf("LastAppliedIndex=%v\n", rf.LastAppliedIndex)
		rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.State = Follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.CommitedIndex = 0
	rf.LastAppliedIndex = 0
	rf.HeartBeatTimeoutPeriod = 50
	rf.ElectionTimeoutPeriod = 150 + rand.Int()%150
	rf.LastHeartBeatTime = time.Now()
	rf.LastElectionTimeoutTime = time.Now()
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.ApplyCh = applyCh
	rf.ReplicatorCond = make([]*sync.Cond, len(rf.peers))
	rf.ApplyCond = *sync.NewCond(&rf.mu)
	for peer := range rf.peers {
		rf.NextIndex[peer] = 1
		rf.MatchIndex[peer] = 0
		if peer != rf.me {
			rf.ReplicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// 后台 goroutine 等待信号量的唤醒，流程如下：
			// 1.Leader收到客户端发来的请求后，将请求命令先写入自己的本地日志中，然后并发的向其它节点发送 AppendEntries RPC，
			// 2.并发 AppendEntries RPC 通过信号量机制通知后台goroutine，后台goroutine被唤醒；
			// 3.后台goroutine负责执行真正的复制Log Entries操作
			go rf.ReplicateLogEntriresTo(peer)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range rf.peers {
		rf.NextIndex[peer] = rf.GetLastLog().Index + 1
	}

	// create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// DPrintf("Make()...")
	go rf.ticker()
	// DPrintf("go ticker.")
	go rf.Heart()
	go rf.ApplyLogEntries()

	return rf
}

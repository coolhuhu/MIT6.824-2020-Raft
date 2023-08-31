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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
		// rf.persist()
	}

	// 候选人的日志至少与接收者的日志一样最新，否则不授予投票
	if args.LastLogTerm < rf.GetLastLog().Term || (args.LastLogTerm == rf.GetLastLog().Term && args.LastLogIndex < rf.GetLastLog().Index) {
		reply.CurrentTerm = rf.CurrentTerm
		return
	}

	rf.VotedFor = args.CandidateId
	reply.CurrentTerm = rf.CurrentTerm
	reply.VoteGranted = true
	rf.ResetElectionTimer()
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
	reply.Success = false
	if args.LeaderTerm < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.LeaderTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.LeaderTerm
		rf.VotedFor = -1
	}
	rf.ToFollower()
	rf.ResetElectionTimer()

	// @TODO: 日志是否匹配，2B

	reply.Term = args.LeaderTerm
	reply.Success = true
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
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// rf.peers[rf.me] 向集群中的其他服务器发送心跳包
func (rf *Raft) BroadcastHeartBeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 发送心跳包给 rf.peers[peer]
		go rf.SendHeartBeatTo(peer)
	}
}

func (rf *Raft) SendHeartBeatTo(peer int) {
	rf.mu.Lock()
	if rf.State != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		LeaderTerm:          rf.CurrentTerm,
		LeaderId:            rf.me,
		PrevLogIndex:        rf.GetLastLog().Index,
		PrevLogTerm:         rf.GetLastLog().Term,
		Entries:             make([]LogEntry, 0),
		LeaderCommitedIndex: rf.CommitedIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	// 对于发送心跳包，AppendEntries 中的 Entries 字段为空
	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		rf.ProcessAppendReply(peer, &args, &reply)
		rf.mu.Unlock()
	}
	// else
}

func (rf *Raft) ProcessAppendReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.State != Leader || rf.CurrentTerm != args.LeaderTerm {
		return
	}
	// 发现更大term的candidate, 转变为follwer
	if !reply.Success && reply.Term > args.LeaderTerm {
		rf.ToFollower()
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		return
	}
	// if !reply.Success && reply.Term < arg.LeaderTerm
	if !reply.Success && reply.Term == args.LeaderTerm {
		// 日志不一致，需要减少 rf.NextIndex 进行重试，在 2A 中暂时不需要考虑
		DPrintf("日志不一致")
	}

	if reply.Success {
		// DPrintf("success.")
	} else {
		rf.ToFollower()
	}
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
								rf.BroadcastHeartBeat()
								// 重置心跳包定时器
								rf.ResetHeart()
							}
							// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
							// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
						} else if rf.CurrentTerm < reply.CurrentTerm {
							rf.ToFollower()
							rf.CurrentTerm = reply.CurrentTerm
							rf.VotedFor = -1
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
				rf.BroadcastHeartBeat()
				rf.ResetHeart()
			}
		}
		rf.mu.Unlock()

		// t := 30 + (rand.Int63() % 5)
		time.Sleep(time.Duration(rf.HeartBeatTimeoutPeriod) * time.Millisecond)
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
	for peer := range rf.peers {
		rf.NextIndex[peer] = 1
		rf.MatchIndex[peer] = 0
		//
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// for peer := range rf.peers {
	// 	rf.NextIndex[peer] = rf.GetLastLog().Index + 1
	// }

	// create a background goroutine that will kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// DPrintf("Make()...")
	go rf.ticker()
	// DPrintf("go ticker.")
	go rf.Heart()

	return rf
}

package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastHeartbeat    time.Time     // 上一次心跳时间
	electionTimeouts time.Duration // 选举超时
	isLeader         bool
	isCandidate      bool
	currentTerm      int
	voteFor          int // -1表示null
	logs             []LogEntry
	commitIndex      int
	lastApplied      int // 最后应用到状态机的log index. 与commitIndex之前的log为已提交但未应用到状态机的log
	nextIndex        []int
	matchIndex       []int
	cond             sync.Cond
}

// log entry
// Command is client command
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
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
	isleader = rf.isLeader

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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if candidate's term < currentTerm, 则表示candidate的vote请求已经不属于当前term，应当无视
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if candidate's term > currentTerm, 变为follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 需要日志新旧比较判断
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		// 当给candidate投出选票后，重置选举超时计时器，给candidate足够时间完成选举
		rf.lastHeartbeat = time.Now()
		rf.electionTimeouts = randomizedTimeouts()
		DPrintf("%v 投票 %v\n", rf.me, args.CandidateId)
	} else {
		// DPrintf("%v 拒绝投票 %v; %v's currentTerm: %v, voteFor: %v; %v's currentTerm: %v\n", rf.me, args.CandidateId, rf.me, rf.currentTerm, rf.voteFor, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// if leader's term > currentTerm, 变为/保持follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.becomeFollower(args.Term)

	// heartbeat
	if args.Entries == nil {
		rf.lastHeartbeat = time.Now()
		rf.electionTimeouts = randomizedTimeouts()
		reply.Term = rf.currentTerm
		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
			rf.cond.Signal()
		}
		return
	}

	// 3B
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.lastHeartbeat = time.Now()
		rf.electionTimeouts = randomizedTimeouts()
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果rf.logs长度比leader还长，舍弃多余部分
	if len(rf.logs) > len(args.Entries)+args.PrevLogIndex+1 {
		diff := len(rf.logs) - len(args.Entries) - args.PrevLogIndex - 1
		rf.logs = rf.logs[:len(rf.logs)-diff]
	}
	for i := 0; i < len(args.Entries); i++ {
		logIndex := i + args.PrevLogIndex + 1
		if logIndex < len(rf.logs) {
			rf.logs[logIndex] = args.Entries[i]
		} else {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		rf.cond.Signal()
	}
	rf.lastHeartbeat = time.Now()
	rf.electionTimeouts = randomizedTimeouts()
	reply.Term = rf.currentTerm
	reply.Success = true
	DPrintf("%v replicates entries from %v in term %v, commitIndex:%v", rf.me, args.LeaderId, rf.currentTerm, rf.commitIndex)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.isLeader
	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{command, index, term})
	DPrintf("%v receives a new command to replicate in term %v", rf.me, rf.currentTerm)

	// leader添加新日志后，发送AppendEntries RPC同步日志状态
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 对每个server启动一个replicator线程进行日志复制
		go rf.replicator(i)
	}
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.isLeader {
			rf.heartbeatBroadcast()
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			if time.Since(rf.lastHeartbeat) > rf.electionTimeouts {
				rf.becomeCandidate()
				DPrintf("%v 检测到选举超时(%v), 启动选举. currentTerm:%v\n", rf.me, rf.electionTimeouts, rf.currentTerm)

				// 重置选举超时，同时重新随机化选举超时时间
				rf.lastHeartbeat = time.Now()
				rf.electionTimeouts = randomizedTimeouts()

				// RPC的发送和接收在单独goroutine中进行(发送还需要创建多个goroutine)
				go rf.startElection()
			}

			rf.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lastHeartbeat = time.Now()
	rf.electionTimeouts = randomizedTimeouts()
	// DPrintf("%v 选举超时(%v)\n", rf.me, rf.electionTimeouts)
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.isLeader = false
	rf.logs = []LogEntry{{Index: 0, Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cond = *sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	// 默认全部初始化为0
	rf.matchIndex = make([]int, len(rf.peers))

	// start ticker goroutine to start elections
	go rf.ticker()
	// applier goroutine
	go rf.applier(applyCh)

	return rf
}

// randomized election timeouts
func randomizedTimeouts() time.Duration {
	// 1000 - 2000 ms
	ms := 1000 + (rand.Int63() % 1000)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startElection() {
	// 有两种值得学习的实现方式：
	// (1) 通过channel在此goroutine中接收各个发送rpc goroutine的消息，但是更复杂，需要控制channel close，需要再加入新的goroutine
	// (2) 在各个rpc goroutine中通过闭包修改voteCounter,判断是否超过半数,即直接在各个rpc发送协程中进行
	// 第一种使用了WaitGroup，但是问题在于，如果某个server一直故障不回复，则会导致一直不统计票数，选举时长变长，会出现经常长于选举超时的情况，导致一直不能正常完成选举
	// 第二种选择更好，可以在计数过半时直接成为leader，无需等待出现故障的server

	// var wg sync.WaitGroup

	// 自己先投一票
	voteCounter := 1

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// wg.Add(1)
		reply := RequestVoteReply{}

		// DPrintf("%v 开始发送request rpc to %v\n", rf.me, i)
		go func(i int) {
			// 这里出现过"分布式死锁".切记,锁要只需要锁住共享资源,范围过大的锁容易出现死锁等问题
			// rf.mu.Lock()
			// defer rf.mu.Unlock()

			// defer wg.Done()

			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					voteCounter++
				}

				// 第一个判断条件很重要,leader需要是当前任期的leader
				// 如果选举时长过长,已经不在当前term,为之前term选出的leader,不能作为当前的leader
				// 后面为了结构更清晰，加上了isCandidate来判断是否为candidate状态.
				// if rf.currentTerm == args.Term && !rf.isLeader && voteCounter > len(rf.peers)/2 {
				if rf.isCandidate && !rf.isLeader && voteCounter > len(rf.peers)/2 {
					rf.becomeLeader()
					// 成为leader立即进行一次heartbeat broadcast
					rf.heartbeatBroadcast()
					DPrintf("%v becomes leader\n", rf.me)
				}
			}
		}(i)
	}

	// wg.Wait()
	// DPrintf("%v's voteCounter: %v\n", rf.me, voteCounter)
}

func (rf *Raft) heartbeatBroadcast() {
	// 之前这里加了锁，但是这个广播函数不是在独立goroutine中执行的，不需要加锁
	// 加锁会导致死锁
	// 记住一点：只有在多个线程/goroutinue中运行的函数才要加锁，不然容易死锁
	// rf.mu.Lock()
	// args := AppendEntriesArgs{
	// 	Term:         rf.currentTerm,
	// 	LeaderId:     rf.me,
	// }
	// rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := AppendEntriesReply{}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		go func(i int) {
			// 如果在之前的rpc send线程中已经发现term大小问题，则无需继续heartbeat
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if rf.sendAppendEntries(i, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 若Success为false，在heartbeat情况下只能是term大小问题，则update currentTerm，且变回follower
				if !reply.Success {
					rf.becomeFollower(reply.Term)
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.isLeader = false
	rf.isCandidate = false
	rf.voteFor = -1
}

func (rf *Raft) becomeLeader() {
	rf.isLeader = true
	rf.isCandidate = false
	rf.voteFor = -1
	rf.reinitializeLeaderVolatileState()
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.isCandidate = true
	rf.voteFor = rf.me
}

func (rf *Raft) reinitializeLeaderVolatileState() {
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
}

// 只有leader设置日志为已提交，follower之后跟随提交
func (rf *Raft) advanceCommitIndex() {
	flag := false
	if !rf.isLeader {
		return
	}
	for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
		if rf.logs[N].Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = N
			flag = true
		} else {
			// 目前日志不满足，后面日志更不可能满足多数条件
			break
		}
	}

	if flag {
		// 唤醒applier线程
		rf.cond.Signal()
	}
}

func (rf *Raft) replicator(server int) {
	for {
		// 若之前的rpc send线程中已变回follower，则其他线程无需继续发送rpc
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}

		// 由于可能进行多次nextIndex的递减，每次send rpc重新定义args
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
			Entries:      rf.logs[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}

		if rf.sendAppendEntries(server, &args, &reply) {
			rf.mu.Lock()
			DPrintf("%v sent a append entries rpc to %v in term %v", rf.me, server, rf.currentTerm)

			if reply.Success {
				// send rpc期间，系统状态是有可能变化的，此期间也没有加锁保护rf中共享变量，所以rf.logs等状态是可能变化的
				// 这里应该用 matchIndx = prevLogIndex + len(entries[])
				// rf.nextIndex[server] = len(rf.logs)
				// rf.matchIndex[server] = len(rf.logs) - 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				// 统计查看是否可以更新commitIndex
				rf.advanceCommitIndex()
				DPrintf("%v's commitIndex:%v", rf.me, rf.commitIndex)
				rf.mu.Unlock()
				return
			} else if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			} else {
				rf.nextIndex[server]--
			}

			rf.mu.Unlock()
		} else {
			return
		}
	}
}

// 将日志应用到状态机的线程函数
func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			applyCh <- msg
			rf.lastApplied = i
		}
		rf.mu.Unlock()
	}
}

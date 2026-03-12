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
	currentTerm      int
	voteFor          int // -1表示null
	logs             []LogEntry
	commitIndex      int
	lastApplied      int // 最后应用到状态机的log index. 与commitIndex之前的log为已提交但未应用到状态机的log
	nextIndex        []int
	matchIndex       []int
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

	// TODO: 3B lab 需要加入日志新旧比较判断
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		// 当给candidate投出选票后，重置选举超时计时器，给candidate足够时间完成选举
		rf.lastHeartbeat = time.Now()
		rf.electionTimeouts = randomizedTimeouts()
		DPrintf("%v 投票 %v\n", rf.me, args.CandidateId)
	} else {
		// DPrintf("%v 拒绝投票 %v; %v's currentTerm: %v, voteFor: %v; %v's currentTerm: %v\n", rf.me, args.CandidateId, rf.me, rf.currentTerm, rf.voteFor, args.CandidateId, args.Term)
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
		return
	}
	// TODO: 3B

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
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() {

		}()
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
				rf.currentTerm++
				rf.voteFor = rf.me
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	// 默认全部初始化为0
	rf.matchIndex = make([]int, len(peers))

	// start ticker goroutine to start elections
	go rf.ticker()

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
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// TODO:3B

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
				if rf.currentTerm == args.Term && !rf.isLeader && voteCounter > len(rf.peers)/2 {
					rf.isLeader = true
					rf.voteFor = -1
					// 成为leader立即进行一次heartbeat broadcast
					rf.heartbeatBroadcast()
					DPrintf("%v become leader\n", rf.me)
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
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	// rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := AppendEntriesReply{}
		go func(i int) {
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
	rf.voteFor = -1
}

func (rf *Raft) initializeLeaderVolatileState() {
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	// 默认全部初始化为0
	rf.matchIndex = make([]int, len(rf.peers))
}

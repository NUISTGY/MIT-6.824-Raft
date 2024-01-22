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
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

// ApplyMsg
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
	// true为log，false为snapshot
	CommandValid bool
	// 向application层提交日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// 向application层安装快照
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

// LogEntry
// each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Command interface{}
	Term    int
}

// ROLE_当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

// Raft
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

	// Persistent state on all servers
	currentTerm       int        // 见过的最大任期 (initialized to 0 on first boot, increases monotonically)
	votedFor          int        // 记录在currentTerm任期投票给谁了 (or null if none)
	log               []LogEntry // 日志记录集 (first index is 1)
	lastIncludedIndex int        // snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm  int        // snapthost最后1个logEntry的term，没有snaphost则无意义

	// Volatile state on all servers
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引 (initialized to 0, increases monotonically)

	// Volatile state on leaders（成为leader时重置）
	nextIndex  []int //	for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// others on all servers
	role           string    // 角色
	leaderId       int       // leader的id
	lastActiveTime time.Time // 最近活跃时间（刷新时机：收到leader AppendEntries心跳、给其他candidates投票、请求其他节点投票）
	heartBeatTime  time.Time // 最近一次心跳时间，作为leader，每隔固定毫秒向手下发送心跳包的时间

	applyCh chan ApplyMsg // 应用层的提交队列
}

// GetState return currentTerm and whether this server
// believes it is the leader.
// 由单元测试程序调用
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == ROLE_LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

// raftStateForPersist 将Persistent state on all servers中涉及的5个数据编码，为了persister持久化
func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// 为了snaphost多做的2个持久化metadata
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// For Lab-2A:
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
// term			leader's term
// leaderId 	so follower can redirect clients index of log entry immediately preceding new ones
// prevLogindex	index of log entry immediately preceding new ones
// prevLogTerm	term of prevLogIndex entry
// entries		log entries to store (empty for heartbeat; may send more than one for efficiency)
// leaderCommit	leader's commitIndex

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply
// term 		currentTerm, for leader to update itself
// success		true if follower contained entry matching prevLogIndex and prevLogTerm
type AppendEntriesReply struct {
	Term    int
	Success bool
	// 新增2个字段，用于leader和follower协调同步位置，优化回退性能
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// RequestVote
// 用于各节点处理接受到的RequestVoteRPC请求并返回RequestVoteReply
// Receiver implementation:
// 1. Reply false if term < current Term (§5.1)
// 2. If votedFor is null or candidateld, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ", rf.me, args.CandidateId, reply.VoteGranted)
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 任期不如自身大，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.leaderId = -1
		// 继续向下走，进行投票
	}

	// 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的日志必须比我的新
		// 1, 最后一条log，任期大的更新
		// 2，更长的log则更新
		lastLogTerm := rf.lastTerm()
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，才会重置自己的下次投票时间
		}
	}

	rf.persist()
}

// AppendEntries
// 用于各节点处理接受到的AppendEntries RPC请求并返回AppendEntriesReply
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log))
	}()

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 更新当前新的leader
	rf.leaderId = args.LeaderId
	// 刷新最近一次活跃时间
	rf.lastActiveTime = time.Now()

	// 如果prevLogIndex在快照内，且不是快照最后一个log，那么只能从index=1开始同步了
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex { // prevLogIndex正好等于快照的最后一个log
		if args.PrevLogTerm != rf.lastIncludedTerm { // 冲突了，那么从index=1开始同步吧
			reply.ConflictIndex = 1
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	} else { // prevLogIndex在快照之后，那么进一步判定
		if args.PrevLogIndex > rf.lastIndex() { // prevLogIndex位置没有日志的case
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// prevLogIndex位置有日志，那么判断term必须相同，否则false
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新提交下标
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

// InstallSnapshot 安装快照RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// leader快照不如本地长，那么忽略这个快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else { // leader快照比本地快照长
		if args.LastIncludedIndex < rf.lastIndex() { // 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0) // term冲突，扔掉快照外的所有日志
			} else { // term没冲突，保留后续日志
				leftLog := make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else {
			rf.log = make([]LogEntry, 0) // 快照比本地日志长，日志就清空了
		}
	}
	// 更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)
	// snapshot提交给应用层
	rf.installSnapshotToApplication()
	DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
}

// Send RPC to a server.
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
// handler function on the server side does not return.  Thus, there
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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//只能往leader上附加日志，其他角色不接受该操作
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}
	log := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.log = append(rf.log, log)
	index = rf.lastIndex()
	term = rf.currentTerm
	rf.persist()

	DPrintf("Leader-Node[%d] add new Command[%d], logIndex[%d] currentTerm[%d]", rf.me, log.Command, index, term)
	return index, term, isLeader
}

// Kill
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

// election逻辑
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化
			elapses := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = now // 重置下次选举时间
				rf.currentTerm += 1     // 发起新任期
				rf.votedFor = rf.me     // 该任期投了自己
				rf.persist()

				// 请求投票req
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
				}
				args.LastLogTerm = rf.lastTerm()

				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)

				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteCount := 1   // 收到投票个数（先给自己投1票）
				finishCount := 1 // 收到应答个数
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 收到全部应答 ｜｜ 收到过半数的票数 -> 跳转到 VOTE_END
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// RPC完成后lock住做state的再次检查，确保还是之前的状态，如果角色改变了，则忽略本轮投票结果
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					rf.leaderId = rf.me

					// for lab-2b. 新选举出来的leader要刷新nextIndex和matchIndex
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

					rf.heartBeatTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

// leader执行的逻辑
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.heartBeatTime) < 100*time.Millisecond {
				return
			}
			rf.heartBeatTime = time.Now()

			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}

				// 如果nextIndex在leader的snapshot内，那么直接同步snapshot
				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					rf.doInstallSnapshot(peerId)
				} else { // 否则同步日志
					rf.doAppendEntries(peerId)
				}
			}
		}()
	}
}

func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]LogEntry, 0)
	args.PrevLogIndex = rf.nextIndex[peerId] - 1

	// 如果prevLogIndex是leader快照的最后1条log, 那么取快照的最后1个term
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else { // 否则一定是log部分
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)

	DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

	go func() {
		// DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args1.Term, id)
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				rf.updateCommitIndex() // 更新commitIndex
			} else {
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex // 用follower首次出现term的index作为同步开始
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					// 这时候我们将返回的conflictIndex设置为nextIndex即可
					rf.nextIndex[peerId] = reply.ConflictIndex
				}
				DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
		}
	}()
}

func (rf *Raft) doInstallSnapshot(peerId int) {
	DPrintf("RaftNode[%d] doInstallSnapshot starts, leaderId[%d] peerId[%d]\n", rf.me, rf.leaderId, peerId)

	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Offset = 0
	args.Data = rf.persister.ReadSnapshot()
	args.Done = true

	reply := InstallSnapshotReply{}

	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1      // 重新从末尾同步log（未经优化，但够用）
			rf.matchIndex[peerId] = args.LastIncludedIndex // 已同步到的位置（未经优化，但够用）
			rf.updateCommitIndex()                         // 更新commitIndex
			DPrintf("RaftNode[%d] doInstallSnapshot ends, leaderId[%d] peerId[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]\n", rf.me, rf.leaderId, peerId, rf.nextIndex[peerId],
				rf.matchIndex[peerId], rf.commitIndex)
		}
	}()
}

// applyLogLoop 定期检查是否有新的日志需要提交给应用层
// 它会一直运行直到这个 Raft 节点被 kill
func (rf *Raft) applyLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				noMore = false
			}
		}()
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = ROLE_FOLLOWER
	rf.votedFor = -1
	rf.leaderId = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 向application层安装快照
	rf.installSnapshotToApplication()

	// election逻辑
	go rf.electionLoop()
	// leader逻辑
	go rf.appendEntriesLoop()
	// apply逻辑
	go rf.applyLogLoop()

	DPrintf("Raftnode[%d]启动", me)

	return rf
}

// TakeSnapshot 保存snapshot，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 要压缩的日志长度
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex

	// 更新快照元信息
	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	// 压缩日志
	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)

	DPrintf("RafeNode[%d] TakeSnapshot ends, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) installSnapshotToApplication() {
	var applyMsg *ApplyMsg

	// 同步给application层的快照
	applyMsg = &ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	// 快照部分就已经提交给application了，所以后续applyLoop提交日志后移
	rf.lastApplied = rf.lastIncludedIndex

	DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- *applyMsg
	return
}

// CheckLogSize 判断日志是否需要压缩，RaftStateSize返回len(ps.raftstate)
func (rf *Raft) CheckLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

func (rf *Raft) updateCommitIndex() {
	// 更新commitIndex, 就是找中位数
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// 如果index属于snapshot范围，那么不要检查term了，因为snapshot的一定是集群提交的
	// 否则还是检查log的term是否满足条件
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
}

// 最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastIncludedTerm // for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

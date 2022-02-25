// https://www.jianshu.com/p/4147cc957400?utm_campaign=maleskine&utm_content=note&utm_medium=seo_notes&utm_source=recommendation
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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	Follower State = iota // value --> 0
	Candidate			  // value --> 1
	Leader			 	  // value --> 2
)

const NULL int = -1

type Log struct {
	Term  	int				//从领导人处收到的任期号
	Command interface{}		//状态机需要执行的命令
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// Persistent state on all servers:
	currentTerm int			//服务器最后知道的任期号，从0开始递增
	votedFor 	int			//在当前任期内收到选票的候选人，没有的话为null
	log 		[]Log		//日志条目

	// Volatile state on all servers:
	commitIndex int			//已知的被提交的最大日志条目的索引值，0开始递增
	lastApplied int			//被状态机执行的最大日志条目的索引值，0开始递增

	// Volatile state on leaders:
	nextIndex	[]int		//对于每一台服务器，记录需要发给它的下一个日志条目索引
	matchIndex	[]int		//对于每一台服务器，记录已经复制到该服务器的日志的最高索引值，从0开始递增

	// channel
	applyCh 	chan ApplyMsg
	// handle rpc
	voteCh		chan bool
	appendLogCh	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int 		//候选人的任期号
	CandidateId 	int			//请求投票的候选人id
	LastLogIndex	int			//候选人最新日志条目的索引值
	LastLogTerm		int			//候选人最新日志条目对应的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int			//目前的任期号，用于候选人更新自己
	VotedGranted	bool		//如果候选人收到选票，则为true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm { // leader, candidate will be follower
		rf.beFollower(args.Term)
		send(rf.voteCh)
	}
	success := false
	if args.Term < rf.currentTerm {

	}else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {
		
	}else if args.LastLogTerm < rf.getLastLogTerm() {
		// candidate log is not new
	}else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIdx() {
		// candidate log is not new
	}else {
		//vote for him
		rf.votedFor = args.CandidateId
		success = true
		rf.state = Follower
		send(rf.voteCh)
	}
	
	reply.Term = rf.currentTerm
	reply.VotedGranted = success
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntriesArgs struct {
	Term			int 		//领导人的任期号
	LeaderId 		int			//领导人的id，为了其他服务器能重定向到客户端
	PrevLogIndex	int			//最新日志之前的日志的索引值
	PrevLogTerm		int			//最新日志之前的日志的领导人任期号
	Entries 		[]Log		//将要存储的日志条目（heartbeats时为空）
	LeaderCommit	int			//领导人提交的日志条目索引值
}

type AppendEntriesReply struct {
	Term 			int			//当前的任期号，用于领导人更新自己的任期号
	Success			bool		//如果其他服务器包含能够匹配上 prevLogTerm 和 prevLogIndex 时为真
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// because goroutine only send the chan to below goroutine, to avoid block, need 1 buffer
	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// because from the hint the test requires that the leader send heartbeats RPCs no more than ten times per seconds
	heartbeatTime := time.Duration(100) * time.Millisecond
	
	//from hint: You'll need to write code that takes actions periodically or after delays in time.
	//The easiest way to do this is to create a goroutine with a loop that calls time.Sleep()
	go func() {
		for  {
			electionTime := time.Duration(rand.Intn(100) + 300) * time.Millisecond
			switch rf.state {
			case Follower, Candidate:
				select {
				case <-rf.voteCh:
				case <-rf.appendLogCh:
				case <-time.After(electionTime):
					rf.beCandidate() //switch to candidate, then start election
				}
			case Leader:
				rf.startAppendLog()
				time.Sleep(heartbeatTime)
			}
		}
	}()

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) beCandidate()  {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me //vote myself first
	// ask for other's vote
	go rf.startElection()
}

func (rf *Raft) startElection()  {
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIdx(),
		rf.getLastLogTerm(),
	}
	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			// 1. send req
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, &args, reply)
			if ret {
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					send(rf.voteCh)
					return
				}
				if rf.state != Candidate {
					return
				}
				if reply.VotedGranted {
					atomic.AddInt32(&votes, 1)
				}
				if atomic.LoadInt32(&votes) > int32(len(rf.peers) / 2) {
					rf.beLeader() // update status
					send(rf.voteCh)
				}
			}
		}(i)
	}
}

func send(ch chan bool)  {
	select {
	case <-ch: // if already set, consume it then re-sent to avoid block
	default:
	}
	ch <- true
}

func (rf *Raft) getLastLogIdx() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < 0 {
		return -1
	}
	return rf.log[prevLogIdx].Term
}

func (rf *Raft) beFollower(term int)  {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
}

func (rf *Raft) beLeader()  {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	// initialize leader data
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIdx() + 1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { // now only for heartbeat
	if args.Term > rf.currentTerm { // leader, candidate will be follower
		rf.beFollower(args.Term)
		send(rf.appendLogCh)
	}
	success := false
	PrevLogIndexTerm := -1
	if args.PrevLogIndex >= 0 {
		PrevLogIndexTerm = rf.log[args.PrevLogIndex].Term
	}
	if args.Term < rf.currentTerm {
		//ret false
	} else if PrevLogIndexTerm != args.PrevLogTerm {
		//ret false
	}
	reply.Term = rf.currentTerm
	reply.Success = success
}

// from leader action
func (rf *Raft) startAppendLog()  {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			if rf.state != Leader {
				return
			}
			args := AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				rf.getPrevLogIdx(idx),
				rf.getPrevLogTerm(idx),
				make([]Log, 0), // will implement real send log
				rf.commitIndex,
			}
			reply := &AppendEntriesReply{}

			ret := rf.sendAppendEntries(idx, &args, reply)
			if !ret {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.beFollower(reply.Term)
				return
			}
		}(i)
	}
}
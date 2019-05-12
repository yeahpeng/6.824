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
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	SLEEP_INTERVAL        = 10
	HEARTBEAT_INTERVAL    = 100 //should less  than MIN_ELECTION_INTERVAL
	MIN_ELECTION_INTERVAL = 300
	MAX_ELECTION_INTERVAL = 600
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int //0, 1 or 2(follower, candidate or leader)

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int //for send log
	matchIndex []int // for update  commit

	electTimer *time.Timer //contorl elect timeout

	requestVote chan struct{}
	appendEntry chan struct{} //only for leader broadcast
	totalVoted  int
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntryReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	go func() { rf.requestVote <- struct{}{} }()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.print("voteBefore ")
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor >= 0) || (args.LastLogTerm < rf.getLastLogEntry().Term || args.LastLogTerm == rf.getLastLogEntry().Term && args.LastLogIndex < rf.getLastLogEntry().Index) {
		if args.Term > rf.currentTerm {
			rf.changeTerm(args.Term)
			rf.convertState(FOLLOWER)
		}
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.changeTerm(args.Term)
		rf.convertState(FOLLOWER)
		rf.votedFor = args.CandidateId
		rf.print("voteTrue")
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {

	go func() { rf.appendEntry <- struct{}{} }()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		rf.changeTerm(args.Term)
		if args.LeaderId != rf.me {
			rf.convertState(FOLLOWER)
		}

		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Index == args.PrevLogIndex && rf.log[i].Term == args.PrevLogTerm {
				rf.log = append(rf.log[:i+1], args.Entries...)
				reply.Success = true
				if args.LeaderCommit < len(rf.log) - 1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.log) - 1
				}
				rf.print("AppendEntryTrue")
				return
			}
		}
		reply.Success = false
	}
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
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
	rf.print("sendRequestVote")
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	rf.print("sendEntriesAfter")
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log)
	if isLeader {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, len(rf.log), command})
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}
	rf.print("Start")

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) resetTimer() {
	// rf.print("reset")
	rf.electTimer.Reset(getElectTimeout())
}

func (rf *Raft) print(msg string) {

	//timestamp := time.Now().UnixNano() / 1e6
	//fmt.Println(timestamp, msg, "state:", rf.state, "id:", rf.me, "term:", rf.currentTerm,  "voteFor:", rf.votedFor, "totalVoted:", rf.totalVoted,
	//	"log:", rf.log, "commitId", rf.commitIndex, "nextIndex", rf.nextIndex)
}

func (rf *Raft) convertState(state int) {
	rf.state = state
	rf.votedFor = -1
	rf.totalVoted = 0
}

func (rf *Raft) changeTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.totalVoted = 0
}

func (rf *Raft) process() {
	for {
		rf.print("process")
		switch rf.getStateAtomic() {
		case FOLLOWER:
			select {
			case <-rf.electTimer.C:
				rf.mu.Lock()
				rf.convertState(CANDIDATE)
				rf.resetTimer()
				rf.mu.Unlock()
			case <-rf.appendEntry:
				rf.resetTimer()
			case <-rf.requestVote:
				rf.resetTimer()
			}
		case CANDIDATE:
			rf.mu.Lock()
			if rf.votedFor < 0 {
				rf.participateElection()
			}
			rf.mu.Unlock()

			select {
			case <-rf.electTimer.C:
				rf.mu.Lock()
				rf.participateElection()
				rf.mu.Unlock()
			case <-rf.appendEntry:
				rf.mu.Lock()
				rf.convertState(FOLLOWER)
				rf.mu.Unlock()
				rf.resetTimer()
			default:
				rf.mu.Lock()
				if rf.totalVoted*2 > len(rf.peers) {
					rf.convertState(LEADER)
					rf.print("becomeLeader")
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
				}
				rf.mu.Unlock()
				time.Sleep(SLEEP_INTERVAL * time.Millisecond)
			}
		case LEADER:
			rf.broadcastEntires()
			time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
		}
	}
}

func (rf *Raft) participateElection() {
	rf.changeTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.totalVoted = 1
	rf.resetTimer()
	rf.broadcastVote()
}

func (rf *Raft) getStateAtomic() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) broadcastVote() {

	rf.totalVoted = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peerId int) {
			var reply RequestVoteReply
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			request := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogEntry().Index, rf.getLastLogEntry().Term}
			rf.print("sendRequestVoteBefore")
			rf.mu.Unlock()
			if rf.sendRequestVote(peerId, &request, &reply) {
				rf.mu.Lock()
				rf.print("sendRequestVoteAfter")
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					rf.totalVoted++
				}

				if reply.Term > rf.currentTerm {
					rf.convertState(FOLLOWER)
					rf.changeTerm(reply.Term)
				}
				if rf.state != CANDIDATE {
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcastEntires() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		timeStamp :=  time.Now().UnixNano() / 1e6
		go func(peerId int, timeStamp int64) {
			var reply RequestAppendEntryReply
			for {
				rf.print(strconv.FormatInt(timeStamp,10) + " " + "boardEntries")
				time.Sleep(SLEEP_INTERVAL * time.Millisecond)
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				request := RequestAppendEntryArgs{rf.currentTerm, rf.me, rf.log[rf.nextIndex[peerId]-1].Index, rf.log[rf.nextIndex[peerId]-1].Term, rf.log[rf.nextIndex[peerId]:], rf.commitIndex}
				rf.mu.Unlock()
				if rf.sendAppendEntries(peerId, &request, &reply) {
					rf.mu.Lock()

					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					if reply.Term == 0 {
						rf.mu.Unlock()
						continue
					}

					if reply.Term > rf.currentTerm {
						rf.convertState(FOLLOWER)
						rf.changeTerm(reply.Term)
						rf.mu.Unlock()
						return
					}
					if !reply.Success {
						rf.nextIndex[peerId]--
					} else {
						if len(request.Entries) != 0 {
							rf.matchIndex[peerId] = request.Entries[len(request.Entries)-1].Index
							rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
						}
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			}
		}(i, timeStamp)
	}

	for {
		tmpCommitIndex := rf.commitIndex + 1
		cnt := 0
		flag := false
		for i := 0; i < rf.getPeerNum(); i++ {
			if tmpCommitIndex <= rf.matchIndex[i] {
				cnt++
				if cnt*2 > rf.getPeerNum() {
					flag = true
					break
				}
			}
		}
		if flag {
			rf.commitIndex = tmpCommitIndex
		} else {
			break
		}
	}
}

func (rf *Raft) getPeerNum() int {
	return len(rf.peers)
}

func getElectTimeout() time.Duration {
	return time.Duration(MIN_ELECTION_INTERVAL+rand.Intn(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)) * time.Millisecond
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	rf.convertState(FOLLOWER)

	rf.electTimer = time.NewTimer(getElectTimeout())
	rf.appendEntry = make(chan struct{})
	rf.requestVote = make(chan struct{})
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.process()
	go rf.applyMsg()

	return rf
}

func (rf *Raft) applyMsg() {
	for {
		rf.mu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			var msg ApplyMsg
			msg.Index = i
			msg.Command = rf.log[i].Command
			rf.applyCh <- msg
			rf.lastApplied = i
			rf.print("applyMsg")
		}
		rf.mu.Unlock()
		time.Sleep(SLEEP_INTERVAL * time.Millisecond)
	}
}

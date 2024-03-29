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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const (
	HeartbeatTime   = 100
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//State
	state string
	//Persistent state on all servers
	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	logs        []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers
	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	//Volatile state on leaderd
	//(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//votes count
	voteCount int

	applyCh chan ApplyMsg
	//election timeout
	time *time.Timer
}

//	logEntry
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //Candidate's term
	CandidateId  int //Candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
//example AppendEntry RPC arguments structure
//
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// Append Entries Reply
type AppendEntryReply struct {
	Term       int
	MatchIndex int
	Success    bool
}

// return currentTerm and whether this server
// believes it is the leader.
// done
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.state == "LEADER"

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// done
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.logs)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
// done
func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		_ = d.Decode(&rf.currentTerm)
		_ = d.Decode(&rf.votedFor)
		_ = d.Decode(&rf.logs)
	}
}

//
// restart the election timeout
// done
func (rf *Raft) restartTime() {
	timeSeed := ElectionMinTime + rand.Int63n(ElectionMaxTime-ElectionMinTime)
	timeout := time.Millisecond * time.Duration(timeSeed)
	if rf.state == "LEADER" {
		timeout = time.Millisecond * time.Duration(HeartbeatTime)
	}
	// init the election timeout
	if rf.time == nil {
		rf.time = time.NewTimer(timeout)
		go func() {
			// endless loop for election timeout
			for {
				<-rf.time.C
				rf.timeout()
			}
		}()
	}
	rf.time.Reset(timeout)
}

//
// when peer timeout, follower and candidate -> candidate, leader -> send heart beat.
// done
func (rf *Raft) timeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// when rf is not the leader, then go to candidate and leader election.
	if rf.state != "LEADER" {
		rf.state = "CANDIDATE"
		rf.currentTerm += 1
		rf.voteCount = 1
		rf.votedFor = rf.me
		rf.persist()
		rf.sendRequestVote()
	} else { // when rf is the leader, then send heartbeat.
		rf.sendAppendEntries()
	}
	//rf restart the election timeout
	rf.restartTime()
}

//
// example RequestVote RPC handler.
// done
func (rf *Raft) RequestVoteRPC(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	canVote := true

	if len(rf.logs) > 0 {
		// candidate's logs is older than follower
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			canVote = false
		}
		// candidate's logs is shorter than follower
		if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
			canVote = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == -1 && canVote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = rf.votedFor == args.CandidateId
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = "FOLLOWER"
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if canVote {
			rf.votedFor = args.CandidateId
			rf.persist()
		}

		rf.restartTime()

		reply.Term = args.Term
		reply.VoteGranted = rf.votedFor == args.CandidateId

		return
	}

}

//
// Candidate ask for vote
// done
func (rf *Raft) sendRequestVote() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
	}

	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, args RequestVoteArgs) {
			var reply RequestVoteReply
			success := rf.peers[server].Call("Raft.RequestVoteRPC", args, &reply)
			if success {
				rf.handleRequestVoteReply(reply)
			}
		}(server, args)
	}
}

//
// vote result handler
// done
func (rf *Raft) handleRequestVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//leader's term is not the latest, back to follower.
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.persist()
		rf.restartTime()
		return
	}

	if rf.state == "CANDIDATE" && reply.VoteGranted {
		rf.voteCount += 1
		if rf.voteCount >= len(rf.peers)/2+1 {
			rf.state = "LEADER"
			for peer, _ := range rf.peers {
				if peer == rf.me {
					continue
				}
				rf.nextIndex[peer] = len(rf.logs)
				rf.matchIndex[peer] = 0
			}
			rf.persist()
			rf.restartTime()
		}
	}
	return
}

//
// AppendEntries
// done
func (rf *Raft) AppendEntriesRPC(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if leader's term is not the latest, tell leader.
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		//candidate should back to follower.
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		//update local term
		rf.currentTerm = args.Term
		reply.Term = args.Term

		if args.PrevLogIndex >= 0 && (len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			//if follower do not match
			index := len(rf.logs) - 1
			if index > args.PrevLogIndex {
				index = args.PrevLogIndex
			}
			//set the index as the local logsIndex has the same term as prevLogTerm
			for index >= 0 {
				if args.PrevLogTerm == rf.logs[index].Term {
					break
				}
				index--
			}
			reply.MatchIndex = index
			reply.Success = false
		} else if args.Entries != nil {
			//log's index and term match with the leader.
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			//cover the local logs with Entries
			rf.logs = append(rf.logs, args.Entries...)
			//commit the logs that leader already commit
			if args.LeaderCommit > rf.commitIndex && len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commit()
			}
			reply.MatchIndex = len(rf.logs) - 1
			reply.Success = true
		} else {
			//local logs is same as leader logs
			//commit the logs that leader already commit
			if args.LeaderCommit > rf.commitIndex && len(rf.logs)-1 >= args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commit()
			}
			reply.MatchIndex = args.LeaderCommit
			reply.Success = true
		}
	}
	rf.persist()
	rf.restartTime()
}

//
// leader send append entries to one follower
// done
func (rf *Raft) sendAppendEntry(server int) {
	//define the massage
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}
	if rf.nextIndex[server] < len(rf.logs) {
		args.Entries = rf.logs[rf.nextIndex[server]:]
	}

	go func(server int, args AppendEntryArgs) {
		var reply AppendEntryReply
		success := rf.peers[server].Call("Raft.AppendEntriesRPC", args, &reply)
		if success {
			rf.handleAppendEntriesReply(server, reply)
		}
	}(server, args)
}

//
// leader send append entries to all followers
// done
func (rf *Raft) sendAppendEntries() {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.sendAppendEntry(peer)
	}
}

//
//	Handle AppendEntry result
//
func (rf *Raft) handleAppendEntriesReply(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "LEADER" {
		return
	}
	//leader's term is old, back to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "FOLLOWER"
		rf.votedFor = -1
		rf.persist()
		rf.restartTime()
		return
	}

	//follower logs match successfully.
	if reply.Success {
		//update follower's nextIndex to a new logIndex
		rf.nextIndex[server] = reply.MatchIndex + 1
		//recommend the matchIndex
		rf.matchIndex[server] = reply.MatchIndex
		count := 1
		for peer, _ := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= reply.MatchIndex {
				count++
			}
		}
		//if majority follower commit, leader can commit.
		if count >= len(rf.peers)/2+1 {
			//leader can just commit the logs in currentTerm, old term's log can not commit directly
			if rf.commitIndex < reply.MatchIndex && rf.logs[reply.MatchIndex].Term == rf.currentTerm {
				rf.commitIndex = reply.MatchIndex
				go rf.commit()
			}
		}
	} else {
		//follower logs do not match, reply.matchIndex do not means match
		rf.nextIndex[server] = reply.MatchIndex + 1
		rf.sendAppendEntry(server)
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	i := rf.lastApplied + 1
	for i <= rf.commitIndex {
		var args ApplyMsg
		args.Index = i + 1
		args.Command = rf.logs[i].Command
		rf.applyCh <- args
		i++
	}
	rf.lastApplied = rf.commitIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	if rf.state != "LEADER" {
		return index, term, false
	}

	newLog := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, newLog)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.persist()

	return index, term, true
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
	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "FOLLOWER"
	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.restartTime()
	return rf
}

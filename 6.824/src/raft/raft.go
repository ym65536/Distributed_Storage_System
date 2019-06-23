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
    "sync"
    "labrpc"
    "fmt"
    "math/rand"
    "time"
    "sync/atomic"
    )

// import "bytes"
// import "labgob"

const (
  FOLLOWER = iota
  CANDIDATE
  LEADER
 
  HEARTBEAT_INTERVAL = 100
  MIN_ELECTION_INTERVAL = 400
  MAX_ELECTION_INTERVAL = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
	  Command      interface{}
    CommandIndex int
}

type LogEntry struct {
  Term int
  Index int
  Command interface {}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

  // Persistent state on all server
  role int32 //  FOLLOWER, CANDIDATE or LEADER
  currentTerm int32 
  voteFor int // vote for which peer
  voteAcquired int // raft.me recv vote count

  electionTimer *time.Timer
  appendChan chan bool
  voteChan chan bool

  // Part B
  log []LogEntry
  commitIndex int
  lastApplied int
  nextIndex []int
  matchIndex []int
  applyCh chan ApplyMsg
}

func (rf *Raft) getTerm() int32 {
  return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) incrTerm() int32 {
  return atomic.AddInt32(&rf.currentTerm, 1)
}

func (rf *Raft) checkState(role int32) bool {
  return atomic.LoadInt32(&rf.role) == role
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
  term = int(rf.getTerm())
  isleader = rf.checkState(LEADER)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
  Term int32
  CandidateId int
  // Part B
  LastLogIndex int
  LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
  Term int32
  VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if args.Term < rf.currentTerm {
    DPrintf("I(%v) not vote Peer(%v) my term:%d, vote term:%d", rf.me, args.CandidateId,
        rf.currentTerm, args.Term)
    reply.VoteGranted = false
    reply.Term = rf.currentTerm
  } else if args.Term > rf.currentTerm {
    rf.currentTerm = args.Term
    rf.updateState(FOLLOWER)
    rf.voteFor = args.CandidateId
    reply.VoteGranted = true
  } else {
    if rf.voteFor == -1 {
      rf.voteFor = args.CandidateId
      reply.VoteGranted = true
    } else {
      reply.VoteGranted = false
    }
  }
  // PartB 
  // check log
  my_lastLogTerm := rf.log[rf.getLastLogIndex()].Term
  if (my_lastLogTerm > args.LastLogTerm) {
    reply.VoteGranted = false;
  } else if (my_lastLogTerm == args.LastLogTerm) {
    if rf.getLastLogIndex() > args.LastLogIndex {
      reply.VoteGranted = false;
    }
  }
  if reply.VoteGranted == true {
    go func() {
      rf.voteChan <- true
    }()
  }
}


type AppendEntryArgs struct {
  Term     int32
  LeaderId int
  // Part B
  PrevLogIndex int
  PrevLogTerm int
  Entries []LogEntry
  LeaderCommit int
}

type AppendEntryReply struct {
  Term    int32
  Success bool
  NextTrial int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
  notify := func() {
    go func() { 
      rf.appendChan <- true 
    }()
  }
  rf.mu.Lock()
  defer notify()
  defer rf.mu.Unlock()
  // term and return value
  if args.Term < rf.currentTerm {
    reply.Success = false
    reply.Term = rf.currentTerm
  } else if args.Term > rf.currentTerm {
    rf.currentTerm = args.Term
    rf.updateState(FOLLOWER)
    reply.Success = true
  } else {
    reply.Success = true
  }

  // log
  if (args.PrevLogIndex > rf.getLastLogIndex()) {
    reply.Success = false;
    reply.NextTrial = rf.getLastLogIndex() + 1
    return
  }
  if (args.PrevLogTerm != rf.log[args.PrevLogIndex].Term) {
    reply.Success = false
    badTerm := rf.log[args.PrevLogIndex].Term
    i := args.PrevLogIndex
    for ; rf.log[i].Term == badTerm; i-- {
    }
    reply.NextTrial = i + 1
    return
  }

  conflitIdx := -1
  if rf.getLastLogIndex() < args.PrevLogIndex + len(args.Entries) {
    conflitIdx = args.PrevLogIndex + 1
  } else {
    for idx := 0; idx < len(args.Entries); idx++ {
      if rf.log[idx + args.PrevLogIndex + 1].Term != args.Entries[idx].Term {
        conflitIdx = idx + args.PrevLogIndex + 1
        break
      }
    }
  }
  if (conflitIdx != -1) {
    rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
  }
  // commit
  if args.LeaderCommit > rf.commitIndex {
    if args.LeaderCommit < rf.getLastLogIndex() {
      rf.commitIndex = rf.getLastLogIndex()
    } else {
      rf.commitIndex = args.LeaderCommit
    }
  }
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

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
  return ok
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
  term ,isLeader = rf.GetState()
  if isLeader == true {
    rf.mu.Lock()
    index = len(rf.log)
    rf.log = append(rf.log, LogEntry{term ,index, command})
    rf.mu.Unlock()
  }

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
  rf.role = FOLLOWER
  rf.voteFor = -1
  rf.voteChan = make(chan bool)
  rf.appendChan = make(chan bool)

  // Part B
  rf.nextIndex = make([]int, len(rf.peers))
  rf.matchIndex = make([]int, len(rf.peers))
  rf.applyCh = applyCh
  rf.log = make([]LogEntry, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

  go rf.startLoop()

	return rf
}

func randElectionDuration() time.Duration {
  r := rand.New(rand.NewSource(time.Now().UnixNano()))
  return time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL)
}

func (rf *Raft) updateState(state int32) {
  if rf.checkState(state) {
    return
  }
  preState := state
  rf.role = state
  switch rf.role {
    case FOLLOWER:
      rf.voteFor = -1
    case CANDIDATE:
      rf.startElection()
    case LEADER:
      for i, _ := range rf.peers {
        rf.nextIndex[i] = rf.getLastLogIndex() - 1
        rf.matchIndex[i] = 0
      }
    default:
      fmt.Printf("Warning: invalid state %d, do nothing.\n", state)
  }
  fmt.Printf("In term %d: Server %d transfer from %d to %d\n", 
      rf.currentTerm, rf.me, preState, rf.role)
}

func (rf *Raft) startElection() {
  rf.incrTerm()
  rf.voteFor = rf.me
  rf.voteAcquired = 1 // Vote for myself
  rf.electionTimer.Reset(randElectionDuration())
  rf.broadcastRequestVote()
}

func (rf *Raft) broadcastRequestVote() {
  args := RequestVoteArgs{Term: atomic.LoadInt32(&rf.currentTerm), CandidateId: rf.me}
  for i, _ := range rf.peers {
    if i == rf.me {
      continue
    }
    go func(server int) {
      var reply RequestVoteReply
      if rf.checkState(CANDIDATE) == false {
        fmt.Printf("My(%d) role not candidate, state=%d.\n", rf.me, rf.role)
        return
      }
      if rf.sendRequestVote(server, &args, &reply) == false {
        fmt.Printf("Server %d send vote req failed, state=%d.\n", rf.me, rf.role)
        return
      }
      rf.mu.Lock()
      defer rf.mu.Unlock()
      if reply.VoteGranted == true {
        rf.voteAcquired += 1
      } else {
        if reply.Term > rf.currentTerm {
          rf.currentTerm = reply.Term
          rf.updateState(FOLLOWER)
        }
      }
    }(i)
  }
}

func (rf *Raft) broadcastAppendEntry() {
  for i, _ := range rf.peers {
    if i == rf.me {
      continue
    }
    go func(server int){
      var args AppendEntryArgs 
      rf.mu.Lock()
      args.Term = rf.currentTerm
      args.LeaderId = rf.me
      args.LeaderCommit = rf.commitIndex
      args.PrevLogIndex = rf.nextIndex[server] - 1
      args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
      if rf.getLastLogIndex() > rf.nextIndex[server] {
        args.Entries = rf.log[rf.nextIndex[server]:]
      }
      rf.mu.Unlock()

      var reply AppendEntryReply
      if rf.checkState(LEADER) == false {
        fmt.Printf("My(%d) role not leader, state=%d.\n", rf.me, rf.role)
        return
      }
      if rf.sendAppendEntry(server, &args, &reply) == false {
        fmt.Printf("Server %d send heartbeat failed, state=%d.\n", rf.me, rf.role)
        return
      }
      if reply.Success == true {
        // success
        rf.nextIndex[server] += len(args.Entries)
        rf.matchIndex[server] = rf.nextIndex[server] - 1
      } else {
        if rf.checkState(LEADER) == false {
          return
        }
        if reply.Term > rf.currentTerm {
          rf.currentTerm = reply.Term
          rf.updateState(FOLLOWER)
        } else {
          rf.nextIndex[server] = reply.NextTrial
          return 
        }
      }
    }(i)
  }
}

func (rf *Raft) getLastLogIndex() int {
  return len(rf.log) - 1
}

func (rf *Raft) startLoop() {
  rf.electionTimer = time.NewTimer(randElectionDuration())
  for {
    switch atomic.LoadInt32(&rf.role) {
      case FOLLOWER:
        select {
          case <- rf.voteChan:
            rf.electionTimer.Reset(randElectionDuration())
          case <- rf.appendChan:
            rf.electionTimer.Reset(randElectionDuration())
          case <-rf.electionTimer.C:
            rf.mu.Lock()
            rf.updateState(CANDIDATE)
            rf.mu.Unlock()
        }
      case CANDIDATE:
        rf.mu.Lock()
        select {
          // candidate will not trigger vote chan, it vote for itself
          case <- rf.appendChan:
            rf.updateState(FOLLOWER)
          case <- rf.electionTimer.C:
            rf.electionTimer.Reset(randElectionDuration())
            rf.startElection()
          default:
            if rf.voteAcquired > len(rf.peers) / 2 {
              rf.updateState(LEADER)
            }
          }
        rf.mu.Unlock()
      case LEADER:
        rf.broadcastAppendEntry()
        rf.updateCommitIndex()
        time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
    }
    go rf.applyLog()
  }
}

func (rf *Raft) updateCommitIndex() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
    matchCount := 1
    for i, matched := range rf.matchIndex {
      if i == rf.me {
        continue
      }
      if matched > rf.commitIndex {
        matchCount += 1
      }
    }
    if (matchCount > len(rf.peers)/2) {
      rf.commitIndex = i
      break
    }
  }
}

func (rf *Raft) applyLog() {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  for i:= rf.lastApplied; i <= rf.commitIndex; i++ {
    var msg ApplyMsg
    msg.CommandIndex = 1
    msg.Command = rf.log[i].Command
    msg.CommandValid = true
    rf.applyCh <- msg
  }
}


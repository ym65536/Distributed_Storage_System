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
//    "sync/atomic"
    )

// import "bytes"
// import "labgob"

const (
  Follower = 0
  Candidate = 1
  Leader = 2
)

const (
  HEARTBEAT_INTERVAL = 100
  MIN_ELECTION_TIMEOUT = 300
  MAX_ELECTION_TIMEOUT = 500
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
  Command interface{}
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

  // Part 2A
  role int
  currentTerm int
  voteFor int // vote for which peer
  voteAcquired int // raft.me recv vote count
  electionTimer *time.Timer
  voteChan chan int // candidate send vote and vote for it
  appendChan chan int // leader send heartbeat or append log request

  // Part 2B
  logs []LogEntry
  commitIndex int
  lastApplied int

  nextIndex []int
  matchIndex []int
  applyChan chan ApplyMsg
}

func (rf *Raft) IncrTerm() {
  rf.currentTerm += 1
}

func (rf *Raft) CheckRole(role int) bool {
  return rf.role == role
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
  term = rf.currentTerm
  isleader = rf.CheckRole(Leader)
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
  Term int
  CandidateId int
  LastLogIndex int
  LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
  Term int 
  VoteGranted bool // candidate get vote?
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if args.Term < rf.currentTerm || 
    (args.Term == rf.currentTerm && rf.voteFor != -1 && 
     rf.voteFor != args.CandidateId) {
    DPrintf("I(%v) not vote for peer(%v), my term:%d,peer term:%d, voteFor:%d", 
        rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.voteFor)
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    return
  }
  rf.voteFor = args.CandidateId
  rf.currentTerm = args.Term
  reply.VoteGranted = true
  if args.Term > rf.currentTerm { // new leader comes
    DPrintf("I(%v) recv from leader(%v), my term:%d,peer term:%d", 
        rf.me, args.CandidateId, rf.currentTerm, args.Term)
    rf.ChangeRole(Follower)
  }

  // part B. candidate vote should be at least up-to-date as recivers's log,
  // see 5.4.1
  lastLogIndex := len(rf.logs) - 1
  if (args.LastLogTerm < rf.logs[lastLogIndex].Term) ||
      (args.LastLogTerm == rf.logs[lastLogIndex].Term && 
       args.LastLogIndex < lastLogIndex) {
    reply.Term = rf.currentTerm;
    reply.VoteGranted = false;
  }

  if reply.VoteGranted == true {
    go func() {
      rf.voteChan <- args.CandidateId
    }()
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

type AppendEntriesArgs struct {
  Term int
  LeaderId int
  // Part B
  PrevLogIndex int 
  PrevLogTerm int
  LogEntries  []LogEntry
  LeaderCommit int
}

type AppendEntriesReply struct {
  Term int 
  Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  // Receiver implementation ==> RIPM_1
  if args.Term < rf.currentTerm {
    reply.Success = false
    reply.Term = rf.currentTerm
    return
  } 
  
  if args.Term > rf.currentTerm { // new leader comes
    DPrintf("I(%v) recv from leader(%v), my term:%d,peer term:%d", 
        rf.me, args.LeaderId, rf.currentTerm, args.Term)
    rf.ChangeRole(Follower)
  }

  // Part B, RIMP_2
  if (len(rf.logs) < args.PrevLogIndex + 1) || 
      (rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
    reply.Success = false
    reply.Term = rf.currentTerm
    return
  }
  // RIMP_3
  mismatch_idx := -1
  for idx := range args.LogEntries {
    if (len(rf.logs) < args.PrevLogIndex + 1 + idx + 1) ||
      (rf.logs[args.PrevLogIndex + 1 + idx].Term != args.LogEntries[idx].Term) {
        mismatch_idx = idx;
        break;
      }
  }
  if mismatch_idx != -1 {
    rf.logs = rf.logs[:args.PrevLogIndex + 1 + mismatch_idx]
    // RIMP_4
    rf.logs = append(rf.logs, args.LogEntries[mismatch_idx:]...)
    DPrintf("I(%v) mismatch index(%v), my term:%d,peer term:%d", 
        rf.me, mismatch_idx, rf.currentTerm, args.Term)
  } 
  // RIMP_5
  if args.LeaderCommit > rf.commitIndex {
    lastLogIndex := len(rf.logs) - 1
    if lastLogIndex < args.LeaderCommit {
      rf.commitIndex = args.LeaderCommit
    } else {
      rf.commitIndex = lastLogIndex
    }
    rf.Apply()
  }
  
  if args.Term > rf.currentTerm {
    reply.Success = true
    rf.currentTerm = args.Term
    rf.ChangeRole(Follower)
  } else {
    reply.Success = true
  }

  // noitfy 
  go func() {
    rf.appendChan <- args.LeaderId
  }()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// should be called with a lock
func (rf *Raft) Apply() {
  // apply all entries between lastApplied and committed
  // should be called after commitIndex updated
  if rf.commitIndex > rf.lastApplied {
    go func(start_idx int, entries []LogEntry) {
      for idx, entry := range entries {
        DPrintf("%v applies command %d on index %d", rf, entry.Command.(int), start_idx+idx)
        var msg ApplyMsg
        msg.CommandValid = true
        msg.Command = entry.Command
        msg.CommandIndex = start_idx + idx
        rf.applyChan <- msg
        rf.mu.Lock()
        rf.lastApplied = msg.CommandIndex
        rf.mu.Unlock()
      }
    }(rf.lastApplied+1, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
  }
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
  term, isLeader = rf.GetState()
  if isLeader {
    rf.mu.Lock()
    index = len(rf.logs)
    rf.logs = append(rf.logs, LogEntry{Command:command, Term:term})
    rf.matchIndex[rf.me] = index
    rf.nextIndex[rf.me] = index + 1
    DPrintf("%v start agreement on command %d on index %d", rf, command.(int), index)
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
  rf.role = Follower
  rf.voteFor = -1
  rf.voteAcquired = 0
  rf.voteChan = make(chan int)
  rf.appendChan = make(chan int)

  rf.applyChan = applyCh
  rf.logs = make([]LogEntry, 1) // start from index 1
  rf.nextIndex = make([]int, len(rf.peers))
  for i := range rf.nextIndex { // initialize with 1
    rf.nextIndex[i] = len(rf.logs)
  } 
  rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  
  go rf.StartLoop()
  
	return rf
}

func (rf *Raft) StartLoop() {
  rf.electionTimer = time.NewTimer(ElectionDuration())
  for {
    switch (rf.role) {
      case Follower: // Rules for follower
        select {
          case <- rf.voteChan: // grant vote for candidate
            rf.electionTimer.Reset(ElectionDuration())
          case <- rf.appendChan: // append log or heart beat from leader
            rf.electionTimer.Reset(ElectionDuration())
          case <- rf.electionTimer.C: // election timeout
            rf.ChangeRole(Candidate)
            rf.StartElection() // according to rules for candidate, start election
        }
      case Candidate: // Rules for candidate
        select {
          case <- rf.appendChan: // recv leader's append log reqeust or heartbeat
            rf.ChangeRole(Follower)
          case <- rf.electionTimer.C: // election timeout, start new election
            rf.electionTimer.Reset(ElectionDuration())
            rf.StartElection()
          default:
            if rf.voteAcquired > len(rf.peers) / 2 {
              rf.ChangeRole(Leader)
            }
        }
      case Leader: // Rules for leader
        rf.BroadcastAppendEntries()
        time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
    }
  }
}

func ElectionDuration() time.Duration {
  r := rand.New(rand.NewSource(time.Now().UnixNano()))
  return time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_TIMEOUT -
        MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT)
}

func (rf *Raft) ChangeRole (role int) {
  if rf.CheckRole(role) {
    fmt.Printf("Warning: role %d exist.\n", role)
    return
  }
  preRole := rf.role
  rf.role = role
  switch (role) {
    case Follower:
      rf.voteFor = -1
      rf.voteAcquired = 0
    case Candidate: 
      rf.StartElection()
    case Leader: 
      for i, _ := range rf.nextIndex {
        rf.nextIndex[i] = len(rf.logs)
        rf.matchIndex[i] = 0
      } 
      rf.StartLoop()
    default:
      fmt.Printf("Error: invalid role %d.\n", role)
  }
  fmt.Printf("In term %d: Server %d transfer from %d to %d\n", 
            rf.currentTerm, rf.me, preRole, rf.role)
}

func (rf *Raft) StartElection() {
  if !rf.CheckRole(Candidate) {
    fmt.Printf("Error: invalid role=%d, not candidate.\n", rf.role)
    return
  }
  rf.IncrTerm() // incream current term
  rf.voteFor = rf.me // vote for myself
  rf.voteAcquired = 1 // myself vote granted
  rf.electionTimer.Reset(ElectionDuration()) 
  rf.BroadcastRequestVote() // send request vote to peers
}

func (rf *Raft) BroadcastRequestVote() {
  if rf.CheckRole(Candidate) == false { // only candidate can send request vote
    fmt.Printf("My(%d) role not candidate, state=%d.\n", rf.me, rf.role)
    return
  }
  lastLogIndex := len(rf.logs) - 1
  args := RequestVoteArgs {
            Term: rf.currentTerm, 
            CandidateId: rf.me,
            LastLogIndex: lastLogIndex,
            LastLogTerm: rf.logs[lastLogIndex].Term}
  for i, _ := range rf.peers {
    if rf.me == i {
      continue // do not need request vote for myself
    }
    go func(peer int) {
      var reply RequestVoteReply
      res := rf.sendRequestVote(peer, &args, &reply)
      if res == false {
        fmt.Printf("Server %d send vote req failed, role=%d.\n", rf.me, rf.role)
        return
      }
      if reply.VoteGranted == true {
        rf.voteAcquired += 1
        fmt.Printf("Server %d request vote granted. acquire=%d.\n", rf.me, rf.voteAcquired)
      } else { // for rules 2: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if reply.Term > rf.currentTerm {
          rf.currentTerm = reply.Term
          rf.ChangeRole(Follower)
        }
      }
    } (i)
  }
}

func (rf *Raft) BroadcastAppendEntries() {
  if rf.CheckRole(Leader) == false { // only leader can send append entries
    fmt.Printf("My(%d) role not leader, state=%d.\n", rf.me, rf.role)
    return
  }
  for i,_ := range rf.peers {
    if rf.me == i {
      continue // do not need append entry for myself
    }
    go func(peer int) {
      // Part B
      prevLogIndex := rf.nextIndex[peer] - 1
      var args AppendEntriesArgs
      args.Term = rf.currentTerm
      args.LeaderId = rf.me
      args.PrevLogIndex = prevLogIndex
      args.PrevLogTerm = rf.logs[prevLogIndex].Term
      args.LogEntries = rf.logs[prevLogIndex + 1:]
      args.LeaderCommit = rf.commitIndex

      var reply AppendEntriesReply
      fmt.Printf("leader=%d send heartbeat to server=%d.\n", rf.me, peer)
      if rf.sendAppendEntries(peer, &args, &reply) == false {
        fmt.Printf("Server %d send heartbeat to peer %d fail.\n", rf.me, peer)
        return
      }
      if reply.Success == true {
        // Part B
        rf.matchIndex[peer] = args.PrevLogIndex + len(args.LogEntries)
        rf.nextIndex[peer] = rf.matchIndex[peer] + 1
        // See Rules for Servers: Leader
        // check if we need to update commitIndex, from last to commited
        for idx := len(rf.logs) - 1; idx > rf.commitIndex; idx -- {
          count := 0
          for _, matchIndex := range rf.matchIndex {
            if matchIndex >= idx {
              count += 1
            }
          }
          if count > len(rf.peers) / 2 { // most nodes agreed
            rf.commitIndex = idx
            rf.Apply()
            break
          }
        }
      } else {
        if reply.Term > rf.currentTerm { // For rule 2
          rf.currentTerm = reply.Term
          rf.ChangeRole(Follower)
        } else { // log mismatch
          rf.nextIndex[peer] -= 1 
          // need retry ?
        }
      }
    } (i)
  }
}


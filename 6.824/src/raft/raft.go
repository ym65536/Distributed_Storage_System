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
  currentTerm int32 
  state int32 //  FOLLOWER, CANDIDATE or LEADER
  voteFor int // vote for which peer
  voteAcquired int // raft.me recv vote count

  electionTimer *time.Timer
  applyChan chan bool
  voteChan chan bool
}

func (rf *Raft) getTerm() int32 {
  return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) incrTerm() int32 {
  return atomic.AddInt32(&rf.currentTerm, 1)
}

func (rf *Raft) checkState(state int32) bool {
  return atomic.LoadInt32(&rf.state) == state
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

type AppendEntryArgs struct {
  Term     int32
  LeaderId int
}

type AppendEntryReply struct {
  Term    int32
  Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

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
  go func() { 
    rf.applyChan <- true 
  }()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()
  if args.Term < rf.currentTerm {
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
  if reply.VoteGranted == true {
    go func() {
      rf.voteChan <- true
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
  rf.state = FOLLOWER
  rf.voteFor = -1
  rf.voteChan = make(chan bool)
  rf.applyChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

  go rf.startLoop()

	return rf
}

func (rf *Raft) startLoop() {
  rf.electionTimer = time.NewTimer(randElectionDuration())
  for {
    switch atomic.LoadInt32(&rf.state) {
      case FOLLOWER:
        select {
          case <- rf.voteChan:
            rf.electionTimer.Reset(randElectionDuration())
          case <- rf.applyChan:
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
          case <- rf.applyChan:
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
        time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
    }
  }
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
  rf.state = state
  switch rf.state {
    case FOLLOWER:
      rf.voteFor = -1
    case CANDIDATE:
      rf.startElection()
    case LEADER:
      // do nothing
    default:
      fmt.Printf("Warning: invalid state %d, do nothing.\n", state)
  }
  fmt.Printf("In term %d: Server %d transfer from %d to %d\n", 
      rf.currentTerm, rf.me, preState, rf.state)
}

func (rf *Raft) startElection() {
  rf.incrTerm()
  rf.voteFor = rf.me
  rf.voteAcquired = 1
  rf.electionTimer.Reset(randElectionDuration())
  rf.broadcastVote()
}

func (rf *Raft) broadcastVote() {
  args := RequestVoteArgs{Term: atomic.LoadInt32(&rf.currentTerm), CandidateId: rf.me}
  for i, _ := range rf.peers {
    if i == rf.me {
      continue
    }
    go func(server int) {
      var reply RequestVoteReply
      if rf.checkState(CANDIDATE) && rf.sendRequestVote(server, &args, &reply) {
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
      } else {
        fmt.Printf("Server %d send vote req failed.\n", rf.me)
      }
    }(i)
  }
}

func (rf *Raft) broadcastAppendEntry() {
  args := AppendEntryArgs {Term: atomic.LoadInt32(&rf.currentTerm), LeaderId:rf.me}
  for i, _ := range rf.peers {
    if i == rf.me {
      continue
    }
    go func(server int){
      var reply AppendEntryReply
      if rf.checkState(LEADER) && rf.sendAppendEntry(server, &args, &reply) {
        if reply.Success == true {
          // success, do nothing
        } else {
          if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.updateState(FOLLOWER)
          }
        }
      }
    }(i)
  }
}

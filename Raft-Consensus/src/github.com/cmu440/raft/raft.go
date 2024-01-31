//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// Defines a custom type 'ServerState' as an integer.
type ServerState int

// Server States
const (
	Leader    ServerState = iota + 1 // the server state is a leader represented by integer 1
	Candidate                        // the server state is a candidate represented by integer 2
	Follower                         // the server state is a follower represented by integer 3
)

// Timer Constants in Milliseconds
const (
	EpochTimerMinMillis  = 450
	EpochTimerMaxMillis  = 950
	HeartBeatTimerMillis = 110
)

// Structure to store Log Entries
type LogEntry struct {
	Command interface{} // command received
	Term    int         // term when this command was received
	Index   int         // log index for this command
}

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Edstem or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	electionTimer       *time.Timer       // election timer for this server to initiate elections
	heartbeatTimer      *time.Timer       // heartbeat timer for this server to initiate hearbeats
	currentTerm         int               // the current term for this node
	votedFor            int               // the peer this node has voted for
	serverType          ServerState       // the server state, it'll be either Leader, Candidate, Follower
	votesReceived       int               // the number of votes received from peers
	serviceApplyChannel chan ApplyCommand // send committed log to this channel
	stopServerReq       chan bool         // request to stop the server
	stopServerResp      chan bool         // response after server has been stopped
	majority            int               // the number of peers which need to be in agreement
	logs                []LogEntry        // list of logs at this server
	commitIndex         int               // to store the latest log index which was committed
	lastApplied         int               // to store the latest log which was applied
	nextIndex           []int             // used only if server is leader and indicates which logs need to be sent next
	matchIndex          []int             // used only if server is leader and indicates upto which the logs match b/w a peer and leader
}

// GetState
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rft *Raft) GetState() (int, int, bool) {
	// Shared state, so lock to avoid potential race
	rft.mux.Lock()
	defer rft.mux.Unlock()

	return rft.me, rft.currentTerm, rft.serverType == Leader
}

// RequestVoteArgs
// ===============
//
// # Structure to store RequestVote RPC arguments
//
// # Please note: Field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateTerm         int // candidate’s term
	CandidateId           int // candidate requesting vote
	CandidateLastLogIndex int // index of candidate’s last log entry
	CandidateLastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply
// ================
//
// # Structure to store RequestVote RPC reply
//
// # Please note: Field names must start with capital letters!
type RequestVoteReply struct {
	CurrentTerm int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote
// ===========
// Handles the receiver end of RequestVote RPC
func (rft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Shared state so lock to avoid potential race
	rft.mux.Lock()
	defer rft.mux.Unlock()

	// received old term or same term
	// same term can be returned false since only
	// competing candidates or server which has already voted will have
	// same term
	if args.CandidateTerm <= rft.currentTerm {
		reply.CurrentTerm = rft.currentTerm
		reply.VoteGranted = false
	} else {
		// received a later term
		// check candidate's log is at least as up to date as receivers log
		rftLastLogIndex := len(rft.logs) - 1
		rftLastLogTerm := -1
		if rftLastLogIndex >= 0 {
			rftLastLogTerm = rft.logs[rftLastLogIndex].Term
		}
		// the condition checks for upto dateness of candidate log
		// if the log lengths are same then
		// the candidate should have last log term >= current server's
		// last log term. Or if the length of logs are different then
		// the candidate's last log term must be strictly greater than
		// current last log term
		if (args.CandidateLastLogIndex == rftLastLogIndex &&
			args.CandidateLastLogTerm >= rftLastLogTerm) ||
			(args.CandidateLastLogIndex != rftLastLogIndex &&
				args.CandidateLastLogTerm > rftLastLogTerm) {
			// only if its as up to date then grant vote
			rft.resetElectionTimer()
			rft.votedFor = args.CandidateId
			rft.serverType = Follower
			rft.currentTerm = args.CandidateTerm
			// this heartbeat stop only affects the previous leader
			rft.heartbeatTimer.Stop()
			// grant vote since log is upto date
			reply.CurrentTerm = args.CandidateTerm
			reply.VoteGranted = true
		} else {
			// if the log was not as up to date then
			// do not grant the vote to candidate
			reply.CurrentTerm = rft.currentTerm
			reply.VoteGranted = false
		}
	}
}

// Structure to store AppendEntries RPC arguments
type AppendEntriesArgs struct {
	LeaderTerm   int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// Structure to store AppendEntries RPC reply
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries Receiver
// ===========
// Handles the receiver end of AppendEntries RPC
func (rft *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Shared state so lock to avoid potential race
	rft.mux.Lock()
	defer rft.mux.Unlock()

	// store the last log index of this server
	rftLastLogIndex := len(rft.logs) - 1

	// Reply false if term < currentTerm
	if args.LeaderTerm < rft.currentTerm {
		reply.Success = false
		reply.Term = rft.currentTerm
	} else if args.PrevLogIndex > rftLastLogIndex {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		reply.Success = false
		reply.Term = rft.currentTerm
		rft.resetElectionTimer()
	} else if args.PrevLogIndex >= 0 && args.PrevLogTerm != rft.logs[args.PrevLogIndex].Term {
		// If an existing entry conflicts with a new one (same index but different terms)
		reply.Success = false
		reply.Term = rft.currentTerm
		rft.resetElectionTimer()
	} else {
		// Append any new entries not already in the log
		rft.resetElectionTimer()
		rft.logs = rft.logs[:args.PrevLogIndex+1]
		rft.logs = append(rft.logs, args.Entries...)
		rft.serverType = Follower
		// heartbeat timer stop will only be affected for a previous leader
		rft.heartbeatTimer.Stop()
		reply.Success = true
		reply.Term = args.LeaderTerm
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry), only
	// in case of success
	if reply.Success {
		rft.commitIndex = min(args.LeaderCommit, len(rft.logs)-1)
	}

	// if the commitIndex of this server is updated
	// iterate from lastApplied index to commitIndex and
	// send newly committed logs to apply channel
	if rft.commitIndex > rft.lastApplied {
		rft.updateLastApplied()
	}
}

// Invoked by leader to replicate log entries and also used as heartbeat
func (rft *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// calls the AppendEntries RPC
	ok := rft.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		// if the response is ok then lock to avoid race
		rft.mux.Lock()

		// if append entries unsuccessful then convert to follower
		if reply.Term > rft.currentTerm {
			rft.serverType = Follower
			rft.currentTerm = reply.Term
			rft.heartbeatTimer.Stop()
			rft.resetElectionTimer()
			rft.mux.Unlock()
			return ok
		}
		// if the server is still the leader i.e. not affected by the above
		// condition
		if rft.serverType == Leader {
			// if append entries is successful
			if reply.Success {
				// update the nextIndex, added if condition here
				// to prevent nextIndex being updated multiple times
				if args.PrevLogIndex+1 == rft.nextIndex[server] {
					rft.nextIndex[server] += len(args.Entries)
				}
				// calculate matchIndex, i.e. till where the logs match for
				// this server
				rft.matchIndex[server] = rft.nextIndex[server] - 1
				// iterate from commitIndex + 1 till the end to see
				// whether any other log can be committed
				for i := rft.commitIndex + 1; i < len(rft.logs); i++ {
					// check needs to happen for logs belonging to the leader's term
					if rft.logs[i].Term == rft.currentTerm {
						count := 1
						// count how many peers have this log
						for j := 0; j < len(rft.peers); j++ {
							if rft.matchIndex[j] >= i {
								count += 1
							}
						}
						// if majority of the peers have the log
						// then commit this log in the leader
						if count >= rft.majority {
							rft.commitIndex = i
						}
					}
				}
				// if any new log was commited then update lastApplied
				if rft.commitIndex > rft.lastApplied {
					rft.updateLastApplied()
					// starts agreement here signaling peers to commit as well
					rft.sendHeartbeats()
				}
			} else {
				// else the logs didn't match try decrementing the nextIndex
				// and retrying again
				rft.nextIndex[server] -= 1
				prevLogIndex := rft.nextIndex[server] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rft.logs[prevLogIndex].Term
				}
				// construct append entries args based on prevLogIndex
				appendEntriesArgs := &AppendEntriesArgs{
					LeaderTerm:   rft.currentTerm,
					LeaderId:     rft.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rft.logs[rft.nextIndex[server]:],
					LeaderCommit: rft.commitIndex,
				}
				appendEntriesReply := &AppendEntriesReply{}
				// spun up a go routine to send the append entries request to the server
				go rft.sendAppendEntries(server, appendEntriesArgs, appendEntriesReply)
			}
		}
		rft.mux.Unlock()
	}
	return ok
}

// Handles updating of lastApplied index of the server
// and sending committed logs to service apply channel
func (rft *Raft) updateLastApplied() {
	// if nothing has been applied then lastApplied would be -1
	if rft.lastApplied < 0 {
		rft.lastApplied = 0
	}
	// iterates from lastApplied to current commitIndex
	// and sends the committed log info to the apply channel
	for i := rft.lastApplied; i <= rft.commitIndex; i++ {
		rft.serviceApplyChannel <- ApplyCommand{
			Index:   rft.logs[i].Index,
			Command: rft.logs[i].Command,
		}
	}
	// update lastApplied to the current commitIndex
	rft.lastApplied = rft.commitIndex
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// calls the RequestVote RPC for the "server" in the arg list
	ok := rft.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rft.mux.Lock()
		// check if the current server is still a candidate
		// and not converted to a follower from a previous response
		if rft.serverType == Candidate {
			// if its still a candidate then check if vote was granted
			if reply.VoteGranted {
				// if vote was granted then increment votes received
				// by this candidate and check majority condition
				rft.votesReceived += 1
				if rft.votesReceived >= rft.majority {
					// if majority voted received then
					// server becomes leader after election
					rft.electionTimer.Stop()
					rft.serverType = Leader
					// initialize match index and next index of peers
					for i := 0; i < len(rft.peers); i++ {
						rft.nextIndex[i] = len(rft.logs)
						rft.matchIndex[i] = -1
					}
					// reset heartbeat timer
					rft.resetHeartbeatTimer()
					// send hearbeats
					rft.sendHeartbeats()
				}
			} else if reply.CurrentTerm > rft.currentTerm {
				// if current term < received then this server
				// steps down to become a follower again
				rft.currentTerm = reply.CurrentTerm
				rft.serverType = Follower
				rft.resetElectionTimer()
			}
		}
		rft.mux.Unlock()
	}
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rft *Raft) PutCommand(command interface{}) (int, int, bool) {
	// client submits request to raft
	rft.mux.Lock()
	defer rft.mux.Unlock()

	// update index and term only if this is leader
	index := -1
	term := -1
	isLeader := rft.serverType == Leader
	if isLeader {
		// if its a leader then note the index and term for the new log
		index = len(rft.logs) + 1
		term = rft.currentTerm
		// the leader has updated its logs
		rft.logs = append(rft.logs, LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		})
		rft.sendHeartbeats()
	}
	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
// Handles stopping the server
// follows a request and response structure to stop the server
func (rf *Raft) Stop() {
	// send request to stop the server
	rf.stopServerReq <- true
	// wait till the main routine is stopped
	<-rf.stopServerResp
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	// every new server starts off as a follower
	rft := &Raft{
		electionTimer:       nil,                     // election timer is nil before main routine call
		heartbeatTimer:      nil,                     // heartbeat timer is nil before main routine call
		currentTerm:         0,                       // current term is initialized with 0
		votedFor:            -1,                      // current this server doesn't participate in voting
		serverType:          Follower,                // the server starts off as a follower
		votesReceived:       0,                       // no election held till now
		serviceApplyChannel: applyCh,                 // the service apply channel to send the committed messages
		stopServerReq:       make(chan bool),         // channel to request stopping the server
		stopServerResp:      make(chan bool),         // channel which send back response when server is stopped
		majority:            len(peers)/2 + 1,        // majority condition required for consensus
		logs:                make([]LogEntry, 0),     // initially no logs present
		commitIndex:         -1,                      // no log has been committed yet
		lastApplied:         -1,                      // no log has been applied yet
		nextIndex:           make([]int, len(peers)), // used by a leader to store from which log index logs needs to be replicated
		matchIndex:          make([]int, len(peers)), // used by a leader to store upto which log index logs are replicated
		peers:               peers,                   // peers information
		me:                  me,                      // id of this server
	}
	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rft.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rft.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rft.logger.Println("logger initialized")
	} else {
		rft.logger = log.New(ioutil.Discard, "", 0)
	}

	// run the mainRoutine as a go routine and return the rft pointer
	go rft.mainRoutine()
	return rft
}

// Main routine for this server which manager election timeouts and heartbeat timeouts
// in case this server is a leader
func (rft *Raft) mainRoutine() {
	rft.mux.Lock()
	// initialize election timer
	electionTimerDurationRange := EpochTimerMinMillis + rand.Intn(EpochTimerMaxMillis-EpochTimerMinMillis)
	rft.electionTimer = time.NewTimer(time.Millisecond * time.Duration(electionTimerDurationRange))

	// initialize heartbeat timer
	rft.heartbeatTimer = time.NewTimer(time.Millisecond * time.Duration(HeartBeatTimerMillis))
	rft.heartbeatTimer.Stop()
	rft.mux.Unlock()

	// an infinite loop to keep on processing for this server
	for {
		select {
		// case when server is stopped
		case <-rft.stopServerReq:
			rft.stopServerResp <- true
			return

		// election epoch timeout case activated
		case <-rft.electionTimer.C:
			rft.mux.Lock()
			// become a candidate and start election
			rft.startElection()
			// get a new random election timer
			rft.resetElectionTimer()
			rft.mux.Unlock()

		// heartbeat timer activated
		case <-rft.heartbeatTimer.C:
			rft.mux.Lock()
			// send hearbeats
			rft.sendHeartbeats()
			// resets hearbeat timer
			rft.resetHeartbeatTimer()
			rft.mux.Unlock()
		}
	}
}

// Handler for candidate to start an election
func (rft *Raft) startElection() {
	// server becomes a candidate
	rft.votesReceived = 0
	rft.serverType = Candidate
	// update term and vote for self
	rft.currentTerm += 1
	rft.votedFor = rft.me
	rft.votesReceived += 1
	// extract lastLogIndex of the candidate based on which
	// the logs upto dateness will be checked
	lastLogIndex := len(rft.logs) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rft.logs[lastLogIndex].Term
	}
	// send requestVote RPCs to other servers
	// by iterating over the peers splice
	for i := 0; i < len(rft.peers); i++ {
		// don't send request to itself
		if i != rft.me {
			reqVoteArgs := &RequestVoteArgs{
				CandidateTerm:         rft.currentTerm,
				CandidateId:           rft.me,
				CandidateLastLogIndex: lastLogIndex,
				CandidateLastLogTerm:  lastLogTerm,
			}
			// to store reply of request vote
			reqVoteReply := &RequestVoteReply{}
			// spun up a go routine to send request vote
			// so that the main routine is not blocked
			go rft.sendRequestVote(i, reqVoteArgs, reqVoteReply)
		}
	}
}

// Handles sending heartbeats to all servers except itself
func (rft *Raft) sendHeartbeats() {
	// iterates over all the peer in the network
	for i := 0; i < len(rft.peers); i++ {
		if i != rft.me {
			// fetch the nextLongIndex to send to this server
			nextLogIndex := rft.nextIndex[i]
			// calculate the prevLogIndex and prevLogTerm
			prevLogIndex := nextLogIndex - 1
			prevLogTerm := -1
			// if its not the first log to send then extract term
			if prevLogIndex >= 0 {
				prevLogTerm = rft.logs[prevLogIndex].Term
			}
			// construct append entried args
			// if entries to send is empty, then it acts like heartbeat
			appendEntriesArgs := &AppendEntriesArgs{
				LeaderTerm:   rft.currentTerm,
				LeaderId:     rft.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      rft.logs[nextLogIndex:],
				LeaderCommit: rft.commitIndex,
			}
			appendEntriesReply := &AppendEntriesReply{}
			// spawn go routines to process sending append entries to each peer
			go rft.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply)
		}
	}
}

// Resets the election timer with a random duration
// within a specified range to trigger elections at different times
func (rft *Raft) resetElectionTimer() {
	duration := EpochTimerMinMillis + rand.Intn(EpochTimerMaxMillis-EpochTimerMinMillis)
	rft.electionTimer.Reset(time.Millisecond * time.Duration(duration))
}

// Resets the heartbeat timer for leader to maintain regular communication with other
// servers by sending heartbeats at a fixed interval
func (rft *Raft) resetHeartbeatTimer() {
	duration := HeartBeatTimerMillis
	rft.heartbeatTimer.Reset(time.Millisecond * time.Duration(duration))
}

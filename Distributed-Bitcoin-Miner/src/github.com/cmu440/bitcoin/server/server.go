// Contains implementation of bitcoin server application
//
// Load balancing strategy:
// Requests from the clients are broken down into jobs
// of a certain max size (10000 operation) and added to a pending
// jobs list.
//
// If a miner is available to process pending jobs, a job with the
// least number of operations and which arrived farthest back in time
// is selected from the pending jobs list and assigned to the miner
//
// A miner is available once it joins the server and also once it
// processes a job, and returns the result to the server
//
// If a miner is closed (detected either by Read or Write failure)
// the job assigned to the miner is then added to the pending jobs
// list and availability of another miner is checked. If a miner is
// available, a pending job is assigned based on the policy described
// above
//
// If a client is closed, (detected either by Read or Write failure)
// all the pending jobs corresponding to the client are dropped

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const (
	MaxOps = 10000 // max number of operations in a job that can be assigned to a miner
)

// Structure to store the details of a job
// (unit split from a request, that can be assigned to a miner)
type job struct {
	id           string    // job identifier
	data         string    // request message string
	lower        uint64    // lower boundary of the nonce range
	upper        uint64    // upper boundary of the nonce range
	requestSize  uint64    // number of operations in the request corresponding to this job
	requestTime  time.Time // time when the request corresponding to this job, arrived
	minHash      uint64    // minimum hash value in the nonce range handled by this job
	minHashNonce uint64    // nonce corresponding to the minimum hash value
}

// Structure to store the request sent by a client
type request struct {
	jobList          []*job // list of the jobs split from this request
	numJobsRemaining int    // count of the jobs that are still pending
}

// Structure to store availability and the job assigned to a miner
type miner struct {
	available bool // boolean to check if a miner is available
	job       *job // job assigned to the miner
}

// Structure to store server details
type server struct {
	lspServer   lsp.Server       // lsp server implementation used by the bitcoin server application
	miners      map[int]*miner   // map of active miners
	requests    map[int]*request // map of active client requests
	pendingJobs []*job           // list of pending jobs
}

// Initialise the lsp server
func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	server := &server{
		lspServer:   lspServer,
		miners:      make(map[int]*miner),
		requests:    make(map[int]*request),
		pendingJobs: make([]*job, 0),
	}
	return server, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	for {
		// Read message from the lsp server
		connID, message, err := srv.readMessage()
		if err == nil {
			// Handle incoming messages from miners and clients
			srv.handleMessage(*message, connID)
		} else {
			// In case Read fails, the connection is considered to be
			// closed. If connection ID exists in the miners map,
			// it can be considered that the closed connection belongs
			// to the miner, else the closed connection belongs to a client
			miner, exists := srv.miners[connID]
			if exists {
				// If the closed miner was processing a job, add the job
				// to the pending jobs list and check if a miner is available
				// to reassign the job
				if miner.job != nil {
					// Check if a miner is available
					// (unavailability can be determined if
					// there is at least one job pending)
					minersMaybeAvailable := true
					if len(srv.pendingJobs) > 0 {
						minersMaybeAvailable = false
					}
					// Add the job to the pending jobs list
					srv.pendingJobs = append(srv.pendingJobs, miner.job)
					if minersMaybeAvailable {
						// If a miner is available, handle the pending jobs
						srv.handlePendingJobs()
					}
				}
				// Remove the miner from the miners map
				delete(srv.miners, connID)
			} else {
				// Handle pending jobs corresponding to the closed client
				srv.handleLostClientConnection(connID)
			}
		}
	}
}

// Handle messages received from the miners and clients
func (srv *server) handleMessage(message bitcoin.Message, connID int) {
	switch message.Type {
	// Join message from a miner
	case bitcoin.Join:
		// Add the new miner to the miners map
		newMiner := &miner{
			available: true,
			job:       nil,
		}
		_, exists := srv.miners[connID]
		if !exists {
			srv.miners[connID] = newMiner
		}
		// Handle any pending jobs when a new miner joins the server
		srv.handlePendingJobs()
	// Request message from a client
	case bitcoin.Request:
		jobIndex := 0
		jobList := make([]*job, 0)
		// Split the request into jobs of 10000 operation chunks
		for lower := message.Lower; lower <= message.Upper; lower += MaxOps {
			// Upper boundary of the job is the lower boundary
			// plus max operations if its less than the upper boundary
			// of the request
			upper := message.Upper
			if lower+MaxOps-1 < message.Upper {
				upper = lower + MaxOps - 1
			}
			newJob := &job{
				id:           strconv.Itoa(connID) + ":" + strconv.Itoa(jobIndex), // job identifier is requestid:index
				data:         message.Data,
				lower:        lower,
				upper:        upper,
				requestSize:  message.Upper - message.Lower + 1,
				requestTime:  time.Now(),
				minHash:      math.MaxUint64,
				minHashNonce: 0,
			}
			jobIndex++
			jobList = append(jobList, newJob)
		}
		newRequest := &request{
			jobList:          jobList,
			numJobsRemaining: jobIndex,
		}
		// New client request is stored in server requests map
		srv.requests[connID] = newRequest

		// Check if a miner is available
		// (unavailability can be determined if
		// there is at least one job pending)
		minersMaybeAvailable := true
		if len(srv.pendingJobs) > 0 {
			minersMaybeAvailable = false
		}

		// Add all the jobs to the pending jobs list
		for _, job := range jobList {
			srv.pendingJobs = append(srv.pendingJobs, job)
		}
		if minersMaybeAvailable {
			// If a miner is available, handle the pending jobs
			srv.handlePendingJobs()
		}
	// Result message from a miner
	case bitcoin.Result:
		// Update the min hash value and corresponding nonce of the job
		// processed by the miner
		connectedMiner := srv.miners[connID]
		connectedMinerJob := connectedMiner.job
		connectedMinerJob.minHash = message.Hash
		connectedMinerJob.minHashNonce = message.Nonce

		// Update the number of remaining operations of the
		// corresponding request if it exists
		connectedMinerRequestID, _ := strconv.Atoi(
			strings.Split(connectedMinerJob.id, ":")[0])
		connectedMinerRequest, exists :=
			srv.requests[connectedMinerRequestID]
		if exists {
			connectedMinerRequest.numJobsRemaining--
			// If all the jobs of the request are processed,
			// determine the min hash of the request from the
			// min hash values computed from all the jobs,
			// and also the corresponding nonce value
			if connectedMinerRequest.numJobsRemaining == 0 {
				var minHash uint64 = math.MaxUint64
				var minHashNonce uint64 = 0
				for _, job := range connectedMinerRequest.jobList {
					if job.minHash < minHash {
						minHash = job.minHash
						minHashNonce = job.minHashNonce
					}
				}
				// sent the result back to the client
				resultMessage := bitcoin.NewResult(minHash, minHashNonce)
				err := srv.writeMessage(connectedMinerRequestID, resultMessage)
				if err != nil {
					// If write fails, drop the pending jobs corresponding
					// to the client
					srv.handleLostClientConnection(connectedMinerRequestID)
				} else {
					// If the write back to the client is successful,
					// delete the request from the requests map
					delete(srv.requests, connectedMinerRequestID)
				}
			}
		}
		// Update the status of the miner
		connectedMiner.available = true
		connectedMiner.job = nil
		// Handle the pending jobs now that a miner is available
		srv.handlePendingJobs()
	}
}

// Read message from lsp server and return the connection ID from
// which the message arrived along with the bitcoin message itself
func (srv *server) readMessage() (int, *bitcoin.Message, error) {
	connID, payload, err := srv.lspServer.Read()
	if err != nil {
		return connID, nil, err
	}
	var message bitcoin.Message
	err = json.Unmarshal(payload, &message)
	if err != nil {
		return 0, nil, err
	}
	return connID, &message, err
}

// Write message to the lsp server, connection ID of the miner or client
// to which the message is to be sent and the bitcoin message itself are
// provided as input arguments.
func (srv *server) writeMessage(connID int, message *bitcoin.Message) error {
	var payload []byte
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return srv.lspServer.Write(connID, payload)
}

// Select a job with the least number of operations
// and which arrived farthest back in time, and assign
// it to an available miner
func (srv *server) handlePendingJobs() {
	if len(srv.pendingJobs) > 0 {
		var minRequestSize uint64 = math.MaxUint64
		oldestRequestTime := time.Now()
		minRequestSizeJob := &job{}
		minRequestSizeJobIdx := 0
		for idx, pendingJob := range srv.pendingJobs {
			if pendingJob.requestSize <= minRequestSize {
				if pendingJob.requestSize < minRequestSize {
					// If the job's request size if less than
					// the minimum request size observed,
					// update the minimum request size
					minRequestSize = pendingJob.requestSize
					oldestRequestTime = pendingJob.requestTime
					minRequestSizeJob = pendingJob
					minRequestSizeJobIdx = idx
				} else if pendingJob.requestTime.Before(oldestRequestTime) {
					// Else if the request size is same as the minimum
					// request size and arrived before the oldest request
					// time observed, update the oldest request time
					oldestRequestTime = pendingJob.requestTime
					minRequestSizeJob = pendingJob
					minRequestSizeJobIdx = idx
				}
			}
		}
		// Assign the selected job to an available miner
		srv.assignJobToMiner(minRequestSizeJob, minRequestSizeJobIdx)
	}
}

// Assign a pending job to an available miner
func (srv *server) assignJobToMiner(job *job, jobIdx int) {
	selectedMinerId := -1
	for connID, miner := range srv.miners {
		if miner.available {
			// If a miner is available, update its status,
			// and send the job request to it
			selectedMinerId = connID
			miner.available = false
			miner.job = job

			requestMessage := bitcoin.NewRequest(job.data, job.lower, job.upper)
			err := srv.writeMessage(connID, requestMessage)
			if err != nil {
				// If write to the miner fails (closed connection),
				// keep looking for another available miner and delete
				// the disconnected miner from the miners map
				selectedMinerId = -1
				delete(srv.miners, connID)
				continue
			}
			break
		}
	}
	// If the job request was sent to a miner successfully,
	// remove it from the pending jobs list
	if selectedMinerId > 0 {
		srv.pendingJobs =
			append(srv.pendingJobs[:jobIdx], srv.pendingJobs[jobIdx+1:]...)
	}
}

// Drop the pending jobs corresponding to a closed client
func (srv *server) handleLostClientConnection(connId int) {
	// Remove the disconnected client jobs from the pending jobs list
	for idx, pendingJob := range srv.pendingJobs {
		// Client connection ID parsed from the job ID
		pendingJobRequestID, _ := strconv.Atoi(
			strings.Split(pendingJob.id, ":")[0])
		if pendingJobRequestID == connId {
			srv.pendingJobs =
				append(srv.pendingJobs[:idx], srv.pendingJobs[idx+1:]...)
		}
	}
	// Remove the client request from the requests map if it exists
	_, exists := srv.requests[connId]
	if exists {
		delete(srv.requests, connId)
	}
}

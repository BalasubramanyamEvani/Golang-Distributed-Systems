package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Each miner gets a job
type job struct {
	data  string // the data message to hash
	lower uint64 // lower bound nonce value
	upper uint64 // upper bound nonce value
}

// Attempt to connect miner as a client to the server.
// TODO: implement this!
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	// create a new LSP client
	minerConn, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	// create new join request to send to the server
	joinMsg := bitcoin.NewJoin()
	payload, err := json.Marshal(joinMsg)
	if err != nil {
		return nil, err
	}

	// if successfully marshaled send to server
	err = minerConn.Write(payload)

	// in case of an error while writing to server
	// return err
	if err != nil {
		return nil, err
	}

	// else return the miner conenction and nil error
	return minerConn, nil
}

var LOGF *log.Logger

// TODO: implement this!
func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]

	// attempt to join with the server
	miner, err := joinWithServer(hostport)

	// if miner fails to join return
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// continue receiving messages from the server
	// for as long as it remains feasible.
	for {
		recv, err := miner.Read()

		// if unable to read or nil response received
		// stop miner by returning
		if err != nil || recv == nil {
			return
		}

		// variable to store the un-marshaled response
		var msg bitcoin.Message
		err = json.Unmarshal(recv, &msg)

		// if error while un-marshaling then continue
		// processing with next Read
		if err != nil {
			continue
		}

		// a job struct wrapper created based on the
		// sent information from the server
		job := &job{
			data:  msg.Data,
			lower: msg.Lower,
			upper: msg.Upper,
		}

		// start mining for minimum hash value
		minHashValue, minNonce := calculateHash(job)

		// create a new Result message to send back to the server
		result := bitcoin.NewResult(minHashValue, minNonce)
		resMsg, err := json.Marshal(result)

		// if error while marshaling result
		// continue processing with new Read request
		if err != nil {
			continue
		}

		// write Result message to server
		err = miner.Write(resMsg)

		// if error while writing result to server
		// then stop the miner
		if err != nil {
			break
		}
	}
}

// calculateHash: method accepts a struct of type job as a parameter which specifies the
// parameters for the hash calculation and then runs the hash calculation routine wherein
// it goes over all the nonce values i.e. in range [lower, upper] and returns
// back "min nonce" and "min hash value"
func calculateHash(job *job) (uint64, uint64) {
	// lower bound for the hash calculation
	var currMinNonce uint64 = job.lower

	// initializing current min hash value to max uint64 value
	var currMinHashValue uint64 = math.MaxUint64

	// runs through all nonce values in range [lower, upper]
	for currNonce := job.lower; currNonce <= job.upper; currNonce++ {
		// calls the provided Hash function on the job data message
		hashValue := bitcoin.Hash(job.data, currNonce)

		// if curr hash value is less than the currMinHashValue
		// then update the currMinNonce and currMinHashValue
		if hashValue < currMinHashValue {
			currMinHashValue = hashValue
			currMinNonce = currNonce
		}
	}

	return currMinHashValue, currMinNonce
}

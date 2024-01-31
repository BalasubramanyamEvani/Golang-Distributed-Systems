package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "clientLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	// send new "Request" message to server from this client
	// with the specified message and maxNonce parameter
	err = sendRequestToServer(client, message, maxNonce)

	// if error in sending request to server
	// stop client by returning
	if err != nil {
		return
	}

	// variable to store response message from server
	var responseMessage bitcoin.Message

	// fetch the response from server
	err = fetchResponseFromServer(client, &responseMessage)

	// if error while receiving response
	// stop client by returning
	if err != nil {
		return
	}

	// if receive successful print the result
	printResult(responseMessage.Hash, responseMessage.Nonce)
}

// sendRequestToServer: method accepts lsp client, message and maxNonce parameters
// and tries to send a new "Request" message to server. this method returns
// an error if marshaling fails or there's an error while writing to the server
// "printDisconnected()" is called in case of the latter
func sendRequestToServer(client lsp.Client, message string, maxNonce uint64) error {
	clientRequest := bitcoin.NewRequest(message, 0, maxNonce)
	clientPayload, err := json.Marshal(clientRequest)
	if err != nil {
		return err
	}
	err = client.Write(clientPayload)
	if err != nil {
		printDisconnected()
		return err
	}
	return nil
}

// fetchResponseFromServer: method accepts lsp client and variable to store
// the response message of type bitcoin.Message. method waits to receive
// response back from  server and calls "printDisconnected()" in case of
// error while reading back from server or if nil payload is received. the method
// returns nil in case of no error
func fetchResponseFromServer(client lsp.Client, responseMessage *bitcoin.Message) error {
	responsePayload, err := client.Read()
	if err != nil {
		printDisconnected()
		return err
	}
	if responsePayload == nil {
		printDisconnected()
		return errors.New("Nil response received from server")
	}
	err = json.Unmarshal(responsePayload, responseMessage)
	if err != nil {
		return err
	}
	return nil
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}

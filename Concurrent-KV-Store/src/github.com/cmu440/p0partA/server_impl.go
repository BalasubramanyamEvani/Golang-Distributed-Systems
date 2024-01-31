// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"

	"github.com/cmu440/p0partA/kvstore"
)

// TCP Connections Maximum Output Buffer Size
const maxBufferSize = 500

// A struct to represent a client connection
type client struct {
	id           uint64      // Unique identifier for the client
	conn         net.Conn    // The network connection associated with the client
	writeReqChan chan []byte // Channel for sending data to the client
	stopRead     chan bool   // Channel to signal stopping of read routine of the client
	stopWrite    chan bool   // Channel to signal stopping of write routine of the client
}

// A struct to represent a request from a client
type clientReq struct {
	clientId uint64   // The identifier of the client making the request
	name     string   // The name of the request or operation - GET, PUT, DELETE, UPDATE
	args     []string // Request Parameters
}

// A struct to represent a Key Value Server and its operations
type keyValueServer struct {
	listener       net.Listener       // The listener associated with the server
	store          kvstore.KVStore    // The actual key value database store
	connCounter    uint64             // To generate Client connection id
	activeClients  map[uint64]*client // Map to maintain information on clients - One to Many
	droppedClients []uint64           // Splice to maintain the identifiers of the dropped clients

	addClientChan    chan net.Conn  // Channel to send request to add a new client connection
	closeClientChan  chan *client   // Channel to send request to close a client
	clientReqChan    chan clientReq // Channel to send request to process a client request
	activecntReqChan chan bool      // Channel to send request to return active clients count
	activeCliLen     chan int       // Channel to which number of active clients would be written at any ppint of time
	dropcntReqChan   chan bool      // Channel to send request to return dropped clients count
	dropCliLen       chan int       // Channel to which number of dropped clients would be written at any point of time
	closeMainChan    chan bool      // Channel to send request to close all connections and terminate main routine
}

// New creates and returns (but does not start) a new KeyValueServer.
// Your keyValueServer should use `store kvstore.KVStore` internally.
func New(store kvstore.KVStore) KeyValueServer {
	// Creates a new Key Value Server
	activeClientsMap := make(map[uint64]*client)
	droppedClientsSlice := []uint64{}

	addClientChannel := make(chan net.Conn)
	closeClientChannel := make(chan *client)
	clientReqChannel := make(chan clientReq)
	activecntReqChannel := make(chan bool)
	activeCliLen := make(chan int)
	dropcntReqChannel := make(chan bool)
	dropCliLenChannel := make(chan int)
	closeMainChannel := make(chan bool)

	server := keyValueServer{
		listener:       nil,
		store:          store,
		connCounter:    0,
		activeClients:  activeClientsMap,
		droppedClients: droppedClientsSlice,

		addClientChan:    addClientChannel,
		closeClientChan:  closeClientChannel,
		clientReqChan:    clientReqChannel,
		activecntReqChan: activecntReqChannel,
		activeCliLen:     activeCliLen,
		dropcntReqChan:   dropcntReqChannel,
		dropCliLen:       dropCliLenChannel,
		closeMainChan:    closeMainChannel,
	}
	return &server
}

// Starts the server on a specified port
func (kvs *keyValueServer) Start(port int) error {
	// Defines to use TCP network type and create the address string
	network := "tcp"
	address := ":" + strconv.Itoa(port)

	// Attempt to listen on the specified network and address
	ln, err := net.Listen(network, address)
	if err != nil {
		// If an error occurs during listening, return the error
		return err
	}

	// Store the listener in the kvs to use in AccepRoutine
	kvs.listener = ln

	// Starts two goroutines: one for accepting incoming client connections
	// and another for handling the main server logic
	go kvs.acceptRoutine()
	go kvs.mainRoutine()
	return nil
}

// Starts the Server's Accept Routine
func (kvs *keyValueServer) acceptRoutine() {
	// Infinite Loop
	for {
		// Use listener to accept client connections
		conn, err := kvs.listener.Accept()
		if err != nil {
			// If an error occurs during accepting a connection, return (terminate)
			return
		}

		// Signal the main routine to add the new client connection
		kvs.addClientChan <- conn
	}
}

// Starts the Server's Main Routine
func (kvs *keyValueServer) mainRoutine() {
	// Infinite Loop
	for {
		select {
		// Add new client case
		case conn := <-kvs.addClientChan:
			kvs.addNewClient(conn)
		// Drop client case
		case cli := <-kvs.closeClientChan:
			kvs.dropClient(cli)
		// Return current active clients count
		case <-kvs.activecntReqChan:
			kvs.activeCliLen <- len(kvs.activeClients)
		// Returns current dropped clients count
		case <-kvs.dropcntReqChan:
			kvs.dropCliLen <- len(kvs.droppedClients)
		// Processes a client's request
		case req := <-kvs.clientReqChan:
			kvs.processReq(req)
		// Closes all clients and Main routine
		case <-kvs.closeMainChan:
			kvs.closeClients()
			return
		}
	}
}

// Drops a client from the active clients map and updates dropped clients splice
func (kvs *keyValueServer) dropClient(cli *client) {
	_, exists := kvs.activeClients[cli.id]

	// If the client exists, remove it from the active clients map and
	// append its client identifier to the dropped clients list
	if exists {
		delete(kvs.activeClients, cli.id)
		kvs.droppedClients = append(kvs.droppedClients, cli.id)
	}
}

// Closes all active client connections
func (kvs *keyValueServer) closeClients() {
	// Iterate through all active clients and close connection and
	// signal stopping of respective read and write routines
	for _, cli := range kvs.activeClients {
		cli.conn.Close()
		cli.stopRead <- true
		cli.stopWrite <- true
	}
}

// Handles addition of a new client
func (kvs *keyValueServer) addNewClient(conn net.Conn) {
	// Creates a new client struct and get its address
	new_client_details := &client{
		id:           kvs.connCounter,
		conn:         conn,
		writeReqChan: make(chan []byte, maxBufferSize),
		stopWrite:    make(chan bool),
		stopRead:     make(chan bool),
	}

	// Adds the client to active clients map in kvs instance
	kvs.activeClients[kvs.connCounter] = new_client_details
	cli := kvs.activeClients[kvs.connCounter]

	// Starts two goroutines: one for reading from the client
	// and another for writing to the client
	go kvs.readRoutine(cli)
	go kvs.writeRoutine(cli)

	// increment conn counter
	kvs.connCounter += 1
}

// Handles processing DB requests
func (kvs *keyValueServer) processReq(req clientReq) {
	switch req.name {
	// If a Put request is reveived
	case "Put":
		key, val := req.args[0], req.args[1]
		kvs.store.Put(key, []byte(val))
	// If a Get request is reveived
	case "Get":
		key := req.args[0]
		val := kvs.store.Get(key)

		// val is a 2D splice, joine using newline and then prefix it with key:
		// as mentioned in writeup and finally add a newline at the end
		resp_join := []byte(key + ":" + string(bytes.Join(val, []byte("\n"))) + "\n")

		// If slow write store only 500 messages, otherwise if there is space
		// send signal to client's write routine to write to the client
		if len(kvs.activeClients[req.clientId].writeReqChan) < maxBufferSize {
			kvs.activeClients[req.clientId].writeReqChan <- resp_join
		}
	// If a Delete request is reveived
	case "Delete":
		key := req.args[0]
		kvs.store.Delete(key)
	// If an Update request is reveived
	case "Update":
		key, old_val, new_val := req.args[0], req.args[1], req.args[2]
		kvs.store.Update(key, []byte(old_val), []byte(new_val))
	}
}

// Handles reading data from a client's network connection
func (kvs *keyValueServer) readRoutine(cli *client) {
	reader := bufio.NewReader(cli.conn)
	for {
		select {
		// Stop the read routine
		case <-cli.stopRead:
			return
		// Default case is to read till newline and parse the client request
		// this is non blocking so if the stop case becomes available
		// it'll be executed
		default:
			// Read client request till newline
			line, err := reader.ReadString('\n')
			if err != nil {
				// If an error occurs while reading, signal the client's write routine to stop
				cli.stopWrite <- true

				// Finally signal main routine to drop the client
				kvs.closeClientChan <- cli
				return
			}
			// If no error occurs, parse and send this to be processed in main routine
			// so that there are no race conditions

			// Trim any trailing newline from the input request
			trimmed := strings.TrimSuffix(line, "\n")

			// Split the trimmed string into parts using ':' as the delimiter
			parts := strings.Split(trimmed, ":")

			// Send to main routine to process this client request
			name, args := parts[0], parts[1:]
			req := clientReq{
				clientId: cli.id,
				name:     name,
				args:     args,
			}
			kvs.clientReqChan <- req
		}
	}
}

// Handles writing data to a client's network connection
func (kvs *keyValueServer) writeRoutine(cli *client) {
	for {
		select {
		// Stop the write routine
		case <-cli.stopWrite:
			return
		// Receive a request and attempt to write it to the client's connection
		case req := <-cli.writeReqChan:
			_, err := cli.conn.Write(req)
			if err != nil {
				// If an error occurs while writing, signal the client's read routine to stop
				// since in read routine default is non blocking
				// this should not pose any problems
				cli.stopRead <- true

				// Finally signal main routine to drop the client
				kvs.closeClientChan <- cli
				return
			}
		}
	}
}

// Closes the server and associated resources
func (kvs *keyValueServer) Close() {
	// Close the listener to stop accepting new client connections
	kvs.listener.Close()

	// Signal the main routine to close
	kvs.closeMainChan <- true
}

// Returns the count of active client connections
func (kvs *keyValueServer) CountActive() int {
	// Blocks till Main Routine receiver is available
	kvs.activecntReqChan <- true

	// Blocks till signal sent from main routine
	return <-kvs.activeCliLen
}

// Returns the count of active client connections
func (kvs *keyValueServer) CountDropped() int {
	// Blocks till Main Routine receiver is available
	kvs.dropcntReqChan <- true

	// Blocks till signal sent from main routin
	return <-kvs.dropCliLen
}

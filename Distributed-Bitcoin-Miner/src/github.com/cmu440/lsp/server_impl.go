// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

// Payload Size
const PayloadSize = 1500

// Client message struct used for encapsulating sending/receiving messages
type clientMsg struct {
	msg     Message
	srcAddr *lspnet.UDPAddr
}

// Status of each message sent from server to client
type sentMsg struct {
	msg               *clientMsg // outgoing message
	sent              bool       // whether sent or not
	ack               bool       // whether the message is acked or not
	epochCount        int        // the epoch count when it was sent
	nextTransmitEpoch int        // when this needs to be retransmitted again
	currentBackoff    int        // the current backoff interval for retransmitting this message
}

// Client struct containing information regarding a client
// connection with the server
type clientInfo struct {
	connId            int                // the connection id for this client
	receivingSeqNum   int                // initial receiving seq number expected from client
	sendingSeqNum     int                // initial sending seq number by server
	addr              lspnet.UDPAddr     // the connection address
	msgs              map[int]*clientMsg // map to store the out of order messages
	closedInit        bool               // whether closing of this client has been initialized
	isClosed          bool               // whether the client is completely closed
	handleOutgoingMsg chan *clientMsg    // channel to handle outgoing message from server to client
	handleSendAckMsg  chan *clientMsg    // channel to handle sending Ack Message
	handleClientMsg   chan *clientMsg    // channel to handle receiving msg from client
	windowLimit       int                // the window limit in the outgoing message list
	lowestUnAckSN     int                // the lowest unacked seq number in the outgoing message list
	unAckMsgCntr      int                // unacked message counter
	outgoingMsgsList  []*sentMsg         // buffer to maintain the outgoing messages (statuses) for this client
	lastAliveEpoch    int                // the last epoch when message was received from this client
	lastSentEpoch     int                // the last epoch when message was sent to this client
	epochLimitReached bool               // whether epoch limit reached
}

// Server struct containing information regarding the
// spun up server
type server struct {
	udpConn               *lspnet.UDPConn     // the udp connection to read a packet from
	params                *Params             // params with which this server was initialized
	currClientId          int                 // the current unique client id which can be given to a new connection
	clients               map[int]*clientInfo // map of clients
	msgReceive            chan *clientMsg     // channel to handle receiving client message
	msgStoreReq           chan *clientMsg     // channel to store read messages
	msgsUnread            *list.List          // queue which stores processed but unread messages
	serverAppMsgReadReq   chan bool           // channel to handle request server read operation
	serverAppMsgReadResp  chan *clientMsg     // channel to handle read response
	serverAppMsgWriteReq  chan *clientMsg     // channel to handle server write request to a client
	serverAppMsgWriteResp chan error          // channel to receive server write response
	serverCloseReq        chan bool           // channel to handle server closing request
	serverCloseResp       chan error          // channel to receive server closing response
	clientCloseReq        chan int            // channel to handle client closing request
	clientCloseResp       chan error          // channel to handle client client closing response
	closeReadRoutine      chan bool           // channel to handle closing of read routine
	closeMsgStoreRoutine  chan bool           // channel to handle closing of msg store routine
	epochCount            int                 // channel to store current epoch time
	serverCloseError      error               // stores error message in case of any client ungraceful exit while server closing
	closeInit             bool                // whether closing of server initialized
	closed                bool                // whether the server is completely closed
	connectedClientAddrs  map[string]int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	// start server by listening on specified port
	network := "udp"
	address := lspnet.JoinHostPort("localhost", strconv.Itoa(port))
	udpAddr, err := lspnet.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, errors.New("error while creating server " + err.Error())
	}
	udpConn, err := lspnet.ListenUDP(network, udpAddr)
	if err != nil {
		return nil, errors.New("error while creating server " + err.Error())
	}
	server := &server{
		udpConn:               udpConn,
		params:                params,
		currClientId:          1,
		clients:               make(map[int]*clientInfo),
		msgReceive:            make(chan *clientMsg),
		msgStoreReq:           make(chan *clientMsg),
		msgsUnread:            list.New(),
		serverAppMsgReadReq:   make(chan bool),
		serverAppMsgReadResp:  make(chan *clientMsg),
		serverAppMsgWriteReq:  make(chan *clientMsg),
		serverAppMsgWriteResp: make(chan error),
		serverCloseReq:        make(chan bool),
		serverCloseResp:       make(chan error),
		clientCloseReq:        make(chan int),
		clientCloseResp:       make(chan error),
		closeReadRoutine:      make(chan bool),
		closeMsgStoreRoutine:  make(chan bool),
		epochCount:            0,
		serverCloseError:      nil,
		closeInit:             false,
		closed:                false,
		connectedClientAddrs:  make(map[string]int),
	}

	// instantiate three go routines

	go server.readRoutine()     // routine to read UDP packets
	go server.mainRoutine()     // routine to handle receiving and outgoing messages
	go server.msgStoreRoutine() // routine to handle server read operationg

	return server, nil
}

// Server read routine to read from the UDP connection
func (s *server) readRoutine() {
	for {
		select {
		// return if a signal to close is received
		case <-s.closeReadRoutine:
			return
		default:
			payload := make([]byte, PayloadSize)
			n, addr, err := s.udpConn.ReadFromUDP(payload)
			if err != nil {
				continue
			}
			payload = payload[:n]
			var message Message
			err = json.Unmarshal(payload, &message)
			if err != nil {
				return
			}
			// checks if payload is malformed or not
			if len(message.Payload) < message.Size {
				continue
			} else {
				message.Payload = message.Payload[:message.Size]
			}
			if err != nil {
				continue
			}
			newMessage := &clientMsg{
				msg:     message,
				srcAddr: addr,
			}
			select {
			// return if a signal to close is received
			case <-s.closeReadRoutine:
				return

			// send the client message to main handler
			case s.msgReceive <- newMessage:
				continue
			}
		}
	}
}

// Server main routine to handle incoming and outgoing messages
func (s *server) mainRoutine() {
	for {
		select {
		// handle receiving client message
		case message := <-s.msgReceive:
			s.handleReq(message)

		// handle server application write request
		case message := <-s.serverAppMsgWriteReq:
			// write message to client from server
			cli := s.clients[message.msg.ConnID]

			// if client close initialized
			if cli.closedInit {
				s.serverAppMsgWriteResp <- errors.New("client connection is closed")
				continue
			}

			// construct message to send to client
			message.srcAddr = &cli.addr
			message.msg.SeqNum = cli.sendingSeqNum
			message.msg.Checksum = CalculateChecksum(message.msg.ConnID, message.msg.SeqNum, message.msg.Size, message.msg.Payload)
			toStore := &sentMsg{
				msg:               message,
				sent:              false,
				ack:               false,
				epochCount:        0,
				nextTransmitEpoch: 0,
				currentBackoff:    0,
			}

			// append current message to outgoing msgs list
			cli.outgoingMsgsList = append(cli.outgoingMsgsList, toStore)

			// if outgoing list length is 1
			// re-initialize lowest unacked sequence number and
			// send limit based on window size
			if len(cli.outgoingMsgsList) == 1 {
				cli.lowestUnAckSN = toStore.msg.msg.SeqNum
				cli.windowLimit = cli.lowestUnAckSN + s.params.WindowSize
			}

			// if message seq within window and MaxUnacked not reached
			// send message
			if cli.unAckMsgCntr < s.params.MaxUnackedMessages && message.msg.SeqNum < cli.windowLimit {
				s.sendMsgToClient(toStore.msg)
				toStore.sent = true
				toStore.epochCount = s.epochCount
				toStore.nextTransmitEpoch = s.epochCount + 1

				cli.unAckMsgCntr += 1
				cli.lastSentEpoch = s.epochCount
			}

			// increment sendingSeqNum for next message
			// and write nil for server write response
			cli.sendingSeqNum += 1
			s.serverAppMsgWriteResp <- nil

		// handle epoch events
		case <-time.Tick(time.Duration(s.params.EpochMillis) * time.Millisecond):
			s.epochCount += 1

			// check if server close is initialized
			if s.closeInit {
				// check for any client all outgoing
				// messages have been sent and acked or not
				anyPendingClient := false
				for _, cli := range s.clients {
					if !cli.isClosed {
						anyPendingClient = true
						break
					}
				}
				// if there are no pending clients
				// close the udp connection and server
				// go routines for read and msg storing
				if !anyPendingClient {
					s.udpConn.Close()
					s.closeReadRoutine <- true
					s.closeMsgStoreRoutine <- true
					s.serverCloseResp <- s.serverCloseError
					s.closed = true
					return
				}
			}

			// iterate of over clients if not server close initialized
			// or server close initialized but have pending clients
			for _, cli := range s.clients {
				// check if any client's epochLimit is reached
				if !cli.epochLimitReached {
					// if not reached check for client closing intialization
					// and length of outgoing messages
					if cli.closedInit && len(cli.outgoingMsgsList) == 0 {
						// if client closing initialized and no outgoing messages present
						// close the connection
						cli.isClosed = true

						// send message to msgStore to indicate closing of a client
						s.msgStoreReq <- &clientMsg{
							Message{
								Type:    MsgData,
								ConnID:  cli.connId,
								Payload: nil,
							},
							&cli.addr,
						}
						delete(s.connectedClientAddrs, cli.addr.String())
						continue
					}

					// if epoch limit reached for a client
					if s.epochCount-cli.lastAliveEpoch > s.params.EpochLimit {
						// update serverCloseError to indicate abrupt closure of a client
						// detected and close the connection
						cli.epochLimitReached = true
						s.serverCloseError = errors.New("abrupt client connection closure")
						cli.closedInit = true
						cli.isClosed = true
						s.msgStoreReq <- &clientMsg{
							Message{
								Type:    MsgData,
								ConnID:  cli.connId,
								Payload: nil,
							},
							&cli.addr,
						}
						delete(s.connectedClientAddrs, cli.addr.String())
						continue
					}

					// process client level backoff
					s.processClientPendingBackoff(cli)

					// send heartbeat from server to client if we didn't send
					// anything in the last epoch
					if !cli.closedInit && cli.lastSentEpoch < s.epochCount-1 {
						s.sendMsgToClient(&clientMsg{
							msg:     *NewAck(cli.connId, 0),
							srcAddr: &cli.addr,
						})
					}
				}
			}

		// handle client closing request
		case connId := <-s.clientCloseReq:
			cli, ok := s.clients[connId]
			// check if client not present or close initialized for that client
			if !ok || cli.closedInit {
				s.clientCloseResp <- errors.New("invalid client closure request")
				continue
			}

			// intialize closing of the client
			cli.closedInit = true

			// client connection already closed by server
			// due to epoch limit reached then send an errror
			if cli.epochLimitReached {
				s.clientCloseResp <- errors.New("epoch limit reached, dropped the connection")
				continue
			}

			// else send nil and not block
			s.clientCloseResp <- nil

		// handle serve closing request
		case <-s.serverCloseReq:
			// initialize closing of the server
			s.closeInit = true

			// iterate over clients and intialize respective closing
			for _, cli := range s.clients {
				cli.closedInit = true
				if len(cli.outgoingMsgsList) == 0 {
					cli.isClosed = true
					s.msgStoreReq <- &clientMsg{
						Message{
							Type:    MsgData,
							ConnID:  cli.connId,
							Payload: nil,
						},
						&cli.addr,
					}
					delete(s.connectedClientAddrs, cli.addr.String())
				}
			}

			// check if any outgoing message present for any client
			anyPendingClient := false
			for _, cli := range s.clients {
				if !cli.isClosed {
					anyPendingClient = true
					break

				}
			}

			// if there are no pending clients close the server
			// and other server routines and return
			if !anyPendingClient {
				s.udpConn.Close()
				s.closeReadRoutine <- true
				s.closeMsgStoreRoutine <- true
				s.serverCloseResp <- s.serverCloseError
				s.closed = true
				return
			}
		}
	}
}

// Handles receiving msg from client part of main routine
func (s *server) handleReq(message *clientMsg) {
	switch message.msg.Type {
	// If connection message received
	case MsgConnect:
		var cli *clientInfo

		// check if it was already received
		// if not received before
		// create a new connection id and update s.currClientId
		// and add to connectedClientAddrs
		storedId, ok := s.connectedClientAddrs[message.srcAddr.String()]
		if !ok {
			id := s.currClientId
			sendingSeqNum := 1
			s.clients[id] = &clientInfo{
				connId:            id,
				receivingSeqNum:   message.msg.SeqNum + 1,
				sendingSeqNum:     sendingSeqNum,
				addr:              *message.srcAddr,
				msgs:              make(map[int]*clientMsg),
				closedInit:        false,
				isClosed:          false,
				handleOutgoingMsg: make(chan *clientMsg),
				handleSendAckMsg:  make(chan *clientMsg),
				handleClientMsg:   make(chan *clientMsg),
				windowLimit:       sendingSeqNum + s.params.WindowSize,
				lowestUnAckSN:     sendingSeqNum,
				unAckMsgCntr:      0,
				outgoingMsgsList:  make([]*sentMsg, 0),
				lastAliveEpoch:    s.epochCount,
				lastSentEpoch:     0,
				epochLimitReached: false,
			}
			s.connectedClientAddrs[message.srcAddr.String()] = id
			s.currClientId += 1
			cli = s.clients[id]
		} else {
			// else its a duplicate request so just
			// get stored cli info and send ack
			cli = s.clients[storedId]

			// any msg from client update lastLiveEpoch
			cli.lastAliveEpoch = s.epochCount
		}

		// send ack back to client
		s.sendMsgToClient(&clientMsg{
			msg:     *NewAck(cli.connId, message.msg.SeqNum),
			srcAddr: message.srcAddr,
		})

		// update last sent epoch of the client
		cli.lastSentEpoch = s.epochCount

	// If data message received
	case MsgData:
		// get connection id and fetch info from
		// clients map
		connId := message.msg.ConnID
		cli := s.clients[connId]

		// any message from client update lastLiveEpoch
		cli.lastAliveEpoch = s.epochCount

		// calculates checksum and validates
		expectedChecksum := CalculateChecksum(connId, message.msg.SeqNum, message.msg.Size, message.msg.Payload)
		if expectedChecksum != message.msg.Checksum {
			return
		}

		// send ack back to client if its not closed
		if !cli.isClosed {
			s.sendMsgToClient(&clientMsg{
				msg:     *NewAck(connId, message.msg.SeqNum),
				srcAddr: message.srcAddr,
			})
			cli.lastSentEpoch = s.epochCount
		}

		// if expected receiving seq num from client
		// greater than seq number recieved now then
		// ignore
		if message.msg.SeqNum < cli.receivingSeqNum {
			return
		}

		// if current message seq is equal to the expected
		// seq number from client then start putting into
		// msg buffer in order to be read later by server read operation
		if message.msg.SeqNum == cli.receivingSeqNum {
			s.msgStoreReq <- message
			cli.receivingSeqNum += 1
			for {
				m, ok := cli.msgs[cli.receivingSeqNum]
				if ok {
					s.msgStoreReq <- m
					delete(cli.msgs, cli.receivingSeqNum)
					cli.receivingSeqNum += 1
				} else {
					break
				}
			}
		} else {
			// else just store locally in client struct
			cli.msgs[message.msg.SeqNum] = message
		}

	// If ack message received
	case MsgAck:
		// get client info
		cli := s.clients[message.msg.ConnID]

		// if client is already closed, ignore ack
		if cli.isClosed {
			return
		}

		// any msg from client update lastLiveEpoch
		cli.lastAliveEpoch = s.epochCount

		// heartbeat
		if message.msg.SeqNum == 0 {
			return
		}

		// outgoing msg index for which we recieved Ack
		outgoingMsgIndx := message.msg.SeqNum - cli.lowestUnAckSN

		// invalid ack
		// some garbage msg or when we received ack for a previous window message or duplicate ack
		if len(cli.outgoingMsgsList) < outgoingMsgIndx ||
			outgoingMsgIndx < 0 || cli.outgoingMsgsList[outgoingMsgIndx].ack {
			return
		}

		// set ack = true for the outgoing message
		// and decrement cli unAcked counter
		cli.outgoingMsgsList[outgoingMsgIndx].ack = true
		cli.unAckMsgCntr -= 1

		// process pending ack
		s.processPendingMessageAck(cli)

	// If cack message received
	case MsgCAck:
		// get client info
		cli := s.clients[message.msg.ConnID]

		// if client is already closed, ignore ack
		if cli.isClosed {
			return
		}

		// any msg from client update lastLiveEpoch
		cli.lastAliveEpoch = s.epochCount

		// outgoing msg index for which we received CAck
		outgoingMsgIndx := message.msg.SeqNum - cli.lowestUnAckSN

		// set ack = true and decrement unAcked counter
		// for all seq <= the receiving seq
		for i := 0; i < outgoingMsgIndx; i++ {
			if !cli.outgoingMsgsList[outgoingMsgIndx].ack {
				cli.outgoingMsgsList[outgoingMsgIndx].ack = true
				cli.unAckMsgCntr -= 1
			}
		}

		// process pending ack
		s.processPendingMessageAck(cli)
	}
}

// Method to handle pending clients outgoing messages
func (s *server) processClientPendingBackoff(cli *clientInfo) {
	// iterate over the outgoing message list for the client
	// in case client epoch limit not reached
	for _, msgRecord := range cli.outgoingMsgsList {
		// if any message sent and not acked initiate retransmission
		if msgRecord.sent && !msgRecord.ack {
			msgRecord.epochCount = s.epochCount

			// if for a message retransmit epoch is reached
			// retransmit message and update message current backoff
			if msgRecord.epochCount == msgRecord.nextTransmitEpoch {
				if msgRecord.currentBackoff == 0 {
					msgRecord.currentBackoff += 1
				} else {
					msgRecord.currentBackoff *= 2
				}
				if msgRecord.currentBackoff > s.params.MaxBackOffInterval {
					msgRecord.currentBackoff = s.params.MaxBackOffInterval
				}
				msgRecord.nextTransmitEpoch = msgRecord.nextTransmitEpoch + msgRecord.currentBackoff + 1
				s.sendMsgToClient(msgRecord.msg)
				cli.lastSentEpoch = s.epochCount
			}
		}
	}
}

// Method to handle processing pending client outgoing messages
// in case of a Ack or a CAck
func (s *server) processPendingMessageAck(cli *clientInfo) {
	// check if all msgs in the window was sent and acked
	allProcessed := true

	// looping over all messages to find a msg
	// which is not yet sent or acked
	for idx, msgRecord := range cli.outgoingMsgsList {
		if !msgRecord.ack || !msgRecord.sent {
			cli.outgoingMsgsList = cli.outgoingMsgsList[idx:]
			allProcessed = false
			break
		}
	}
	// if all msgs in the window processed and
	// we couldn't find a message over the window
	// then empty out outgoing list and return
	// LSN would be updated when new outgoing msg would be
	// put
	if allProcessed {
		cli.outgoingMsgsList = cli.outgoingMsgsList[:0]
		return
	}

	// calculate window send limit
	cli.lowestUnAckSN = cli.outgoingMsgsList[0].msg.msg.SeqNum
	cli.windowLimit = cli.lowestUnAckSN + s.params.WindowSize

	// till window send limit send msgs till Unacked <= MaxUnacked
	for _, msgRecord := range cli.outgoingMsgsList {
		// if we reach window limit break
		if msgRecord.msg.msg.SeqNum >= cli.windowLimit {
			break
		}

		// if message not sent in the window send it
		if !msgRecord.sent {
			s.sendMsgToClient(msgRecord.msg)
			msgRecord.sent = true
			msgRecord.epochCount = s.epochCount
			msgRecord.nextTransmitEpoch = s.epochCount + 1

			// update last sent epoch for client
			cli.lastSentEpoch = s.epochCount
			// increment cli unAckMsg counter
			cli.unAckMsgCntr += 1

			// if max unAckMsg reached break
			if cli.unAckMsgCntr == s.params.MaxUnackedMessages {
				break
			}
		}
	}
}

// Go routine to store the processed messages to be accessed
// by the server read operation
func (s *server) msgStoreRoutine() {
	for {
		select {
		// return if signal to close the routine received
		case <-s.closeMsgStoreRoutine:
			close(s.serverAppMsgReadResp)
			return

		// stores the processed message in a queue in-order fashion
		case req := <-s.msgStoreReq:
			s.msgsUnread.PushBack(req)

		// serve the read operation request from the server application
		case <-s.serverAppMsgReadReq:
			// if messages are present in the queue, then return the head
			if s.msgsUnread.Len() > 0 {
				head := s.msgsUnread.Front()
				s.msgsUnread.Remove(head)
				s.serverAppMsgReadResp <- head.Value.(*clientMsg)
			} else {
				// else block again till any message recieved
				// or in between any server close call happens
				select {
				// return if signal to close the server close occurs
				case <-s.closeMsgStoreRoutine:
					close(s.serverAppMsgReadResp)
					return
				// wait for a new processed message to be receievd otherwise
				case req := <-s.msgStoreReq:
					s.serverAppMsgReadResp <- req
				}

			}
		}
	}
}

// Method to send message to the client over UDP conenction
func (s *server) sendMsgToClient(req *clientMsg) {
	payload, err := json.Marshal(req.msg)
	if err != nil {
		return
	}
	_, err = s.udpConn.WriteToUDP(payload, req.srcAddr)
	if err != nil {
		return
	}
}

// Method to process the Server application read requests
func (s *server) Read() (int, []byte, error) {
	s.serverAppMsgReadReq <- true
	message, ok := <-s.serverAppMsgReadResp
	if !ok {
		return 0, nil, errors.New("server is closed")
	}
	if message.msg.Payload != nil {
		return message.msg.ConnID, message.msg.Payload, nil
	}
	// in case we receive a dummpy struct indicatini closign of client
	return message.msg.ConnID, nil, errors.New("client connection is closed")
}

// Method to process the Server application write requests to a client
func (s *server) Write(connId int, payload []byte) error {
	s.serverAppMsgWriteReq <- &clientMsg{
		msg:     *NewData(connId, -1, len(payload), payload, 0),
		srcAddr: nil,
	}
	resp := <-s.serverAppMsgWriteResp
	return resp
}

// Method to process request to close a client conenction
func (s *server) CloseConn(connId int) error {
	s.clientCloseReq <- connId
	resp := <-s.clientCloseResp
	return resp
}

// Method to process the request to close the server
func (s *server) Close() error {
	s.serverCloseReq <- true
	return <-s.serverCloseResp
}

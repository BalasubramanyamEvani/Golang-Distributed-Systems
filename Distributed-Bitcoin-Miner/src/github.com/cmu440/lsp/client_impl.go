// Contains the implementation of LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"time"

	"github.com/cmu440/lspnet"
)

// Status of each data message sent from client to server
type messageRecord struct {
	message           *Message
	sent              bool
	acked             bool
	epochCount        int
	currentBackoff    int
	nextTransmitEpoch int
}

// Client Struct containing attributes of the client and
// channels used for communication between the go routines
type client struct {
	// connection status fields
	connection      *lspnet.UDPConn
	connID          int
	connEstablished chan bool
	// channels to request and fetch conn ID
	connReq  chan bool
	connResp chan int
	// initial sequence number sent by client
	sendingSeqNum int
	// initial sequence number expected by client
	receivingSeqNum int
	// channel to process message sent by server
	serverMsg chan Message
	// channels to request and read from the processed messages
	readReq  chan bool
	readResp chan Message
	// channel to store read messages
	msgStoreReq chan Message
	// channels to request and fetch response of a write to client
	writeReq  chan []byte
	writeResp chan error
	// channels to handle closure of go routines
	readHandlerClose     chan bool
	msgStoreHandlerClose chan bool
	// buffer storing processed read messages
	readMsgList *list.List
	// map storing out-of-order read messages
	readMsgMap map[int]Message
	// channels to request and respond to a close request
	closeReq  chan bool
	closeResp chan error
	// boolean indicating if close has been requested
	closeInit bool
	// boolean indicating if epoch limit has been reached
	epochLimitReached bool
	// slice containing pending (not sent or acked or both) message records
	messageRecordList []*messageRecord
	// current epoch
	epochCount int
	// epoch when a message was received from the server
	lastLiveEpoch int
	// epoch when a message was last sent from the client
	lastSentEpoch int
	// unacked message counter
	unAckedMessagesCount int
	// lowest sequence number in the pending message list
	lowestSeqNum int
	// window limit in the pending message list
	windowLimit int
	// epoch and sliding window related params
	params Params
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(
	hostport string, initialSeqNum int, params *Params) (Client, error) {
	// resolve the server address and connect to it
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udpConn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	connectionMessage := NewConnect(initialSeqNum)

	client := &client{
		connection:           udpConn,
		connID:               -1,
		connEstablished:      make(chan bool),
		connReq:              make(chan bool),
		connResp:             make(chan int),
		sendingSeqNum:        initialSeqNum,
		receivingSeqNum:      1,
		serverMsg:            make(chan Message),
		readReq:              make(chan bool),
		readResp:             make(chan Message),
		msgStoreReq:          make(chan Message),
		writeReq:             make(chan []byte),
		writeResp:            make(chan error),
		readHandlerClose:     make(chan bool),
		msgStoreHandlerClose: make(chan bool),
		readMsgList:          list.New(),
		readMsgMap:           make(map[int]Message),
		closeReq:             make(chan bool),
		closeResp:            make(chan error),
		closeInit:            false,
		epochLimitReached:    false,
		messageRecordList:    make([]*messageRecord, 0),
		lowestSeqNum:         initialSeqNum,
		windowLimit:          params.WindowSize,
		params:               *params,
	}

	// send a connection request message
	err = client.writeMessage(connectionMessage)
	if err != nil {
		return nil, err
	}

	// instantiate go routine for main handler,
	// read handler and message store handler
	go client.mainHandler()
	go client.readHandler()
	go client.msgStoreHandler()

	connEstablished := <-client.connEstablished
	if connEstablished {
		return client, nil
	}
	return client, errors.New("failed to establish connection")
}

// Go routine to handle incoming read requests,
// outgoing write requests and epoch functionality
func (c *client) mainHandler() {
	for {
		select {
		// handle connID fetch request
		case <-c.connReq:
			c.connResp <- c.connID
		// handle incoming read messages from the UDP connection
		case message := <-c.serverMsg:
			c.incomingMessageHandler(&message)
		// handle outgoing write messages
		case payload := <-c.writeReq:
			c.outgoingMessageHandler(payload)
		// handle the epoch events
		case <-time.Tick(time.Duration(c.params.EpochMillis) * time.Millisecond):
			c.epochCount++
			// check if epoch limit is not reached
			if !c.epochLimitReached {
				// If all the pending messages have been sent and acked
				// close the connection
				if c.closeInit && len(c.messageRecordList) == 0 {
					c.closeConnection()
					c.closeResp <- nil
					return
				}

				// if epoch limit is reached close the connection
				if c.epochCount-c.lastLiveEpoch > c.params.EpochLimit {
					c.epochLimitReached = true
					c.closeConnection()
					if c.closeInit {
						c.closeResp <- errors.New("epoch limit reached, " +
							"dropped the connection")
						return
					}
					continue
				}

				// retransmit connection request if not acked yet
				if c.connID < 0 {
					connectionRequest := NewConnect(c.sendingSeqNum)
					err := c.writeMessage(connectionRequest)
					if err != nil {
						continue
					}
					c.lastSentEpoch = c.epochCount
					continue
				}

				// process the pending messages
				c.processPendingMessageBackoff()

				// send heartbeat if no message has been sent in the
				// previous epoch
				if !c.closeInit && c.lastSentEpoch < c.epochCount-1 {
					heartBeat := NewAck(c.connID, 0)
					err := c.writeMessage(heartBeat)
					if err != nil {
						continue
					}
					c.lastSentEpoch = c.epochCount
				}
			}
		// handle connection close request
		case <-c.closeReq:
			// if epoch limit is already reached return the error response
			if c.epochLimitReached {
				c.closeResp <- errors.New("epoch limit reached, " +
					"dropped the connection")
				return
			}
			c.closeInit = true
			// if there are pending messages, process the close request
			// in the next epoch
			if len(c.messageRecordList) > 0 {
				continue
			}
			c.closeConnection()
			c.closeResp <- nil
			return
		}
	}
}

// Method handling the Data, Ack and Cack messages from the server
func (c *client) incomingMessageHandler(message *Message) {
	// update last live epoch of the server
	c.lastLiveEpoch = c.epochCount
	switch message.Type {
	case MsgAck:
		// connection request ack
		if c.connID < 0 && message.SeqNum == c.sendingSeqNum {
			c.connID = message.ConnID
			c.connEstablished <- true
			return
		}
		// heartbeat ack
		if message.SeqNum == 0 {
			return
		}
		// data message ack
		messageIndex := message.SeqNum - c.lowestSeqNum
		// received ack for a message that is not in the pending message list
		if len(c.messageRecordList) < messageIndex ||
			messageIndex < 0 ||
			c.messageRecordList[messageIndex].acked {
			return
		}
		// update the ack status of the pending message and
		// accordingly the unacked message counter
		c.messageRecordList[messageIndex].acked = true
		c.unAckedMessagesCount--

		c.processPendingMessageAck()

	case MsgCAck:
		// data message cack
		// update the ack status of all pending messages whose
		// sequence number is lower than the message sequence number
		messageIndex := message.SeqNum - c.lowestSeqNum
		for i := 0; i <= messageIndex; i++ {
			if !c.messageRecordList[i].acked {
				c.messageRecordList[i].acked = true
				c.unAckedMessagesCount--
			}
		}
		c.processPendingMessageAck()

	case MsgData:
		// return if checksum calculated doesn't match the
		// checksum in the message
		expectedChecksum := CalculateChecksum(
			message.ConnID, message.SeqNum, message.Size, message.Payload)
		if expectedChecksum != message.Checksum {
			return
		}

		// send ack message back
		ackMessage := NewAck(c.connID, message.SeqNum)
		err := c.writeMessage(ackMessage)
		if err != nil {
			return
		}
		c.lastSentEpoch = c.epochCount

		// return if message is already processed
		if message.SeqNum < c.receivingSeqNum {
			return
		}

		// if expected sequence number is received start adding to the
		// message store (so that they can be accessed by read calls)
		// else put the message in the map containing out-of-order messages
		if message.SeqNum == c.receivingSeqNum {
			c.msgStoreReq <- *message
			c.receivingSeqNum++
			for {
				readMessage, ok := c.readMsgMap[c.receivingSeqNum]
				if ok {
					c.msgStoreReq <- readMessage
					delete(c.readMsgMap, c.receivingSeqNum)
					c.receivingSeqNum++
				} else {
					break
				}
			}
		} else {
			c.readMsgMap[message.SeqNum] = *message
		}
	}
}

// Method to handle acknowledgement of messages in the pending
// message list
func (c *client) processPendingMessageAck() {
	// remove message records from the head if sent and acked
	allMessagesAcked := true
	for index, messageRecord := range c.messageRecordList {
		if !(messageRecord.sent && messageRecord.acked) {
			c.messageRecordList = c.messageRecordList[index:]
			allMessagesAcked = false
			break
		}
	}
	// if all messages in the pending message list are acked, reset it
	if allMessagesAcked {
		c.messageRecordList = c.messageRecordList[:0]
		return
	}
	// update the lowest sequence number and window limit
	c.lowestSeqNum = c.messageRecordList[0].message.SeqNum
	c.windowLimit = c.lowestSeqNum + c.params.WindowSize - 1
	// send messages within window limit
	for _, messageRecord := range c.messageRecordList {
		if messageRecord.message.SeqNum <= c.windowLimit &&
			c.unAckedMessagesCount < c.params.MaxUnackedMessages {
			if !messageRecord.sent {
				c.writeDataMessage(messageRecord)
			}
		}
	}
}

// Method to process pending messages based on their backoff status
func (c *client) processPendingMessageBackoff() {
	// iterate over the pending messages list
	for _, record := range c.messageRecordList {
		// check if the message has been sent before and has not
		// been acked yet (only try to retransmit in this case)
		if record.sent && !record.acked {
			record.epochCount = c.epochCount
			// check if the message can be retransmitted in this epoch
			if record.epochCount == record.nextTransmitEpoch {
				// update current back off and next transmit epoch
				// of the message
				if record.currentBackoff == 0 {
					record.currentBackoff++
				} else {
					record.currentBackoff =
						record.currentBackoff * 2
				}
				if record.currentBackoff > c.params.MaxBackOffInterval {
					record.currentBackoff = c.params.MaxBackOffInterval
				}
				record.nextTransmitEpoch = record.nextTransmitEpoch +
					record.currentBackoff + 1
				c.lastSentEpoch = c.epochCount
				err := c.writeMessage(record.message)
				if err != nil {
					continue
				}
			}
		}
	}
}

// Method to handle processing of outgoing messages
func (c *client) outgoingMessageHandler(payload []byte) {
	// if connection closure is initiated or epoch limit is reached
	// return error
	if c.closeInit || c.epochLimitReached {
		c.writeResp <- errors.New("epoch limit reached, dropped the connection")
		return
	}
	// return error if write is requested before the connection is established
	if c.connID < 0 {
		c.writeResp <- errors.New("connection not established yet")
		return
	}
	// construct the data message to be sent
	c.sendingSeqNum++
	checksum := CalculateChecksum(
		c.connID, c.sendingSeqNum, len(payload), payload)
	dataMessage := NewData(
		c.connID, c.sendingSeqNum, len(payload), payload, checksum)
	messageRecord := &messageRecord{
		message: dataMessage,
	}
	// append the data message to the pending message list
	c.messageRecordList = append(c.messageRecordList, messageRecord)

	// update the lowest sequence number if this is the
	// only message in the message record list
	if len(c.messageRecordList) == 1 {
		c.lowestSeqNum = c.sendingSeqNum
		c.windowLimit = c.lowestSeqNum + c.params.WindowSize - 1
	}

	// send the message if its within window limit and
	// number of unacked messages is less than max unacked messages
	if c.sendingSeqNum <= c.windowLimit &&
		c.unAckedMessagesCount < c.params.MaxUnackedMessages {
		c.writeDataMessage(messageRecord)
	}
	// return response to the write function call
	c.writeResp <- nil
}

// Method to write data message to the server
func (c *client) writeDataMessage(record *messageRecord) {
	record.sent = true
	record.epochCount = c.epochCount
	record.nextTransmitEpoch = c.epochCount + 1
	c.unAckedMessagesCount++
	c.lastSentEpoch = c.epochCount
	err := c.writeMessage(record.message)
	if err != nil {
		return
	}
}

// Method to sent message to the server over the UDP connection
func (c *client) writeMessage(msg *Message) error {
	buffer, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = c.connection.Write(buffer)
	if err != nil {
		return err
	}
	return nil
}

// Method to handle connection and the go routines closure
func (c *client) closeConnection() {
	// close the UDP connection and signal the go routines to return
	err := c.connection.Close()
	if err != nil {
		return
	}
	c.readHandlerClose <- true
	c.msgStoreHandlerClose <- true
}

// Go routine to read from the UDP connection buffer
func (c *client) readHandler() {
	buffer := make([]byte, PayloadSize)
	var message Message
	for {
		select {
		// return if a signal to close the routine is received
		case <-c.readHandlerClose:
			return
		default:
			n, err := c.connection.Read(buffer)
			if err != nil {
				continue
			}
			err = json.Unmarshal(buffer[0:n], &message)
			if err != nil {
				continue
			}
			// Check if the payload is malformed, return
			if message.Size > len(message.Payload) {
				continue
			} else if message.Size <= len(message.Payload) {
				message.Payload = message.Payload[:message.Size]
			}
			select {
			// send the server message to the main handler
			case c.serverMsg <- message:
				continue
			// return if a signal to close the routine is received
			case <-c.readHandlerClose:
				return
			}
		}
	}
}

// Go routine to store the processed messages to be accessed
// by the read function calls from the client application
func (c *client) msgStoreHandler() {
	for {
		select {
		// append to the processed message list if an in-order
		// message is received
		case req := <-c.msgStoreReq:
			c.readMsgList.PushBack(req)
		// serve the read request from the client application
		case <-c.readReq:
			// if there are messages in the processed message list
			// fetch and remove from the list
			if c.readMsgList.Len() > 0 {
				head := c.readMsgList.Front()
				c.readMsgList.Remove(head)
				c.readResp <- head.Value.(Message)
			} else {
				select {
				// wait for a processed message to be sent to the
				// message store
				case req := <-c.msgStoreReq:
					c.readResp <- req
				// return if a signal to close the routine is received
				case <-c.msgStoreHandlerClose:
					close(c.readResp)
					return
				}
			}
		// return if a signal to close the routine is received
		case <-c.msgStoreHandlerClose:
			close(c.readResp)
			return
		}
	}
}

// Method to fetch the ID of the connection with the server
func (c *client) ConnID() int {
	c.connReq <- true
	return <-c.connResp
}

// Method to process the read requests from the client application
func (c *client) Read() ([]byte, error) {
	c.readReq <- true
	message, ok := <-c.readResp
	if !ok {
		return nil, errors.New("the connection to server is closed")
	}
	return message.Payload, nil
}

// Method to process the write requests from the client application
func (c *client) Write(payload []byte) error {
	c.writeReq <- payload
	return <-c.writeResp
}

// Method to handle the client close request
func (c *client) Close() error {
	c.closeReq <- true
	return <-c.closeResp
}

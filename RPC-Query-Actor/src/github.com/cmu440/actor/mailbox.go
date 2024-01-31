package actor

import (
	"sync"
)

// A mailbox, i.e., a thread-safe unbounded FIFO queue.
//
// You can think of Mailbox like a Go channel with an infinite buffer.
//
// Mailbox is only exported outside of the actor package for use in tests;
// we do not expect you to use it, just implement it.
type Mailbox struct {
	messages []any      // stores messages
	mutex    sync.Mutex // mutex lock to protect this resource
	cond     *sync.Cond // condition variable to signal when queue is not empty
	closed   bool       // whether this queue is closed for operation or not
	size     int        // size of the queue
}

// Returns a new mailbox that is ready for use.
func NewMailbox() *Mailbox {
	newMailbox := &Mailbox{
		messages: make([]any, 0),
		size:     0,
	}
	newMailbox.cond = sync.NewCond(&newMailbox.mutex)
	return newMailbox
}

// Pushes message onto the end of the mailbox's FIFO queue.
//
// This function should NOT block.
//
// If mailbox.Close() has already been called, this may ignore
// the message. It still should NOT block.
//
// Note: message is not a literal actor message; it is an ActorSystem
// wrapper around a marshalled actor message.
func (mailbox *Mailbox) Push(message any) {
	mailbox.mutex.Lock()
	defer mailbox.mutex.Unlock()

	if mailbox.closed {
		return
	}
	mailbox.messages = append(mailbox.messages, message)
	mailbox.size += 1
	mailbox.cond.Signal()
}

// Pops a message from the front of the mailbox's FIFO queue,
// blocking until a message is available.
//
// If mailbox.Close() is called (either before or during a Pop() call),
// this should unblock and return (nil, false). Otherwise, it should return
// (message, true).
func (mailbox *Mailbox) Pop() (message any, ok bool) {
	mailbox.mutex.Lock()
	defer mailbox.mutex.Unlock()

	for mailbox.size == 0 && !mailbox.closed {
		mailbox.cond.Wait()
	}

	if mailbox.closed {
		return nil, false
	}
	messageAtHead := mailbox.messages[0]
	mailbox.messages = mailbox.messages[1:]
	mailbox.size -= 1
	return messageAtHead, true
}

// returns the size of the queue
// useful for debugging
func (mailbox *Mailbox) Size() int {
	mailbox.mutex.Lock()
	defer mailbox.mutex.Unlock()

	return mailbox.size
}

// Closes the mailbox, causing future Pop() calls to return (nil, false)
// and terminating any goroutines running in the background.
//
// If Close() has already been called, this may exhibit undefined behavior,
// including blocking indefinitely.
func (mailbox *Mailbox) Close() {
	mailbox.mutex.Lock()
	defer mailbox.mutex.Unlock()

	mailbox.closed = true
	mailbox.cond.Broadcast()
}

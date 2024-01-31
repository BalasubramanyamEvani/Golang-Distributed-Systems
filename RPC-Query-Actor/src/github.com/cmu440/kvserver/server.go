// Package kvserver implements the backend server for a
// geographically distributed, highly available, NoSQL key-value store.
package kvserver

import (
	"net"
	"net/rpc"
	"strconv"
	"strings"

	"github.com/cmu440/actor"
)

type listener struct {
	ln        net.Listener
	rpcServer *rpc.Server
}

// A single server in the key-value store, running some number of
// query actors - nominally one per CPU core. Each query actor
// provides a key/value storage service on its own port.
//
// Different query actors (both within this server and across connected
// servers) periodically sync updates (Puts) following an eventually
// consistent, last-writer-wins strategy.
type Server struct {
	actorSystemRef *actor.ActorSystem
	listeners      []*listener
	queryReceivers []*queryReceiver
}

// OPTIONAL: Error handler for ActorSystem.OnError.
//
// Print the error or call debug.PrintStack() in this function.
// When starting an ActorSystem, call ActorSystem.OnError(errorHandler).
// This can help debug server-side errors more easily.
func errorHandler(err error) {
}

// Starts a server running queryActorCount query actors.
//
// The server's actor system listens for remote messages (from other actor
// systems) on startPort. The server listens for RPCs from kvclient.Clients
// on ports [startPort + 1, startPort + 2, ..., startPort + queryActorCount].
// Each of these "query RPC servers" answers queries by asking a specific
// query actor.
//
// remoteDescs contains a "description" string for each existing server in the
// key-value store. Specifically, each slice entry is the desc returned by
// an existing server's own NewServer call. The description strings are opaque
// to callers, but typically an implementation uses JSON-encoded data containing,
// e.g., actor.ActorRef's that remote servers' actors should contact.
//
// Before returning, NewServer starts the ActorSystem, all query actors, and
// all query RPC servers. If there is an error starting anything, that error is
// returned instead.
// Tips:
// - The "HTTP service" example in the net/rpc docs does not support
// multiple RPC servers in the same process. Instead, use the following
// template to start RPC servers (adapted from
// https://groups.google.com/g/Golang-Nuts/c/JTn3LV_bd5M/m/cMO_DLyHPeUJ ):
//
//	rpcServer := rpc.NewServer()
//	err := rpcServer.RegisterName("QueryReceiver", [*queryReceiver instance])
//	ln, err := net.Listen("tcp", ...)
//	go func() {
//	  for {
//	    conn, err := ln.Accept()
//	    if err != nil {
//	      return
//	    }
//	    go rpcServer.ServeConn(conn)
//	  }
//	}()
//
// - To start query actors, call your ActorSystem's
// StartActor(newQueryActor), where newQueryActor is defined in ./query_actor.go.
// Do this queryActorCount times. (For the checkpoint tests,
// queryActorCount will always be 1.)
// - remoteDescs and desc: see doc comment above.
// For the checkpoint, it is okay to ignore remoteDescs and return "" for desc.
func NewServer(startPort int, queryActorCount int, remoteDescs []string) (server *Server, desc string, err error) {
	// starts a new actor system on the start port
	actorSystem, err := actor.NewActorSystem(startPort)
	if err != nil {
		return nil, "", err
	}
	// initialize the server
	newServer := &Server{
		actorSystemRef: actorSystem,
		listeners:      make([]*listener, 0),
		queryReceivers: make([]*queryReceiver, 0),
	}

	descArr := make([]string, 0)
	actorRefs := make([]*actor.ActorRef, 0)

	// starts queryActorCount amount of query actors
	for i := 1; i <= queryActorCount; i++ {
		// starts a new actor
		actorRef := actorSystem.StartActor(newQueryActor)
		// passes this actor ref to query receiver
		queryReceiverInstance := &queryReceiver{
			actorSystemRef: actorSystem,
			actorRef:       actorRef,
		}
		rpcServer := rpc.NewServer()
		err := rpcServer.RegisterName("QueryReceiver", queryReceiverInstance)
		if err != nil {
			newServer.Close()
			return nil, "", err
		}
		// start listening
		address := strings.Split(actorRef.Address, ":")[0] + ":" + strconv.Itoa(startPort+i)
		ln, err := net.Listen("tcp", address)
		if err != nil {
			newServer.Close()
			return nil, "", err
		}
		newListener := listener{
			ln:        ln,
			rpcServer: rpcServer,
		}
		go newListener.startListening()

		// put this information inside server
		newServer.queryReceivers = append(newServer.queryReceivers, queryReceiverInstance)
		newServer.listeners = append(newServer.listeners, &newListener)

		descArr = append(descArr, queryReceiverInstance.actorRef.Uid())
		actorRefs = append(actorRefs, queryReceiverInstance.actorRef)
	}
	// send actor refs to all actors
	for _, qReceiver := range newServer.queryReceivers {
		sendActorRefInfo(qReceiver, actorRefs)
	}
	desc = strings.Join(descArr, ",")
	return newServer, desc, nil
}

func sendActorRefInfo(rcvr *queryReceiver, actorRefs []*actor.ActorRef) {
	synActorChanRef, respCh := rcvr.actorSystemRef.NewChannelRef()
	rcvr.actorSystemRef.Tell(rcvr.actorRef, MActorRef{Sender: synActorChanRef, Actors: actorRefs})
	_ = (<-respCh).(MActorRefResult)
}

func (listener *listener) startListening() {
	for {
		conn, err := listener.ln.Accept()
		if err != nil {
			return
		}
		go listener.rpcServer.ServeConn(conn)
	}
}

// OPTIONAL: Closes the server, including its actor system
// and all RPC servers.
//
// You are not required to implement this function for full credit; the tests end
// by calling Close but do not check that it does anything. However, you
// may find it useful to implement this so that you can run multiple/repeated
// tests in the same "go test" command without cross-test interference (in
// particular, old test servers' squatting on ports.)
//
// Likewise, you may find it useful to close a partially-started server's
// resources if there is an error in NewServer.
func (server *Server) Close() {
	for _, listener := range server.listeners {
		listener.ln.Close()
	}
}

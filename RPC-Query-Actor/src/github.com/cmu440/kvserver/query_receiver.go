package kvserver

import (
	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// RPC handler implementing the kvcommon.QueryReceiver interface.
// There is one queryReceiver per queryActor, each running on its own port,
// created and registered for RPCs in NewServer.
//
// A queryReceiver MUST answer RPCs by sending a message to its query
// actor and getting a response message from that query actor (via
// ActorSystem's NewChannelRef). It must NOT attempt to answer queries
// using its own state, and it must NOT directly coordinate with other
// queryReceivers - all coordination is done within the actor system
// by its query actor.
type queryReceiver struct {
	actorSystemRef *actor.ActorSystem
	actorRef       *actor.ActorRef
}

// Get implements kvcommon.QueryReceiver.Get.
func (rcvr *queryReceiver) Get(args kvcommon.GetArgs, reply *kvcommon.GetReply) error {
	// Asking the actor for its value
	synActorChanRef, respCh := rcvr.actorSystemRef.NewChannelRef()
	rcvr.actorSystemRef.Tell(rcvr.actorRef, MGet{Sender: synActorChanRef, Key: args.Key})
	resp := (<-respCh).(MGetResult)
	reply.Value = resp.Value
	reply.Ok = resp.Ok
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	// Asking the actor to list key-value with prefix key
	synActorChanRef, respCh := rcvr.actorSystemRef.NewChannelRef()
	rcvr.actorSystemRef.Tell(rcvr.actorRef, MList{Sender: synActorChanRef, Prefix: args.Prefix})
	reply.Entries = (<-respCh).(MListResult).Result
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	// Asking the actor to store key-value
	synActorChanRef, respCh := rcvr.actorSystemRef.NewChannelRef()
	rcvr.actorSystemRef.Tell(rcvr.actorRef, MPut{Sender: synActorChanRef, Key: args.Key, Value: args.Value})
	_ = (<-respCh).(MPutResult).Ok
	return nil
}

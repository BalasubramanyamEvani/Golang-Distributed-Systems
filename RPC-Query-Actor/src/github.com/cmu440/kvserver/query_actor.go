package kvserver

import (
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/cmu440/actor"
)

const (
	SyncDuration = 100
)

// Implement your queryActor in this file.
// See example/counter_actor.go for an example actor using the
// github.com/cmu440/actor package.

// defining message types as structs

// Get Message
type MGet struct {
	Sender *actor.ActorRef
	Key    string
}

type MGetResult struct {
	Value string
	Ok    bool
}

// Put Message
type MPut struct {
	Sender *actor.ActorRef
	Key    string
	Value  string
}

type MPutResult struct {
	Ok bool
}

// List Message
type MList struct {
	Sender *actor.ActorRef
	Prefix string
}

type MListResult struct {
	Result map[string]string
}

// recieved puts messages buffer struct
type PutMsgItem struct {
	Key   string
	Value string
	Time  int64
}

// for syncing
type MSyncPutMsgs struct {
	PutMsgs []PutMsgItem
	Uid     string
}

// actor refs message
type MActorRef struct {
	Sender *actor.ActorRef
	Actors []*actor.ActorRef
}

type MActorRefResult struct {
	Ok bool
}

type MSyncTimer struct {
}

func init() {
	// Register Message Types for marshalling and unmarshalling
	gob.Register(MGet{})
	gob.Register(MGetResult{})
	gob.Register(MPut{})
	gob.Register(MPutResult{})
	gob.Register(MList{})
	gob.Register(MListResult{})
	gob.Register(MActorRef{})
	gob.Register(MActorRefResult{})
	gob.Register(MSyncTimer{})
	gob.Register(MSyncPutMsgs{})
}

type kvStoreValue struct {
	value string
	time  int64
}

type queryActor struct {
	context            *actor.ActorContext     // context of actor system
	kvstore            map[string]kvStoreValue // map specific to an actor
	putsMessagesBuffer []PutMsgItem            // received puts messages
	actors             []*actor.ActorRef
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	newQueryActor := &queryActor{
		context:            context,
		kvstore:            make(map[string]kvStoreValue),
		putsMessagesBuffer: make([]PutMsgItem, 0),
	}
	// tell after sync duration
	// will be reset when this message gets processed
	newQueryActor.context.TellAfter(newQueryActor.context.Self, MSyncTimer{}, time.Duration(SyncDuration)*time.Millisecond)
	return newQueryActor
}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	switch msg := message.(type) {
	case MGet:
		value, ok := actor.kvstore[msg.Key]
		actor.context.Tell(msg.Sender, MGetResult{Value: value.value, Ok: ok})

	case MPut:
		currTime := time.Now().UnixMilli()
		actor.kvstore[msg.Key] = kvStoreValue{value: msg.Value, time: currTime}
		actor.putsMessagesBuffer = append(actor.putsMessagesBuffer, PutMsgItem{
			Key:   msg.Key,
			Value: msg.Value,
			Time:  currTime,
		})

		actor.context.Tell(msg.Sender, MPutResult{Ok: true})

	case MList:
		result := actor.fetchKeysAndValueWithPrefix(msg.Prefix)
		actor.context.Tell(msg.Sender, MListResult{Result: result})

	case MActorRef:
		actor.actors = msg.Actors
		actor.context.Tell(msg.Sender, MActorRefResult{Ok: true})

	case MSyncTimer:
		if len(actor.putsMessagesBuffer) > 0 {
			for _, actorRef := range actor.actors {
				if actorRef.Counter != actor.context.Self.Counter {
					actor.context.Tell(actorRef, MSyncPutMsgs{
						PutMsgs: actor.putsMessagesBuffer,
						Uid:     actor.context.Self.Uid(),
					})
				}
			}
			// empty it out
			actor.putsMessagesBuffer = actor.putsMessagesBuffer[:0]
		}
		// resync
		actor.context.TellAfter(actor.context.Self, MSyncTimer{}, time.Duration(SyncDuration)*time.Millisecond)

	case MSyncPutMsgs:
		for _, putMsg := range msg.PutMsgs {
			value, ok := actor.kvstore[putMsg.Key]
			if !ok || putMsg.Time > value.time || (putMsg.Time == value.time && msg.Uid > actor.context.Self.Uid()) {
				actor.kvstore[putMsg.Key] = kvStoreValue{
					value: putMsg.Value,
					time:  putMsg.Time,
				}
			}
		}

	default:
		return fmt.Errorf("Unexpected query actor message type: %T", msg)
	}
	return nil
}

func (actor *queryActor) fetchKeysAndValueWithPrefix(prefix string) map[string]string {
	resultMap := make(map[string]string)
	for key, value := range actor.kvstore {
		if strings.HasPrefix(key, prefix) {
			resultMap[key] = value.value
		}
	}
	return resultMap
}

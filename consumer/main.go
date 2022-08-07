package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/dranikpg/gtrs"
	"github.com/go-redis/redis/v8"
)

// Our type that is sent in the stream.
type Event struct {
	Kind     string
	Priority int
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx, cancelFunc := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancelFunc()

	// Create three consumers
	// - one on streams main-stream and other-stream
	// - two on group g1 of stream group-stream
	mainStream := gtrs.NewConsumer[Event](ctx, rdb,
		gtrs.StreamIDs{"main-stream": "0-0", "other-stream": "0-0"},
	)
	groupC1 := gtrs.NewGroupConsumer[Event](ctx, rdb, "g1", "c1", "group-stream", "0-0")
	groupC2 := gtrs.NewGroupConsumer[Event](ctx, rdb, "g1", "c2", "group-stream", "0-0")

	// Do some recovery when we exit.
	defer func() {
		// Lets see what acknowledgements were not delivered
		remC1 := groupC1.Close()
		remC2 := groupC2.Close()
		fmt.Println("Those acks were not sent", remC1, remC2)

		// Lets see where we stopped reading "main-stream" and "other-stream"
		seenIds := mainStream.Close()
		fmt.Println("Main stream reader stopped on", seenIds)
	}()

	for {
		var msg gtrs.Message[Event]                    // our message
		var ackTarget *gtrs.GroupConsumer[Event] = nil // who to send the confimration

		select {
		// Consumers just close the stream on close or cancellation without
		// sending any cancellation errors.
		// So lets not forget checking the context ourselves
		case <-ctx.Done():
			return
		// Block simultaneously on all consumers and wait for first to respond
		case msg = <-mainStream.Chan():
		case msg = <-groupC1.Chan():
			ackTarget = groupC1
		case msg = <-groupC2.Chan():
			ackTarget = groupC2
		}

		switch errv := msg.Err.(type) {
		// This interface-nil comparison in safe. Consumers never return typed nil errors.
		case nil:
			fmt.Printf("Got event %v: %v, priority %v, from %v\n",
				msg.ID, msg.Data.Kind, msg.Data.Priority, msg.Stream)

			// Ack blocks only if the inner ackBuffer is full.
			// Use it only inside the loop or from another goroutine with continous error processing.
			if ackTarget != nil {
				ackTarget.Ack(msg)
			}

			// Lets sometimes send an acknowledgement to the wrong stream.
			// Just for demonstration purposes to get a real AckError
			if ackTarget == nil && rand.Float32() < 0.1 {
				fmt.Println("Sending bad ack :)")
				groupC1.Ack(msg)
			}
		case gtrs.ReadError:
			// One of the consumers will stop. So lets stop altogether.
			fmt.Fprintf(os.Stderr, "Read error! %v Exiting...\n", msg.Err)
			return
		case gtrs.AckError:
			// We can identify the failed ack by stream & id
			fmt.Printf("Ack failed %v-%v :( \n", msg.Stream, msg.ID)
		case gtrs.ParseError:
			// We can do something useful with errv.Data
			fmt.Println("Parse failed: raw data: ", errv.Data)
		}
	}
}

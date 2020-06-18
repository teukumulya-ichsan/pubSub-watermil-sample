package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func main() {

	// define a channel pubsub with no debug and no trace
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		watermill.NewStdLogger(false, false),
	)

	// receive/subscribe message to process from publisher
	messages, err := pubSub.Subscribe(context.Background(), "topic-name")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publishMessages(pubSub)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish("topic-name", msg); err != nil {
			panic(err)
		}

		// the pubsub will scheduling per hour
		time.Sleep(time.Hour)
	}
}

// chanelling process
func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("Received message : %s , payload: %s", msg.UUID, string(msg.Payload))

		// need to set as ack that because we receive and processing message
		// If not, it will be sent repeatedly.
		msg.Ack()
	}
}

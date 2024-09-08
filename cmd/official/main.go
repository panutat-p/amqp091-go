package main

import (
	"log"
	"rabbitmq-go/official"
)

// This exports a Client object that wraps this library. It
// automatically reconnects when the connection fails, and
// blocks all pushes until the connection succeeds. It also
// confirms every outgoing message, so none are lost.
// It doesn't automatically ack each message, but leaves that
// to the parent process, since it is usage-dependent.
//
// Try running this in one terminal, and rabbitmq-server in another.
// Stop & restart RabbitMQ to see how the queue reacts.

const (
	RABBITMQ_DSN  = "amqp://guest:guest@localhost:5672/"
	RABBIMQ_QUEUE = "official-001"
)

func main() {
	publishDone := make(chan struct{})
	consumeDone := make(chan struct{})

	go official.Publish(publishDone, RABBITMQ_DSN, RABBIMQ_QUEUE)
	go official.Consume(consumeDone, RABBITMQ_DSN, RABBIMQ_QUEUE)

	select {
	case <-publishDone:
		log.Println("💤 publishing is done")
	}

	select {
	case <-consumeDone:
		log.Println("💤 consuming is done")
	}

	log.Println("💤 Shutdown")
}

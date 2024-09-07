package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rabbitmq-go/internal"
)

const (
	RABBITMQ_DSN   = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_QUEUE = "queue_001"
)

var CHAN_STOP = make(chan os.Signal, 1)

func main() {
	signal.Notify(
		CHAN_STOP,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	go internal.Handle(RABBITMQ_DSN, RABBITMQ_QUEUE)

	v := <-CHAN_STOP
	fmt.Println("❌ <-CHAN_STOP:", v)
}

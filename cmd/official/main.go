package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rabbitmq-go/official"
)

const (
	RABBITMQ_DSN      = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_EXCHANGE = "fruit"
)

var (
	CHAN_STOP = make(chan os.Signal, 1)
)

func main() {
	signal.Notify(
		CHAN_STOP,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	go official.Handle(RABBITMQ_DSN, RABBITMQ_EXCHANGE, "001", "001")

	v := <-CHAN_STOP
	fmt.Println("❌ <-CHAN_STOP:", v)
}

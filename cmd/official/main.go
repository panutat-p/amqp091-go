package main

import (
	"context"
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
	CHAN_DONE = make(chan struct{})
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	go official.Handle(ctx, CHAN_DONE, RABBITMQ_DSN, RABBITMQ_EXCHANGE, "002", "001")

	<-CHAN_DONE
	fmt.Println("💤 Shutdown")
}

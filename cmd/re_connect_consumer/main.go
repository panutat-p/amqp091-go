package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rabbitmq-go/re_connect_consumer"
)

const (
	RABBITMQ_DSN             = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_EXCHANGE_FOX    = "fox"
	RABBITMQ_EXCHANGE_MONKEY = "monkey"
	RABBITMQ_EXCHANGE_TURTLE = "turtle"
)

var (
	CHAN_DONE = make(chan struct{}, 3)
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	c := re_connect_consumer.NewClient(CHAN_DONE, RABBITMQ_DSN)
	c.Connect(ctx)

	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE_FOX, "001", "001")
	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE_MONKEY, "002", "002")
	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE_TURTLE, "003", "003")

	<-CHAN_DONE
	fmt.Println("💤 Shutdown")
}

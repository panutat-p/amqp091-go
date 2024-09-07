package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rabbitmq-go/pool"
)

const (
	RABBITMQ_DSN      = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_EXCHANGE = "fruit"
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

	c := pool.NewClient(CHAN_DONE, RABBITMQ_DSN)
	c.Connect(ctx)
	defer c.Close()

	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE, "001", "001")
	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE, "002", "002")
	go c.StartConsumer(ctx, RABBITMQ_EXCHANGE, "003", "003")

	<-CHAN_DONE
	fmt.Println("💤 Shutdown")
}

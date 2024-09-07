package main

import (
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
	CHAN_STOP = make(chan os.Signal, 1)
)

func main() {
	signal.Notify(
		CHAN_STOP,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	c := pool.NewClient(RABBITMQ_DSN)
	c.Connect()
	defer c.Close()

	go c.StartConsumer(RABBITMQ_EXCHANGE, "001", "001")
	go c.StartConsumer(RABBITMQ_EXCHANGE, "002", "002")
	go c.StartConsumer(RABBITMQ_EXCHANGE, "003", "003")

	select {
	case v := <-CHAN_STOP:
		fmt.Println("❌ <-CHAN_STOP:", v)
	}
}

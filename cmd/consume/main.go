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

	go func() {
		consumer := re_connect_consumer.NewConsumer(c.Conn, RABBITMQ_EXCHANGE_FOX, "001", "001")
		err := consumer.Init(ctx)
		if err != nil {
			panic(err)
		}
		consumer.Handle(ctx)
	}()

	go func() {
		consumer := re_connect_consumer.NewConsumer(c.Conn, RABBITMQ_EXCHANGE_MONKEY, "002", "002")
		err := consumer.Init(ctx)
		if err != nil {
			panic(err)
		}
		consumer.Handle(ctx)
	}()

	go func() {
		consumer := re_connect_consumer.NewConsumer(c.Conn, RABBITMQ_EXCHANGE_TURTLE, "003", "003")
		err := consumer.Init(ctx)
		if err != nil {
			panic(err)
		}
		consumer.Handle(ctx)
	}()

	<-CHAN_DONE
	fmt.Println("💤 Shutdown")
}

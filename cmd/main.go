package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rabbitmq-go/internal"
)

var CHAN_STOP = make(chan os.Signal, 1)

func main() {
	signal.Notify(
		CHAN_STOP,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	go internal.Handle()

	v := <-CHAN_STOP
	fmt.Println("❌ <-CHAN_STOP:", v)
}

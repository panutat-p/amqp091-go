package official

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Publish(done chan struct{}, dsn string, queueName string) {
	queue := New(queueName, dsn)
	message := []byte("🐢")

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

Publisher:
	for {
		select {
		case <-time.After(time.Second * 2):
			// Attempt to push a message every 2 seconds
			if err := queue.Push(message); err != nil {
				queue.errlog.Printf("Failed to Push: %s\n", err)
			} else {
				queue.infolog.Println("push succeeded")
			}
		case <-ctx.Done():
			if err := queue.Close(); err != nil {
				queue.errlog.Printf("close failed: %s\n", err)
			}
			break Publisher
		}
	}

	close(done)
}

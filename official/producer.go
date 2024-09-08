package official

import (
	"context"
	"time"
)

func Publish(done chan struct{}, dsn string, queueName string) {
	queue := New(queueName, dsn)
	message := []byte("🐢")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
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

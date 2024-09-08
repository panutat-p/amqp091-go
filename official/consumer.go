package official

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func Consume(done chan struct{}, dsn string, queueName string) {
	queue := New(queueName, dsn)
	<-time.After(time.Second) // Give the connection sometime to set up

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	deliveries, err := queue.Consume()
	if err != nil {
		queue.errlog.Printf("could not start consuming: %s\n", err)
		return
	}

	// This channel will receive a notification when a channel closed event
	// happens. This must be different from Client.notifyChanClose because the
	// library sends only one notification and Client.notifyChanClose already has
	// a receiver in handleReconnect().
	// Recommended to make it buffered to avoid deadlocks
	chClosedCh := make(chan *amqp091.Error, 1)
	queue.channel.NotifyClose(chClosedCh)

Consumer:
	for {
		select {
		case <-ctx.Done():
			err := queue.Close()
			if err != nil {
				queue.errlog.Printf("Failed to Close: %s\n", err)
			}
			break Consumer

		case amqErr := <-chClosedCh:
			// This case handles the event of closed channel e.g. abnormal shutdown
			queue.errlog.Printf("📣 got notify channel close: %s\n", amqErr)

			deliveries, err = queue.Consume()
			if err != nil {
				// If the AMQP channel is not ready, it will continue the loop.
				// Next iteration will enter this case because chClosedCh is closed by the library
				queue.errlog.Println("🟠 Failed to Consume, will try again...")
				time.Sleep(1 * time.Second)
				continue
			}

			// Re-set channel to receive notifications
			// The library closes this channel after abnormal shutdown
			chClosedCh = make(chan *amqp091.Error, 1)
			queue.channel.NotifyClose(chClosedCh)

		case delivery := <-deliveries:
			if err := delivery.Ack(false); err != nil {
				queue.errlog.Printf("Failed to Ack: %s\n", err)
			}
			queue.infolog.Printf("ACK %s\n", delivery.Body)
		}
	}

	close(done)
}

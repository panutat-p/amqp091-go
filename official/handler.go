package official

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func Handle(ctx context.Context, done chan struct{}, dsn, exchange, queue, consumer string) {
	c := NewClient(exchange, queue, dsn)

	// Give the connection sometime to set up
	<-time.After(time.Second)

	deliveries, err := c.Consume(consumer)
	if err != nil {
		fmt.Println("🔴 Failed to Consume, err:", err)
		return
	}
	fmt.Println("🟢 Succeeded to create deliveries")

	// This channel will receive a notification when a channel closed event
	// happens. This must be different from Client.notifyChanClose because the
	// library sends only one notification and Client.notifyChanClose already has
	// a receiver in handleReconnect().
	// Recommended to make it buffered to avoid deadlocks
	chClosedCh := make(chan *amqp091.Error, 1)
	c.channel.NotifyClose(chClosedCh)

Consumer:
	for {
		select {
		case <-ctx.Done():
			fmt.Println("💤 Graceful shutdown")
			err := c.Close()
			if err != nil {
				fmt.Println("🔴 Failed to Close, err:", err)
			}
			fmt.Println("🔴 Closed connection")
			break Consumer
		case amqErr := <-chClosedCh:
			time.Sleep(1 * time.Second)
			// This case handles the event of closed channel e.g. abnormal shutdown
			fmt.Println("🔴 AMQP Channel closed due to", amqErr)

			deliveries, err = c.Consume(consumer)
			if err != nil {
				// If the AMQP channel is not ready, it will continue the loop.
				// Next iteration will enter this case because chClosedCh is closed by the library
				fmt.Println("🔴 Failed to Consume, retrying..., err:", err)
				continue Consumer
			}

			// Re-set channel to receive notifications
			// The library closes this channel after abnormal shutdown
			chClosedCh = make(chan *amqp091.Error, 1)
			c.channel.NotifyClose(chClosedCh)

		case delivery := <-deliveries:
			err := delivery.Ack(false)
			if err != nil {
				fmt.Println("🔴 Failed to Ack, err:", err)
			}
			fmt.Println("ACK", string(delivery.Body))
		}
	}
	done <- struct{}{}
}

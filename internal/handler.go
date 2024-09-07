package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func Handle(dsn, queue string) {
	c := NewClient(queue, dsn)

	// Give the connection sometime to set up
	<-time.After(time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*25)
	defer cancel()

	deliveries, err := c.Consume()
	if err != nil {
		fmt.Println("🔴 Failed to Consume, err:", err)
		return
	}

	// This channel will receive a notification when a channel closed event
	// happens. This must be different from Client.notifyChanClose because the
	// library sends only one notification and Client.notifyChanClose already has
	// a receiver in handleReconnect().
	// Recommended to make it buffered to avoid deadlocks
	chClosedCh := make(chan *amqp091.Error, 1)
	c.channel.NotifyClose(chClosedCh)

loop:
	for {
		select {
		case <-ctx.Done():
			err := c.Close()
			if err != nil {
				fmt.Println("🔴 Failed to Close, err:", err)
			}
			break loop

		case amqErr := <-chClosedCh:
			// This case handles the event of closed channel e.g. abnormal shutdown
			fmt.Println("🔴 AMQP Channel closed due to", amqErr)

			deliveries, err = c.Consume()
			if err != nil {
				// If the AMQP channel is not ready, it will continue the loop.
				// Next iteration will enter this case because chClosedCh is closed by the library
				fmt.Println("🔴 Failed to Consume, retrying..., err:", err)
				continue
			}

			// Re-set channel to receive notifications
			// The library closes this channel after abnormal shutdown
			chClosedCh = make(chan *amqp091.Error, 1)
			c.channel.NotifyClose(chClosedCh)

		case delivery := <-deliveries:
			// Ack a message every 2 seconds
			fmt.Println("🟢 received message:", string(delivery.Body))
			if err := delivery.Ack(false); err != nil {
				fmt.Println("🔴 Failed to Ack, err:", err)
			}
			<-time.After(time.Second * 2)
		}
	}
}

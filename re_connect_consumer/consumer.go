package re_connect_consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	Exchange           string
	Queue              string
	Name               string
	Conn               *amqp091.Connection
	Channel            *amqp091.Channel
	Deliveries         <-chan amqp091.Delivery
	NotifyCloseChannel chan *amqp091.Error
}

func NewConsumer(conn *amqp091.Connection, exchange, queue, consumer string) *Consumer {
	return &Consumer{
		Conn:     conn,
		Exchange: exchange,
		Queue:    queue,
		Name:     consumer,
	}
}

func (c *Consumer) Init(ctx context.Context) error {
	channel, err := c.Conn.Channel()
	if err != nil {
		return err
	}
	c.Channel = channel
	c.NotifyCloseChannel = make(chan *amqp091.Error, 1)
	channel.NotifyClose(c.NotifyCloseChannel)

	err = channel.ExchangeDeclare(
		c.Exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	q, err := channel.QueueDeclare(
		c.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = channel.QueueBind(
		q.Name,
		"",
		c.Exchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	deliveries, err := channel.Consume(
		c.Queue,
		c.Name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	c.Deliveries = deliveries

	return nil
}

func (c *Consumer) ReInit(ctx context.Context) error {
	channel, err := c.Conn.Channel()
	if err != nil {
		return err
	}
	c.Channel = channel
	c.NotifyCloseChannel = make(chan *amqp091.Error, 1)
	channel.NotifyClose(c.NotifyCloseChannel)

	err = channel.ExchangeDeclarePassive(
		c.Exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	q, err := channel.QueueDeclarePassive(
		c.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = channel.QueueBind(
		q.Name,
		"",
		c.Exchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	deliveries, err := channel.Consume(
		c.Queue,
		c.Name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	c.Deliveries = deliveries

	return nil
}

func (c *Consumer) Handle(ctx context.Context) {
Consumer:
	for {
		select {
		case <-ctx.Done():
			fmt.Println("💤 Graceful shutdown")
			break Consumer
		case v := <-c.NotifyCloseChannel:
			fmt.Println("Got NOTIFY_CLOSE_CHANNEL", v)
			time.Sleep(5 * time.Second)
			err := c.ReInit(ctx)
			if err != nil {
				fmt.Println("Failed to ReInit, err:", err)
				continue Consumer
			}
		case d, ok := <-c.Deliveries:
			if !ok {
				fmt.Println("Delivery was closed")
				break Consumer
			}
			err := d.Ack(false)
			if err != nil {
				fmt.Println("Failed to Ack, err:", err)
				continue Consumer
			}
			fmt.Printf("ACK %+v\n", string(d.Body))
		}
	}
	fmt.Println("🔴 Consumer stopped")
}

func (c *Consumer) Close() error {
	return c.Channel.Close()
}

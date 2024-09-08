package re_connect_consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Client struct {
	done                  chan struct{}
	dsn                   string
	Conn                  *amqp091.Connection
	NotifyCloseConnection chan *amqp091.Error
}

func NewClient(done chan struct{}, dsn string) *Client {
	return &Client{
		done:                  done,
		dsn:                   dsn,
		NotifyCloseConnection: make(chan *amqp091.Error),
	}
}

func (c *Client) Connect(ctx context.Context) {
	conn, err := amqp091.Dial(c.dsn)
	if err != nil {
		panic(err)
	}
	conn.NotifyClose(c.NotifyCloseConnection)
	c.Conn = conn
	fmt.Println("🔵 Succeeded to Dial a connection")

	go c.ReConnect(ctx)
}

func (c *Client) ReConnect(ctx context.Context) {
ReConnectLoop:
	for {
		select {
		case <-ctx.Done():
			fmt.Println("💤 Graceful shutdown")
			c.Close()
			fmt.Println("💤 AMQP connection is closed")
			break ReConnectLoop
		case v := <-c.NotifyCloseConnection:
			fmt.Println("📣 <-CHAN_NOTIFY_CLOSE_CONNECTION:", v)
			for {
				conn, err := amqp091.Dial(c.dsn)
				if err != nil {
					fmt.Println("🔵 Failed to Dial, err:", err)
					time.Sleep(1 * time.Second)
					continue
				}
				c.NotifyCloseConnection = make(chan *amqp091.Error)
				conn.NotifyClose(c.NotifyCloseConnection)
				c.Conn = conn
				break
			}
			fmt.Println("🔵 Succeeded to re-connect")
		}
	}
	c.done <- struct{}{}
}

func (c *Client) StartConsumer(ctx context.Context, exchange, queue, consumer string) {
	var (
		channel            *amqp091.Channel
		deliveries         <-chan amqp091.Delivery
		notifyCloseChannel = make(chan *amqp091.Error, 1)
		err                error
	)

	channel, err = c.Conn.Channel()
	if err != nil {
		panic(err)
	}
	channel.NotifyClose(notifyCloseChannel)
	fmt.Println("🟠 Succeeded to open a channel")

	err = channel.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	q, err := channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	err = channel.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	deliveries, err = channel.Consume(
		queue,
		consumer,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("🟠 Succeeded to init a deliveries")

Consumer:
	for {
		select {
		case <-ctx.Done():
			fmt.Println("💤 Graceful shutdown")
			break Consumer
		case v := <-notifyCloseChannel:
			fmt.Println("📣 <-CHAN_NOTIFY_CLOSE_CHANNEL:", v)
		ReInit:
			for {
				ch, err := c.Conn.Channel()
				if err != nil {
					fmt.Println("🟠 Failed to open a channel, err:", err)
					time.Sleep(1 * time.Second)
					continue ReInit
				}
				channel = ch
				notifyCloseChannel = make(chan *amqp091.Error, 1)
				channel.NotifyClose(notifyCloseChannel)
				deliveries, err = channel.Consume(
					queue,
					consumer,
					false,
					false,
					false,
					false,
					nil,
				)
				fmt.Println("🟠 Succeeded to re-init")
				break ReInit
			}
		case d, ok := <-deliveries:
			if !ok {
				fmt.Println("❌ Channel closed")
				break Consumer
			}
			err = d.Ack(false)
			if err != nil {
				fmt.Println("🔴 Failed to Ack, err:", err)
				continue Consumer
			}
			fmt.Printf("ACK %+v\n", string(d.Body))
		}
	}
}

func (c *Client) Close() {
	err := c.Conn.Close()
	if err != nil {
		fmt.Println("🔴 Failed to close a connection, err:", err)
	}
}

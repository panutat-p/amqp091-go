package re_connect_consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Client struct {
	mu                    sync.Mutex
	isReConnect           bool
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

func (c *Client) Connect(ctx context.Context) error {
	conn, err := amqp091.Dial(c.dsn)
	if err != nil {
		return err
	}
	conn.NotifyClose(c.NotifyCloseConnection)
	c.Conn = conn
	fmt.Println("Succeeded to Dial a connection")

	go c.ReConnect(ctx)
	return nil
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
			fmt.Println("Got NOTIFY_CLOSE_CONNECTION", v)
			c.mu.Lock()
			c.isReConnect = true
			c.mu.Unlock()
			for {
				conn, err := amqp091.Dial(c.dsn)
				if err != nil {
					fmt.Println("Failed to Dial, err:", err)
					time.Sleep(1 * time.Second)
					continue
				}
				c.NotifyCloseConnection = make(chan *amqp091.Error)
				conn.NotifyClose(c.NotifyCloseConnection)
				c.Conn = conn
				break
			}
			c.mu.Lock()
			c.isReConnect = false
			c.mu.Unlock()
			fmt.Println("Succeeded to re-connect")
		}
	}
	c.done <- struct{}{}
}

func (c *Client) Close() {
	err := c.Conn.Close()
	if err != nil {
		fmt.Println("Failed to close a connection, err:", err)
	}
}

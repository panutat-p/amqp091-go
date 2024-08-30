package main

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Client is the base struct for handling connection recovery, consumption and
// publishing. Note that this struct has an internal mutex to safeguard against
// data races. As you develop and iterate over this example, you may need to add
// further locks, or safeguards, to keep your application safe from data races
type Client struct {
	dsn             string
	mu              *sync.Mutex
	queueName       string
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewClient(queueName, dsn string) *Client {
	client := Client{
		dsn:       dsn,
		mu:        &sync.Mutex{},
		queueName: queueName,
		done:      make(chan bool),
	}
	go client.handleReconnect()
	return &client
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (c *Client) handleReconnect() {
	for {
		c.mu.Lock()
		c.isReady = false
		c.mu.Unlock()

		fmt.Println("attempting to connect")

		conn, err := c.connect()
		if err != nil {
			select {
			case <-c.done:
				return
			case <-time.After(DELAY_RECONNECT):
			}
			continue
		}

		isDone := c.handleReInit(conn)
		if isDone {
			// When client.done is closed
			break
		}
	}
}

// connect will create a new AMQP connection
func (c *Client) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(c.dsn)
	if err != nil {
		fmt.Println("🔴 Failed to Dial, err:", err)
		return nil, err
	}

	c.changeConnection(conn)
	fmt.Println("🟢 Succeeded to connect")
	return conn, nil
}

// handleReInit will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (c *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		c.mu.Lock()
		c.isReady = false
		c.mu.Unlock()
		fmt.Println("🟡 client is not ready")

		err := c.init(conn)
		if err != nil {
			select {
			case <-c.done:
				return true
			case <-c.notifyConnClose:
				fmt.Println("🟡 notify connection closed")
				return false
			case <-time.After(DELAY_REINIT):
			}
			continue
		}

		select {
		case <-c.done:
			return true
		case <-c.notifyConnClose:
			fmt.Println("🟡 notify connection closed")
			return false
		case <-c.notifyChanClose:
			fmt.Println("🟡 notify channel closed")
			// continue the loop
		}
	}
}

// init will initialize channel & declare queue
func (c *Client) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("🔴 Failed to init Channel, err:", err)
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		fmt.Println("🔴 Failed to Confirm, err:", err)
		return err
	}
	_, err = ch.QueueDeclare(
		c.queueName,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		fmt.Println("🔴 Failed to QueueDeclare, err:", err)
		return err
	}

	c.changeChannel(ch)
	c.mu.Lock()
	c.isReady = true
	c.mu.Unlock()
	fmt.Println("🟢 client is ready")
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (c *Client) changeConnection(connection *amqp.Connection) {
	c.connection = connection
	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.connection.NotifyClose(c.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (c *Client) changeChannel(channel *amqp.Channel) {
	c.channel = channel
	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.notifyConfirm = make(chan amqp.Confirmation, 1)
	c.channel.NotifyClose(c.notifyChanClose)
	c.channel.NotifyPublish(c.notifyConfirm)
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (c *Client) Consume() (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	if !c.isReady {
		c.mu.Unlock()
		return nil, ErrNotConnected
	}
	c.mu.Unlock()

	if err := c.channel.Qos(
		1,     // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		return nil, err
	}

	return c.channel.Consume(
		c.queueName,
		"",    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}

// Close will cleanly shut down the channel and connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isReady {
		return ErrAlreadyClosed
	}
	close(c.done)
	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.connection.Close()
	if err != nil {
		return err
	}

	c.isReady = false // lock is need
	fmt.Println("🔴 client is not ready")
	return nil
}

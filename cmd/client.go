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
func (client *Client) handleReconnect() {
	for {
		client.mu.Lock()
		client.isReady = false
		client.mu.Unlock()

		fmt.Println("attempting to connect")

		conn, err := client.connect()
		if err != nil {
			select {
			case <-client.done:
				return
			case <-time.After(DELAY_RECONNECT):
			}
			continue
		}

		isDone := client.handleReInit(conn)
		if isDone {
			// When client.done is closed
			break
		}
	}
}

// connect will create a new AMQP connection
func (client *Client) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(client.dsn)
	if err != nil {
		fmt.Println("🔴 Failed to Dial, err:", err)
		return nil, err
	}

	client.changeConnection(conn)
	fmt.Println("🟢 Succeeded to connect")
	return conn, nil
}

// handleReInit will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		client.mu.Lock()
		client.isReady = false
		client.mu.Unlock()
		fmt.Println("🟡 client is not ready")

		err := client.init(conn)
		if err != nil {
			select {
			case <-client.done:
				return true
			case <-client.notifyConnClose:
				fmt.Println("🟡 notify connection closed")
				return false
			case <-time.After(DELAY_REINIT):
			}
			continue
		}

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			fmt.Println("🟡 notify connection closed")
			return false
		case <-client.notifyChanClose:
			fmt.Println("🟡 notify channel closed")
			// continue the loop
		}
	}
}

// init will initialize channel & declare queue
func (client *Client) init(conn *amqp.Connection) error {
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
		client.queueName,
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

	client.changeChannel(ch)
	client.mu.Lock()
	client.isReady = true
	client.mu.Unlock()
	fmt.Println("🟢 client is ready")
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (client *Client) Consume() (<-chan amqp.Delivery, error) {
	client.mu.Lock()
	if !client.isReady {
		client.mu.Unlock()
		return nil, ErrNotConnected
	}
	client.mu.Unlock()

	if err := client.channel.Qos(
		1,     // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.queueName,
		"",    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}

// Close will cleanly shut down the channel and connection.
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.isReady {
		return ErrAlreadyClosed
	}
	close(client.done)
	err := client.channel.Close()
	if err != nil {
		return err
	}
	err = client.connection.Close()
	if err != nil {
		return err
	}

	client.isReady = false // lock is need
	fmt.Println("🔴 client is not ready")
	return nil
}

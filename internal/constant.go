package internal

import (
	"errors"
	"time"
)

const (
	RABBITMQ_DSN   = "amqp://guest:guest@localhost:5672/"
	RABBITMQ_QUEUE = "queue_001"
)

const (
	DELAY_RECONNECT = 5 * time.Second
	DELAY_REINIT    = 2 * time.Second
)

var (
	ErrNotConnected  = errors.New("not connected to a server")
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
)

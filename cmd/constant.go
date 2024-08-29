package main

import (
	"errors"
	"time"
)

const (
	DELAY_RECONNECT = 5 * time.Second
	DELAY_REINIT    = 2 * time.Second
)

var (
	ErrNotConnected  = errors.New("not connected to a server")
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
)

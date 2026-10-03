package cdc

import "context"

type CDCEvent struct {
	ID      string
	Content []byte
	Ack     func(context.Context) error
}

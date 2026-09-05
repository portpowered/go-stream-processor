// Package esp provides an embedded stream processor library.
// This file implements bounded message buffers with backpressure support.
package engine

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrBufferClosed is returned when receiving from a closed, empty buffer.
var ErrBufferClosed = errors.New("buffer closed")

// DefaultBufferCapacity is the default buffer size if none is specified.
const DefaultBufferCapacity = 128

// Buffer is a bounded message channel between two nodes in the graph.
// When the buffer is full, Send blocks until space is available, providing
// natural backpressure from slow consumers to fast producers.
type Buffer struct {
	ch       chan Message
	capacity int

	// Metrics tracked atomically for thread-safe reads.
	sendCount atomic.Uint64
	recvCount atomic.Uint64
}

// NewBuffer creates a new bounded buffer with the given capacity.
// Capacity must be at least 1; if zero or negative, DefaultBufferCapacity is used.
func NewBuffer(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = DefaultBufferCapacity
	}
	return &Buffer{
		ch:       make(chan Message, capacity),
		capacity: capacity,
	}
}

// Send sends a message into the buffer. It blocks if the buffer is full,
// applying backpressure to the sender. Returns an error if the context
// is cancelled before the message can be sent.
func (b *Buffer) Send(ctx context.Context, msg Message) error {
	select {
	case b.ch <- msg:
		b.sendCount.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recv receives a message from the buffer. It blocks if the buffer is empty.
// Returns an error if the context is cancelled before a message is available.
func (b *Buffer) Recv(ctx context.Context) (Message, error) {
	select {
	case msg, ok := <-b.ch:
		if !ok {
			return Message{}, ErrBufferClosed
		}
		b.recvCount.Add(1)
		return msg, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

// Close closes the underlying channel. After Close, no more messages can be
// sent, but remaining messages can still be received.
func (b *Buffer) Close() {
	close(b.ch)
}

// Len returns the current number of messages in the buffer.
func (b *Buffer) Len() int {
	return len(b.ch)
}

// Cap returns the buffer's capacity.
func (b *Buffer) Cap() int {
	return b.capacity
}

// BufferMetrics contains a snapshot of the buffer's metrics.
type BufferMetrics struct {
	// QueueDepth is the current number of messages in the buffer.
	QueueDepth int
	// Capacity is the maximum number of messages the buffer can hold.
	Capacity int
	// SendCount is the total number of messages successfully sent.
	SendCount uint64
	// RecvCount is the total number of messages successfully received.
	RecvCount uint64
}

// Metrics returns a snapshot of the buffer's current metrics.
func (b *Buffer) Metrics() BufferMetrics {
	return BufferMetrics{
		QueueDepth: len(b.ch),
		Capacity:   b.capacity,
		SendCount:  b.sendCount.Load(),
		RecvCount:  b.recvCount.Load(),
	}
}

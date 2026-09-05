package engine

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

// ErrorStrategy determines how the pipeline handles errors from operators.
type ErrorStrategy int

const (
	// ErrorStrategySkipAndLog skips the failed message and logs the error.
	// This is the default strategy.
	ErrorStrategySkipAndLog ErrorStrategy = iota
	// ErrorStrategyDeadLetter routes failed messages to a dead-letter sink
	// for inspection.
	ErrorStrategyDeadLetter
	// ErrorStrategyFailPipeline stops the pipeline on the first error.
	ErrorStrategyFailPipeline
)

// ErrorConfig configures error handling for a pipeline.
type ErrorConfig struct {
	// Strategy determines how errors are handled. Defaults to SkipAndLog.
	Strategy ErrorStrategy
	// DeadLetterSink is the operator that receives dead-letter messages.
	// Only used when Strategy is ErrorStrategyDeadLetter. The sink's
	// ProcessMessage receives a DataMessage with Value set to a
	// DeadLetterMessage.
	DeadLetterSink Operator
}

// DeadLetterMessage wraps a failed message with error context for dead-letter
// routing. It is delivered as the Value field of a DataMessage to the
// dead-letter sink.
type DeadLetterMessage struct {
	// OriginalMsg is the message that caused the error.
	OriginalMsg DataMessage
	// Err is the error returned by the operator.
	Err error
	// NodeID identifies the node that produced the error.
	NodeID string
}

// errorHandler is called by the runtime when an operator returns an error
// or when EmitError is called. It returns nil to continue processing, or
// an error to stop the node.
type errorHandler func(nodeID string, msg DataMessage, err error) error

// errorCounts tracks per-node error counts in a thread-safe manner.
type errorCounts struct {
	mu     sync.RWMutex
	counts map[string]*atomic.Int64
}

func newErrorCounts() *errorCounts {
	return &errorCounts{counts: make(map[string]*atomic.Int64)}
}

func (ec *errorCounts) increment(nodeID string) {
	ec.mu.RLock()
	counter, ok := ec.counts[nodeID]
	ec.mu.RUnlock()
	if ok {
		counter.Add(1)
		return
	}
	ec.mu.Lock()
	counter, ok = ec.counts[nodeID]
	if !ok {
		counter = &atomic.Int64{}
		ec.counts[nodeID] = counter
	}
	ec.mu.Unlock()
	counter.Add(1)
}

// snapshot returns a copy of the current error counts.
func (ec *errorCounts) snapshot() map[string]int64 {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	result := make(map[string]int64)
	for k, v := range ec.counts {
		if c := v.Load(); c > 0 {
			result[k] = c
		}
	}
	return result
}

// makeErrorHandler creates an errorHandler based on the given ErrorConfig.
// The handler increments error counts, and depending on the strategy either
// logs and skips, routes to a dead-letter buffer, or fails the pipeline.
func makeErrorHandler(cfg ErrorConfig, counts *errorCounts, dlBuffer *Buffer) errorHandler {
	switch cfg.Strategy {
	case ErrorStrategySkipAndLog:
		return func(nodeID string, msg DataMessage, err error) error {
			counts.increment(nodeID)
			log.Printf("[esp] node %q: skipping message due to error: %v", nodeID, err)
			return nil // continue processing
		}

	case ErrorStrategyDeadLetter:
		return func(nodeID string, msg DataMessage, err error) error {
			counts.increment(nodeID)
			dlMsg := NewDataMessage(msg.Key, DeadLetterMessage{
				OriginalMsg: msg,
				Err:         err,
				NodeID:      nodeID,
			}, msg.EventTime)
			// Best-effort send to dead-letter buffer. If the buffer is full
			// or context is cancelled, log and continue.
			if sendErr := dlBuffer.Send(context.Background(), dlMsg); sendErr != nil {
				log.Printf("[esp] node %q: failed to send to dead-letter sink: %v (original error: %v)", nodeID, sendErr, err)
			}
			return nil // continue processing
		}

	case ErrorStrategyFailPipeline:
		return func(nodeID string, msg DataMessage, err error) error {
			counts.increment(nodeID)
			return err // propagate error to stop the node
		}

	default:
		// Fallback to skip-and-log.
		return func(nodeID string, msg DataMessage, err error) error {
			counts.increment(nodeID)
			log.Printf("[esp] node %q: skipping message due to error: %v", nodeID, err)
			return nil
		}
	}
}

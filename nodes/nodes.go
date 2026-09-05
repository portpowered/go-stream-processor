// Package nodes provides prebuilt operators and functional constructors for the
// go-stream-processor library.
//
// # Functional constructors
//
// The simplest way to build an operator is to pass a plain function:
//
//	// Map: transform each message value
//	double := nodes.Map(func(v int) int { return v * 2 })
//
//	// Filter: keep only matching messages
//	evens := nodes.Filter(func(v int) bool { return v%2 == 0 })
//
//	// SourceFunc: wrap a function as a Source
//	src := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
//	    for i := 0; i < 10; i++ {
//	        if err := emit("", i); err != nil {
//	            return err
//	        }
//	    }
//	    return nil
//	})
//
//	// SinkFunc: wrap a function as a Sink
//	print := nodes.SinkFunc(func(v int) error {
//	    fmt.Println(v)
//	    return nil
//	})
//
// # Routing operators
//
//	fanout := nodes.FanOut()
//	merge  := nodes.Merge()
//	router := nodes.Router(func(msg sp.DataMessage) string {
//	    if msg.Value.(int) > 0 { return "positive" }
//	    return "non-positive"
//	})
//
// # Concurrent processing
//
// Wrap any operator to process messages on a pool of goroutines:
//
//	concurrent := nodes.Concurrent(myOp, 4) // 4 parallel workers
package nodes

import (
	"context"
	"sync"

	sp "github.com/portpowered/go-stream-processor/stream_processor"
)

// ---------------------------------------------------------------------------
// Functional type helpers
// ---------------------------------------------------------------------------

// EmitFunc is the simplified emit signature used by [SourceFunc]. It takes a
// key and a value, hiding the full DataMessage construction from the caller.
type EmitFunc func(key string, value any) error

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

// Map creates an [sp.Operator] that transforms each incoming message value
// using fn. The key and event time are preserved on the output message.
// Messages whose value cannot be type-asserted to In are silently skipped.
//
// Example:
//
//	doubler := nodes.Map(func(v int) int { return v * 2 })
func Map[In any, Out any](fn func(In) Out) sp.Operator {
	return &mapOp[In, Out]{fn: fn}
}

type mapOp[In any, Out any] struct {
	sp.BaseOperator
	fn func(In) Out
}

func (m *mapOp[In, Out]) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
	in, ok := msg.Value.(In)
	if !ok {
		return nil // skip wrong-type messages
	}
	return c.Emit(sp.DataMessage{Key: msg.Key, Value: m.fn(in), EventTime: msg.EventTime})
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

// Filter creates an [sp.Operator] that passes through only messages whose
// value satisfies pred. Messages of the wrong type are silently skipped.
//
// Example:
//
//	evens := nodes.Filter(func(v int) bool { return v%2 == 0 })
func Filter[T any](pred func(T) bool) sp.Operator {
	return &filterOp[T]{pred: pred}
}

type filterOp[T any] struct {
	sp.BaseOperator
	pred func(T) bool
}

func (f *filterOp[T]) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
	v, ok := msg.Value.(T)
	if !ok {
		return nil
	}
	if f.pred(v) {
		return c.Emit(msg)
	}
	return nil
}

// ---------------------------------------------------------------------------
// SourceFunc
// ---------------------------------------------------------------------------

// SourceFunc wraps a plain function as an [sp.Source]. The function receives
// an [EmitFunc] helper that hides DataMessage construction; just provide a
// key (empty string is fine) and any value.
//
// Example:
//
//	src := nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
//	    for _, item := range data {
//	        if err := emit("", item); err != nil {
//	            return err
//	        }
//	    }
//	    return nil
//	})
func SourceFunc(fn func(ctx context.Context, emit EmitFunc) error) sp.Source {
	return &funcSource{fn: fn}
}

type funcSource struct {
	fn func(ctx context.Context, emit EmitFunc) error
}

func (s *funcSource) Run(ctx context.Context, c sp.Collector) error {
	return s.fn(ctx, func(key string, value any) error {
		return c.Emit(sp.DataMessage{Key: key, Value: value})
	})
}

// ---------------------------------------------------------------------------
// SinkFunc
// ---------------------------------------------------------------------------

// SinkFunc wraps a typed function as an [sp.Operator] sink. The function
// receives the typed value directly; messages of the wrong type are skipped.
//
// Example:
//
//	printer := nodes.SinkFunc(func(v string) error {
//	    fmt.Println(v)
//	    return nil
//	})
func SinkFunc[T any](fn func(T) error) sp.Operator {
	return &funcSink[T]{fn: fn}
}

type funcSink[T any] struct {
	sp.BaseOperator
	fn func(T) error
}

func (s *funcSink[T]) ProcessMessage(_ context.Context, msg sp.DataMessage, _ sp.Collector) error {
	v, ok := msg.Value.(T)
	if !ok {
		return nil
	}
	return s.fn(v)
}

// ---------------------------------------------------------------------------
// FanOut
// ---------------------------------------------------------------------------

// FanOut returns an [sp.Operator] that duplicates each input message to all
// output ports. Connect it to multiple downstream nodes to broadcast data.
func FanOut() sp.Operator {
	return &fanOutOp{}
}

type fanOutOp struct{ sp.BaseOperator }

func (f *fanOutOp) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
	return c.Emit(msg)
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

// Merge returns an [sp.Operator] that merges multiple input streams into one
// output stream, passing every message through unchanged.
func Merge() sp.Operator {
	return &mergeOp{}
}

type mergeOp struct{ sp.BaseOperator }

func (m *mergeOp) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
	return c.Emit(msg)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

// Router returns an [sp.Operator] that routes each message to a named output
// port determined by routeFn. If routeFn returns "" and defaultPort is
// non-empty, the message goes to defaultPort; otherwise it is dropped.
//
// Connect the router's output ports by name using [sp.GraphBuilder.ConnectPort].
//
// Example:
//
//	router := nodes.Router(func(msg sp.DataMessage) string {
//	    if msg.Value.(int) > 0 { return "positive" }
//	    return "non-positive"
//	}, "")
func Router(routeFn func(sp.DataMessage) string, defaultPort string) sp.Operator {
	return &routerOp{routeFn: routeFn, defaultPort: defaultPort}
}

type routerOp struct {
	sp.BaseOperator
	routeFn     func(sp.DataMessage) string
	defaultPort string
}

func (r *routerOp) ProcessMessage(_ context.Context, msg sp.DataMessage, c sp.Collector) error {
	port := r.routeFn(msg)
	if port == "" {
		if r.defaultPort != "" {
			port = r.defaultPort
		} else {
			return nil // drop
		}
	}
	return c.EmitToPort(port, msg)
}

// ---------------------------------------------------------------------------
// Concurrent
// ---------------------------------------------------------------------------

// Concurrent wraps any [sp.Operator] to process messages using a pool of n
// goroutines for parallel execution. Output ordering is NOT guaranteed.
//
// Checkpoint barriers wait for all in-flight workers to complete before
// proceeding, ensuring consistent snapshots.
//
// Example:
//
//	heavyOp := nodes.Concurrent(myExpensiveOp, 8)
func Concurrent(op sp.Operator, n int) sp.Operator {
	if n < 1 {
		n = 1
	}
	return &concurrentOp{inner: op, sem: make(chan struct{}, n)}
}

type concurrentOp struct {
	inner sp.Operator
	sem   chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	err   error
}

func (c *concurrentOp) ProcessMessage(ctx context.Context, msg sp.DataMessage, col sp.Collector) error {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { <-c.sem }()
		if err := c.inner.ProcessMessage(ctx, msg, col); err != nil {
			c.mu.Lock()
			if c.err == nil {
				c.err = err
			}
			c.mu.Unlock()
		}
	}()
	return nil
}

func (c *concurrentOp) HandleWatermark(ctx context.Context, wm sp.WatermarkSignal, col sp.Collector) error {
	c.wg.Wait()
	if err := c.drainErr(); err != nil {
		return err
	}
	return c.inner.HandleWatermark(ctx, wm, col)
}

func (c *concurrentOp) HandleCheckpoint(ctx context.Context, b sp.BarrierSignal) ([]byte, error) {
	c.wg.Wait()
	if err := c.drainErr(); err != nil {
		return nil, err
	}
	return c.inner.HandleCheckpoint(ctx, b)
}

func (c *concurrentOp) OnStart(ctx context.Context) error { return c.inner.OnStart(ctx) }

func (c *concurrentOp) OnClose(ctx context.Context) error {
	c.wg.Wait()
	return c.inner.OnClose(ctx)
}

func (c *concurrentOp) drainErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.err
	c.err = nil
	return err
}

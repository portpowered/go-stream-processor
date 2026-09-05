package engine

import (
	"context"
	"sync"
)

// ConcurrentOperator wraps any Operator and runs ProcessMessage on a pool of
// N goroutines for parallel processing. Output ordering is NOT guaranteed.
//
// Backpressure: when all workers are busy, new messages block until a worker
// becomes available. Checkpoint barriers wait for all in-flight workers to
// complete before proceeding (ensuring consistent snapshots).
type ConcurrentOperator struct {
	inner       Operator
	concurrency int

	// sem limits concurrent ProcessMessage calls.
	sem chan struct{}
	// wg tracks in-flight workers for barrier alignment.
	wg sync.WaitGroup
	// mu protects lastErr.
	mu      sync.Mutex
	lastErr error
}

// Compile-time check: ConcurrentOperator implements Drainable.
var _ Drainable = (*ConcurrentOperator)(nil)

// NewConcurrentOperator creates a ConcurrentOperator wrapping the given operator
// with the specified concurrency level. Concurrency must be >= 1.
func NewConcurrentOperator(op Operator, concurrency int) *ConcurrentOperator {
	if concurrency < 1 {
		concurrency = 1
	}
	return &ConcurrentOperator{
		inner:       op,
		concurrency: concurrency,
		sem:         make(chan struct{}, concurrency),
	}
}

// ProcessMessage dispatches the message to a worker goroutine from the pool.
// Blocks when all workers are busy (backpressure). Output ordering is not
// guaranteed since messages are processed concurrently.
func (c *ConcurrentOperator) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	// Check for errors from previous workers.
	c.mu.Lock()
	if c.lastErr != nil {
		err := c.lastErr
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	// Acquire a worker slot (blocks when pool is full = backpressure).
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { <-c.sem }()

		if err := c.inner.ProcessMessage(ctx, msg, collector); err != nil {
			c.mu.Lock()
			if c.lastErr == nil {
				c.lastErr = err
			}
			c.mu.Unlock()
		}
	}()

	return nil
}

// HandleWatermark waits for all in-flight workers to complete, then delegates
// to the inner operator.
func (c *ConcurrentOperator) HandleWatermark(ctx context.Context, watermark WatermarkSignal, collector Collector) error {
	c.wg.Wait()
	if err := c.drainError(); err != nil {
		return err
	}
	return c.inner.HandleWatermark(ctx, watermark, collector)
}

// HandleCheckpoint waits for all in-flight workers to complete before
// snapshotting state, ensuring the checkpoint is consistent.
func (c *ConcurrentOperator) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	c.wg.Wait()
	if err := c.drainError(); err != nil {
		return nil, err
	}
	return c.inner.HandleCheckpoint(ctx, barrier)
}

// OnStart delegates to the inner operator.
func (c *ConcurrentOperator) OnStart(ctx context.Context) error {
	return c.inner.OnStart(ctx)
}

// Drain blocks until all in-flight workers complete and their results have been
// emitted to downstream output buffers. It respects context cancellation and is
// idempotent — calling it multiple times is safe.
func (c *ConcurrentOperator) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return c.drainError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OnClose waits for all in-flight workers, then delegates to the inner operator.
func (c *ConcurrentOperator) OnClose(ctx context.Context) error {
	c.wg.Wait()
	return c.inner.OnClose(ctx)
}

// drainError returns and clears any accumulated error from workers.
func (c *ConcurrentOperator) drainError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.lastErr
	c.lastErr = nil
	return err
}

// Concurrency returns the configured concurrency level.
func (c *ConcurrentOperator) Concurrency() int {
	return c.concurrency
}

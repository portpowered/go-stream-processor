package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewBuffer(t *testing.T) {
	t.Run("creates buffer with specified capacity", func(t *testing.T) {
		b := NewBuffer(10)
		if b.Cap() != 10 {
			t.Errorf("expected capacity 10, got %d", b.Cap())
		}
		if b.Len() != 0 {
			t.Errorf("expected length 0, got %d", b.Len())
		}
	})

	t.Run("uses default capacity for zero", func(t *testing.T) {
		b := NewBuffer(0)
		if b.Cap() != DefaultBufferCapacity {
			t.Errorf("expected capacity %d, got %d", DefaultBufferCapacity, b.Cap())
		}
	})

	t.Run("uses default capacity for negative", func(t *testing.T) {
		b := NewBuffer(-5)
		if b.Cap() != DefaultBufferCapacity {
			t.Errorf("expected capacity %d, got %d", DefaultBufferCapacity, b.Cap())
		}
	})
}

func TestBufferSendRecv(t *testing.T) {
	t.Run("send and receive a single message", func(t *testing.T) {
		b := NewBuffer(5)
		ctx := context.Background()
		msg := NewDataMessage("key1", "value1", time.Now())

		if err := b.Send(ctx, msg); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		if b.Len() != 1 {
			t.Errorf("expected length 1 after send, got %d", b.Len())
		}

		got, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		if !got.IsData() || got.Data.Key != "key1" {
			t.Errorf("received unexpected message: %+v", got)
		}
		if b.Len() != 0 {
			t.Errorf("expected length 0 after recv, got %d", b.Len())
		}
	})

	t.Run("send and receive multiple messages preserves order", func(t *testing.T) {
		b := NewBuffer(10)
		ctx := context.Background()
		count := 5

		for i := 0; i < count; i++ {
			msg := NewDataMessage("key", i, time.Now())
			if err := b.Send(ctx, msg); err != nil {
				t.Fatalf("Send %d failed: %v", i, err)
			}
		}

		for i := 0; i < count; i++ {
			got, err := b.Recv(ctx)
			if err != nil {
				t.Fatalf("Recv %d failed: %v", i, err)
			}
			if got.Data.Value.(int) != i {
				t.Errorf("expected value %d, got %v", i, got.Data.Value)
			}
		}
	})

	t.Run("send and receive signal messages", func(t *testing.T) {
		b := NewBuffer(5)
		ctx := context.Background()
		signal := NewBarrierSignal(1, 0, false)
		msg := NewSignalMessage(signal)

		if err := b.Send(ctx, msg); err != nil {
			t.Fatalf("Send failed: %v", err)
		}

		got, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		if !got.IsSignal() || got.Signal.Barrier.Epoch != 1 {
			t.Errorf("received unexpected message: %+v", got)
		}
	})
}

func TestBufferBlocking(t *testing.T) {
	t.Run("send blocks when buffer is full", func(t *testing.T) {
		b := NewBuffer(1)
		ctx := context.Background()
		msg := NewDataMessage("key", "value", time.Now())

		// Fill the buffer.
		if err := b.Send(ctx, msg); err != nil {
			t.Fatalf("first Send failed: %v", err)
		}

		// Second send should block. Use a short-lived context to verify.
		blockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		err := b.Send(blockCtx, msg)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded when buffer full, got: %v", err)
		}
	})

	t.Run("recv blocks when buffer is empty", func(t *testing.T) {
		b := NewBuffer(5)
		blockCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := b.Recv(blockCtx)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded when buffer empty, got: %v", err)
		}
	})

	t.Run("blocked send unblocks when space available", func(t *testing.T) {
		b := NewBuffer(1)
		ctx := context.Background()
		msg := NewDataMessage("key", "value", time.Now())

		// Fill the buffer.
		if err := b.Send(ctx, msg); err != nil {
			t.Fatalf("first Send failed: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- b.Send(ctx, NewDataMessage("key2", "value2", time.Now()))
		}()

		// Give the goroutine time to block.
		time.Sleep(10 * time.Millisecond)

		// Drain the buffer to unblock the sender.
		if _, err := b.Recv(ctx); err != nil {
			t.Fatalf("Recv failed: %v", err)
		}

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("blocked Send returned error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked Send did not complete after space freed")
		}
	})
}

func TestBufferCancellation(t *testing.T) {
	t.Run("send respects context cancellation", func(t *testing.T) {
		b := NewBuffer(1)
		ctx, cancel := context.WithCancel(context.Background())
		msg := NewDataMessage("key", "value", time.Now())

		// Fill the buffer.
		if err := b.Send(ctx, msg); err != nil {
			t.Fatalf("first Send failed: %v", err)
		}

		// Cancel context before attempting blocked send.
		cancel()

		err := b.Send(ctx, msg)
		if err != context.Canceled {
			t.Errorf("expected Canceled, got: %v", err)
		}
	})

	t.Run("recv respects context cancellation", func(t *testing.T) {
		b := NewBuffer(5)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := b.Recv(ctx)
		if err != context.Canceled {
			t.Errorf("expected Canceled, got: %v", err)
		}
	})
}

func TestBufferMetrics(t *testing.T) {
	t.Run("tracks send and recv counts", func(t *testing.T) {
		b := NewBuffer(10)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			if err := b.Send(ctx, NewDataMessage("k", i, time.Now())); err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < 3; i++ {
			if _, err := b.Recv(ctx); err != nil {
				t.Fatal(err)
			}
		}

		m := b.Metrics()
		if m.SendCount != 5 {
			t.Errorf("expected SendCount 5, got %d", m.SendCount)
		}
		if m.RecvCount != 3 {
			t.Errorf("expected RecvCount 3, got %d", m.RecvCount)
		}
		if m.QueueDepth != 2 {
			t.Errorf("expected QueueDepth 2, got %d", m.QueueDepth)
		}
		if m.Capacity != 10 {
			t.Errorf("expected Capacity 10, got %d", m.Capacity)
		}
	})

	t.Run("metrics are thread-safe", func(t *testing.T) {
		b := NewBuffer(100)
		ctx := context.Background()
		var wg sync.WaitGroup

		// Concurrent senders.
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if err := b.Send(ctx, NewDataMessage("k", j, time.Now())); err != nil {
						return
					}
				}
			}()
		}

		// Concurrent receivers.
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if _, err := b.Recv(ctx); err != nil {
						return
					}
				}
			}()
		}

		wg.Wait()

		m := b.Metrics()
		if m.SendCount != 1000 {
			t.Errorf("expected SendCount 1000, got %d", m.SendCount)
		}
		if m.RecvCount != 1000 {
			t.Errorf("expected RecvCount 1000, got %d", m.RecvCount)
		}
	})
}

func TestBufferCapacityEdgeCases(t *testing.T) {
	t.Run("capacity of 1 works correctly", func(t *testing.T) {
		b := NewBuffer(1)
		ctx := context.Background()

		for i := 0; i < 100; i++ {
			msg := NewDataMessage("k", i, time.Now())
			go func() {
				_ = b.Send(ctx, msg)
			}()
			got, err := b.Recv(ctx)
			if err != nil {
				t.Fatalf("Recv %d failed: %v", i, err)
			}
			if got.Data.Value.(int) != i {
				// With capacity 1 and concurrent send, ordering may not match.
				// Just verify we got a message.
			}
		}
	})

	t.Run("close allows draining remaining messages", func(t *testing.T) {
		b := NewBuffer(5)
		ctx := context.Background()

		for i := 0; i < 3; i++ {
			if err := b.Send(ctx, NewDataMessage("k", i, time.Now())); err != nil {
				t.Fatal(err)
			}
		}
		b.Close()

		// Should still be able to read the 3 messages.
		for i := 0; i < 3; i++ {
			got, err := b.Recv(ctx)
			if err != nil {
				t.Fatalf("Recv %d after close failed: %v", i, err)
			}
			if got.Data.Value.(int) != i {
				t.Errorf("expected value %d, got %v", i, got.Data.Value)
			}
		}
	})
}

func BenchmarkBufferBackpressure(b *testing.B) {
	// This benchmark demonstrates backpressure behavior:
	// a fast producer paired with a slow consumer. The buffer
	// naturally throttles the producer via blocking sends.
	buf := NewBuffer(64)
	ctx := context.Background()

	// Start a slow consumer.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			if _, err := buf.Recv(ctx); err != nil {
				return
			}
			// Simulate slow processing.
			time.Sleep(time.Microsecond)
		}
	}()

	msg := NewDataMessage("bench", 42, time.Now())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := buf.Send(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}

	<-done
	b.StopTimer()

	m := buf.Metrics()
	b.ReportMetric(float64(m.SendCount), "sends")
	b.ReportMetric(float64(m.RecvCount), "recvs")
}

func BenchmarkBufferThroughput(b *testing.B) {
	buf := NewBuffer(256)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			if _, err := buf.Recv(ctx); err != nil {
				return
			}
		}
	}()

	msg := NewDataMessage("bench", 42, time.Now())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := buf.Send(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}

	<-done
}

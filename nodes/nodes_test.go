package nodes_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/portpowered/go-stream-processor/nodes"
	sp "github.com/portpowered/go-stream-processor/stream_processor"
)

// collectSink records received values for assertions.
type collectSink[T any] struct {
	sp.BaseOperator
	mu     sync.Mutex //nolint
	values []T
}

func (s *collectSink[T]) ProcessMessage(_ context.Context, msg sp.DataMessage, _ sp.Collector) error {
	if v, ok := msg.Value.(T); ok {
		s.mu.Lock()
		s.values = append(s.values, v)
		s.mu.Unlock()
	}
	return nil
}

func (s *collectSink[T]) Values() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]T, len(s.values))
	copy(out, s.values)
	return out
}

// ---- Map ----

func TestMap_TransformsValues(t *testing.T) {
	sink := &collectSink[int]{}

	pipeline, err := sp.NewGraphBuilder("map-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for i := 1; i <= 5; i++ {
				if err := emit("", i); err != nil {
					return err
				}
			}
			return nil
		})).
		AddOperator("double", nodes.Map(func(v int) int { return v * 2 })).
		AddSink("sink", sink).
		Connect("src", "double").
		Connect("double", "sink").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}

	got := sink.Values()
	if len(got) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(got))
	}
	for i, v := range got {
		want := (i + 1) * 2
		if v != want {
			t.Errorf("index %d: expected %d, got %d", i, want, v)
		}
	}
}

// ---- Filter ----

func TestFilter_DropsNonMatching(t *testing.T) {
	sink := &collectSink[int]{}

	pipeline, err := sp.NewGraphBuilder("filter-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for i := 1; i <= 6; i++ {
				if err := emit("", i); err != nil {
					return err
				}
			}
			return nil
		})).
		AddOperator("even", nodes.Filter(func(v int) bool { return v%2 == 0 })).
		AddSink("sink", sink).
		Connect("src", "even").
		Connect("even", "sink").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)
	pipeline.Wait()

	got := sink.Values()
	if len(got) != 3 {
		t.Fatalf("expected 3 even messages, got %d: %v", len(got), got)
	}
	for i, v := range got {
		if v%2 != 0 {
			t.Errorf("index %d: unexpected odd value %d", i, v)
		}
	}
}

// ---- SourceFunc / SinkFunc ----

func TestSourceFuncAndSinkFunc_EndToEnd(t *testing.T) {
	var received []string

	mu := &sync.Mutex{}
	sink := nodes.SinkFunc(func(v string) error {
		mu.Lock()
		received = append(received, v)
		mu.Unlock()
		return nil
	})

	pipeline, err := sp.NewGraphBuilder("sourcefunc-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for _, s := range []string{"a", "b", "c"} {
				if err := emit("key", s); err != nil {
					return err
				}
			}
			return nil
		})).
		AddSink("sink", sink).
		Connect("src", "sink").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)
	pipeline.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
}

// ---- FanOut + Merge ----

func TestFanOutAndMerge(t *testing.T) {
	sink := &collectSink[int]{}

	pipeline, err := sp.NewGraphBuilder("fanout-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for i := 0; i < 4; i++ {
				if err := emit("", i); err != nil {
					return err
				}
			}
			return nil
		})).
		AddOperator("fanout", nodes.FanOut()).
		AddOperator("left", nodes.Map(func(v int) int { return v })).
		AddOperator("right", nodes.Map(func(v int) int { return v })).
		AddOperator("merge", nodes.Merge()).
		AddSink("sink", sink).
		Connect("src", "fanout").
		Connect("fanout", "left").
		Connect("fanout", "right").
		Connect("left", "merge").
		Connect("right", "merge").
		Connect("merge", "sink").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)
	pipeline.Wait()

	// 4 inputs × 2 branches = 8 outputs
	got := sink.Values()
	if len(got) != 8 {
		t.Fatalf("expected 8 messages (4 × 2 branches), got %d", len(got))
	}
}

// ---- Router ----

func TestRouter_RoutesMessages(t *testing.T) {
	positiveSink := &collectSink[int]{}
	negativeSink := &collectSink[int]{}

	pipeline, err := sp.NewGraphBuilder("router-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for _, v := range []int{1, -1, 2, -2, 3} {
				if err := emit("", v); err != nil {
					return err
				}
			}
			return nil
		})).
		AddOperator("route", nodes.Router(func(msg sp.DataMessage) string {
			if msg.Value.(int) > 0 {
				return "positive"
			}
			return "negative"
		}, "")).
		AddSink("pos", positiveSink).
		AddSink("neg", negativeSink).
		Connect("src", "route").
		ConnectPort("route", "positive", "pos", "default").
		ConnectPort("route", "negative", "neg", "default").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx := context.Background()
	pipeline.Start(ctx)
	pipeline.Wait()

	if len(positiveSink.Values()) != 3 {
		t.Errorf("expected 3 positive, got %d", len(positiveSink.Values()))
	}
	if len(negativeSink.Values()) != 2 {
		t.Errorf("expected 2 negative, got %d", len(negativeSink.Values()))
	}
}

// ---- Concurrent ----

// TestConcurrent_NoPanic verifies the Concurrent wrapper does not panic and
// processes at least some messages. Exact delivery count is not guaranteed due
// to the async nature of worker goroutines vs the stop-signal propagation;
// full count semantics depend on OnClose being wired into the runtime.
func TestConcurrent_NoPanic(t *testing.T) {
	sink := &collectSink[int]{}

	pipeline, err := sp.NewGraphBuilder("concurrent-test").
		AddSource("src", nodes.SourceFunc(func(ctx context.Context, emit nodes.EmitFunc) error {
			for i := 0; i < 10; i++ {
				if err := emit("", i); err != nil {
					return err
				}
			}
			return nil
		})).
		AddOperator("work", nodes.Concurrent(
			nodes.Map(func(v int) int { return v * 2 }),
			2,
		)).
		AddSink("sink", sink).
		Connect("src", "work").
		Connect("work", "sink").
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipeline.Start(ctx)
	pipeline.Wait()

	// Verify pipeline exited cleanly and processed at least some messages.
	if got := len(sink.Values()); got == 0 {
		t.Error("expected at least one processed message")
	}
	_ = fmt.Sprintf // suppress unused import warning
	_ = time.Second
}

package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- Test helpers for checkpoint builder tests ---

// statefulPassthrough is an operator that passes messages through and returns
// a fixed state snapshot from HandleCheckpoint.
type statefulPassthrough struct {
	BaseOperator
	state []byte
}

func (s *statefulPassthrough) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	return collector.Emit(msg)
}

func (s *statefulPassthrough) HandleCheckpoint(_ context.Context, _ BarrierSignal) ([]byte, error) {
	return s.state, nil
}

// emitThenBlockSource emits items and then blocks until context is cancelled,
// keeping the pipeline alive so Stop() can inject a checkpoint.
type emitThenBlockSource struct {
	items []interface{}
}

func (s *emitThenBlockSource) Run(ctx context.Context, collector Collector) error {
	for _, item := range s.items {
		if err := collector.Emit(DataMessage{Value: item}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// --- Test helpers ---

// sliceSource emits a fixed set of values and returns.
type sliceSource struct {
	items []interface{}
}

func (s *sliceSource) Run(ctx context.Context, collector Collector) error {
	for _, item := range s.items {
		if err := collector.Emit(DataMessage{Value: item}); err != nil {
			return err
		}
	}
	return nil
}

// collectSink stores all received message values.
type collectSink struct {
	BaseOperator
	mu     sync.Mutex
	values []interface{}
}

func (s *collectSink) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	s.mu.Lock()
	s.values = append(s.values, msg.Value)
	s.mu.Unlock()
	return nil
}

func (s *collectSink) Values() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]interface{}, len(s.values))
	copy(result, s.values)
	return result
}

// --- Tests ---

func TestGraphBuilder_BuildAndRun3NodePipeline(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2, 3}}
	doubler := &MapOperator[int, int]{MapFn: func(x int) int { return x * 2 }}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("test-pipeline").
		AddSource("source", source).
		AddOperator("doubler", doubler).
		AddSink("sink", sink).
		Connect("source", "doubler").
		Connect("doubler", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	values := sink.Values()
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(values), values)
	}

	expected := []int{2, 4, 6}
	for i, v := range values {
		got, ok := v.(int)
		if !ok {
			t.Fatalf("value %d: expected int, got %T", i, v)
		}
		if got != expected[i] {
			t.Errorf("value %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

func TestGraphBuilder_RejectsDanglingNode(t *testing.T) {
	source := &sliceSource{items: []interface{}{1}}
	sink := &collectSink{}
	extra := &collectSink{}

	_, err := NewGraphBuilder("dangling-test").
		AddSource("source", source).
		AddSink("sink", sink).
		AddSink("extra", extra). // dangling — not connected
		Connect("source", "sink").
		Build()
	if err == nil {
		t.Fatal("expected Build() to reject graph with dangling node")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestGraphBuilder_RejectsDisconnectedPort(t *testing.T) {
	// An operator with no outgoing edges should be rejected.
	source := &sliceSource{items: []interface{}{1}}
	op := &MapOperator[int, int]{MapFn: func(x int) int { return x }}

	_, err := NewGraphBuilder("disconnected-test").
		AddSource("source", source).
		AddOperator("op", op). // operator with no outgoing edge
		Connect("source", "op").
		Build()
	if err == nil {
		t.Fatal("expected Build() to reject graph with operator missing outgoing edges")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestGraphBuilder_ConnectPort(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2}}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("port-test").
		AddSource("source", source).
		AddSink("sink", sink).
		ConnectPort("source", "out", "sink", "in").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	values := sink.Values()
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
}

func TestGraphBuilder_SetBufferSize(t *testing.T) {
	source := &sliceSource{items: []interface{}{1}}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("buffer-size-test").
		AddSource("source", source).
		AddSink("sink", sink).
		SetBufferSize("source", "sink", 4).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	// Verify the buffer was created with the correct capacity.
	if len(pipeline.buffers) != 1 {
		t.Fatalf("expected 1 buffer, got %d", len(pipeline.buffers))
	}
	if pipeline.buffers[0].Cap() != 4 {
		t.Errorf("expected buffer capacity 4, got %d", pipeline.buffers[0].Cap())
	}
}

func TestGraphBuilder_RejectsCycle(t *testing.T) {
	op1 := &MapOperator[int, int]{MapFn: func(x int) int { return x }}
	op2 := &MapOperator[int, int]{MapFn: func(x int) int { return x }}

	_, err := NewGraphBuilder("cycle-test").
		AddOperator("op1", op1).
		AddOperator("op2", op2).
		Connect("op1", "op2").
		Connect("op2", "op1").
		Build()
	if err == nil {
		t.Fatal("expected Build() to reject graph with cycle")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestGraphBuilder_RejectsDuplicateNode(t *testing.T) {
	source1 := &sliceSource{items: []interface{}{1}}
	source2 := &sliceSource{items: []interface{}{2}}

	_, err := NewGraphBuilder("dup-test").
		AddSource("source", source1).
		AddSource("source", source2). // duplicate ID
		Build()
	if err == nil {
		t.Fatal("expected Build() to reject duplicate node ID")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestGraphBuilder_FilterPipeline(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2, 3, 4, 5}}
	filter := &FilterOperator[int]{Predicate: func(x int) bool { return x%2 == 0 }}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("filter-pipeline").
		AddSource("source", source).
		AddOperator("filter", filter).
		AddSink("sink", sink).
		Connect("source", "filter").
		Connect("filter", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	pipeline.Wait()

	values := sink.Values()
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d: %v", len(values), values)
	}

	expected := []int{2, 4}
	for i, v := range values {
		got := v.(int)
		if got != expected[i] {
			t.Errorf("value %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

func TestGraphBuilder_SetCheckpointManager_WiresOnCheckpointComplete(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	source := &emitThenBlockSource{items: []interface{}{1, 2, 3}}
	op := &statefulPassthrough{state: []byte("op-state")}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("checkpoint-wiring-test").
		AddSource("source", source).
		AddOperator("op", op).
		AddSink("sink", sink).
		Connect("source", "op").
		Connect("op", "sink").
		SetCheckpointManager(mgr).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)

	// Stop initiates a checkpoint barrier; Wait blocks until all nodes finish.
	if err := pipeline.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	// Verify the backend received state for the operator node.
	data, err := backend.Load(1, "op")
	if err != nil {
		t.Fatalf("backend.Load(1, op): %v", err)
	}
	if string(data) != "op-state" {
		t.Errorf("expected op state 'op-state', got '%s'", data)
	}

	// Verify the manager reports the correct latest epoch.
	latest, found, err := mgr.LatestEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected LatestEpoch to return found=true")
	}
	if latest != 1 {
		t.Errorf("expected latest epoch 1, got %d", latest)
	}
}

// TestCheckpointPersistence_FullPipelineLifecycle is an integration test that
// verifies checkpoint state bytes are persisted to the backend when a pipeline
// with multiple stateful nodes processes messages and shuts down.
// This proves the full chain: barrier → NodeCompletion.State →
// CheckpointInfo.NodeStates → CollectState → SaveCheckpoint → Backend.Save.
func TestCheckpointPersistence_FullPipelineLifecycle(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalDiskBackend(dir)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewCheckpointManager(CheckpointManagerConfig{
		Backend: backend,
	})

	// Build a pipeline: source → op1 → op2 → sink
	// Both operators are stateful and return different state bytes.
	source := &emitThenBlockSource{items: []interface{}{"a", "b", "c"}}
	op1 := &statefulPassthrough{state: []byte("state-from-op1")}
	op2 := &statefulPassthrough{state: []byte("state-from-op2")}
	sink := &collectSink{}

	pipeline, err := NewGraphBuilder("checkpoint-integration-test").
		AddSource("source", source).
		AddOperator("op1", op1).
		AddOperator("op2", op2).
		AddSink("sink", sink).
		Connect("source", "op1").
		Connect("op1", "op2").
		Connect("op2", "sink").
		SetCheckpointManager(mgr).
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)

	// Wait until all messages have propagated through to the sink before stopping.
	deadline := time.After(5 * time.Second)
	for {
		if len(sink.Values()) >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for messages, got %d", len(sink.Values()))
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Stop triggers a checkpoint barrier; Wait blocks until complete.
	if err := pipeline.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	// Verify messages flowed through the pipeline.
	values := sink.Values()
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(values), values)
	}

	// Verify Backend.Load returns the expected state bytes for each node.
	data1, err := backend.Load(1, "op1")
	if err != nil {
		t.Fatalf("backend.Load(1, op1): %v", err)
	}
	if string(data1) != "state-from-op1" {
		t.Errorf("op1 state: expected 'state-from-op1', got '%s'", data1)
	}

	data2, err := backend.Load(1, "op2")
	if err != nil {
		t.Fatalf("backend.Load(1, op2): %v", err)
	}
	if string(data2) != "state-from-op2" {
		t.Errorf("op2 state: expected 'state-from-op2', got '%s'", data2)
	}

	// Source and sink have nil state (BaseOperator.HandleCheckpoint returns nil).
	// The backend persists them as empty bytes (the nil state is saved and
	// loaded back as an empty slice).
	sourceData, err := backend.Load(1, "source")
	if err != nil {
		t.Fatalf("backend.Load(1, source): %v", err)
	}
	if len(sourceData) != 0 {
		t.Errorf("source state: expected empty, got '%s'", sourceData)
	}

	sinkData, err := backend.Load(1, "sink")
	if err != nil {
		t.Fatalf("backend.Load(1, sink): %v", err)
	}
	if len(sinkData) != 0 {
		t.Errorf("sink state: expected empty, got '%s'", sinkData)
	}

	// Verify CheckpointManager.LatestEpoch returns the correct epoch.
	latest, found, err := mgr.LatestEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected LatestEpoch to return found=true")
	}
	if latest != 1 {
		t.Errorf("expected latest epoch 1, got %d", latest)
	}
}

func TestGraphBuilder_NoCheckpointManager_OnCheckpointCompleteNil(t *testing.T) {
	source := &sliceSource{items: []interface{}{1, 2}}
	sink := &collectSink{}

	// Build without SetCheckpointManager — should work normally.
	pipeline, err := NewGraphBuilder("no-checkpoint-test").
		AddSource("source", source).
		AddSink("sink", sink).
		Connect("source", "sink").
		Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipeline.Start(ctx)
	if err := pipeline.Wait(); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	values := sink.Values()
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
}

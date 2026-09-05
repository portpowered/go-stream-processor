package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockNode is a configurable mock implementation of the Node interface for testing.
type mockNode struct {
	id              string
	processFunc     func(ctx context.Context, msg DataMessage, emit Emitter) error
	watermarkFunc   func(ctx context.Context, wm WatermarkSignal, emit Emitter) error
	checkpointFunc  func(ctx context.Context, barrier BarrierSignal) ([]byte, error)
	processCount    atomic.Int64
	watermarkCount  atomic.Int64
	checkpointCount atomic.Int64
}

func newMockNode(id string) *mockNode {
	return &mockNode{
		id: id,
		processFunc: func(ctx context.Context, msg DataMessage, emit Emitter) error {
			return nil
		},
		watermarkFunc: func(ctx context.Context, wm WatermarkSignal, emit Emitter) error {
			return nil
		},
		checkpointFunc: func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
			return nil, nil
		},
	}
}

func (m *mockNode) ID() string { return m.id }

func (m *mockNode) ProcessMessage(ctx context.Context, msg DataMessage, emit Emitter) error {
	m.processCount.Add(1)
	return m.processFunc(ctx, msg, emit)
}

func (m *mockNode) HandleWatermark(ctx context.Context, wm WatermarkSignal, emit Emitter) error {
	m.watermarkCount.Add(1)
	return m.watermarkFunc(ctx, wm, emit)
}

func (m *mockNode) HandleCheckpoint(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
	m.checkpointCount.Add(1)
	return m.checkpointFunc(ctx, barrier)
}

func TestNodeRuntime_DataMessageDispatched(t *testing.T) {
	node := newMockNode("test")
	var received DataMessage
	node.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		received = msg
		return nil
	}

	input := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	msg := NewDataMessage("key1", "value1", time.Now())
	if err := input.Send(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Close input to signal completion.
	input.Close()
	if err := rt.Wait(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if node.processCount.Load() != 1 {
		t.Errorf("expected 1 process call, got %d", node.processCount.Load())
	}
	if received.Key != "key1" {
		t.Errorf("expected key 'key1', got %q", received.Key)
	}
}

func TestNodeRuntime_MultipleInputsMultiplexed(t *testing.T) {
	node := newMockNode("test")

	input1 := NewBuffer(16)
	input2 := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input1, input2},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send messages on both inputs.
	for i := 0; i < 5; i++ {
		_ = input1.Send(ctx, NewDataMessage("a", i, time.Now()))
		_ = input2.Send(ctx, NewDataMessage("b", i, time.Now()))
	}

	input1.Close()
	input2.Close()
	_ = rt.Wait()

	if node.processCount.Load() != 10 {
		t.Errorf("expected 10 process calls, got %d", node.processCount.Load())
	}
}

func TestNodeRuntime_BarrierSignalHandled(t *testing.T) {
	node := newMockNode("test")
	var capturedEpoch uint64
	node.checkpointFunc = func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
		capturedEpoch = barrier.Epoch
		return []byte("state"), nil
	}

	input := NewBuffer(16)
	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send a barrier signal.
	barrier := NewBarrierSignal(42, 1, false)
	_ = input.Send(ctx, NewSignalMessage(barrier))

	input.Close()
	_ = rt.Wait()

	if node.checkpointCount.Load() != 1 {
		t.Errorf("expected 1 checkpoint call, got %d", node.checkpointCount.Load())
	}
	if capturedEpoch != 42 {
		t.Errorf("expected epoch 42, got %d", capturedEpoch)
	}

	// Verify barrier was forwarded to output.
	fwd, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !fwd.IsSignal() || fwd.Signal.SignalType != SignalTypeBarrier {
		t.Error("expected barrier signal forwarded to output")
	}
	if fwd.Signal.Barrier.Epoch != 42 {
		t.Errorf("expected forwarded barrier epoch 42, got %d", fwd.Signal.Barrier.Epoch)
	}
}

func TestNodeRuntime_WatermarkSignalHandled(t *testing.T) {
	node := newMockNode("test")
	var capturedWM WatermarkSignal
	node.watermarkFunc = func(ctx context.Context, wm WatermarkSignal, emit Emitter) error {
		capturedWM = wm
		return nil
	}

	input := NewBuffer(16)
	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	wm := NewWatermarkSignal(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	_ = input.Send(ctx, NewSignalMessage(wm))

	input.Close()
	_ = rt.Wait()

	if node.watermarkCount.Load() != 1 {
		t.Errorf("expected 1 watermark call, got %d", node.watermarkCount.Load())
	}
	if capturedWM.EventTime != time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) {
		t.Errorf("unexpected watermark event time: %v", capturedWM.EventTime)
	}

	// Verify watermark was forwarded.
	fwd, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !fwd.IsSignal() || fwd.Signal.SignalType != SignalTypeWatermark {
		t.Error("expected watermark signal forwarded to output")
	}
}

func TestNodeRuntime_StopSignalForwardsAndShutdown(t *testing.T) {
	node := newMockNode("test")

	input := NewBuffer(16)
	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send a stop signal.
	_ = input.Send(ctx, NewSignalMessage(NewStopSignal()))

	err := rt.Wait()
	// After stop, the runtime shuts down (context cancelled).
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify stop was forwarded.
	fwd, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !fwd.IsSignal() || fwd.Signal.SignalType != SignalTypeStop {
		t.Error("expected stop signal forwarded to output")
	}
}

func TestNodeRuntime_ControlPauseResume(t *testing.T) {
	node := newMockNode("test")

	var mu sync.Mutex
	var processedKeys []string
	node.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		mu.Lock()
		processedKeys = append(processedKeys, msg.Key)
		mu.Unlock()
		return nil
	}

	input := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send first message.
	_ = input.Send(ctx, NewDataMessage("before-pause", nil, time.Now()))
	time.Sleep(50 * time.Millisecond)

	// Pause the node.
	_ = rt.SendControl(ctx, NewPauseControl())
	time.Sleep(50 * time.Millisecond)

	// Send messages while paused — they queue in the merged channel.
	_ = input.Send(ctx, NewDataMessage("during-pause", nil, time.Now()))
	time.Sleep(50 * time.Millisecond)

	// Resume.
	_ = rt.SendControl(ctx, NewResumeControl())
	time.Sleep(50 * time.Millisecond)

	// Close input to signal completion.
	input.Close()
	_ = rt.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(processedKeys) != 2 {
		t.Fatalf("expected 2 processed messages, got %d: %v", len(processedKeys), processedKeys)
	}
	if processedKeys[0] != "before-pause" {
		t.Errorf("expected first message 'before-pause', got %q", processedKeys[0])
	}
	if processedKeys[1] != "during-pause" {
		t.Errorf("expected second message 'during-pause', got %q", processedKeys[1])
	}
}

func TestNodeRuntime_ControlTerminate(t *testing.T) {
	node := newMockNode("test")

	input := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send terminate control.
	_ = rt.SendControl(ctx, NewTerminateControl())

	err := rt.Wait()
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNodeRuntime_ControlCheckpointRequest(t *testing.T) {
	node := newMockNode("test")
	var checkpointEpochs []uint64
	node.checkpointFunc = func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
		checkpointEpochs = append(checkpointEpochs, barrier.Epoch)
		return nil, nil
	}

	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeSource,
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send checkpoint request via control channel (like the controller would for sources).
	_ = rt.SendControl(ctx, NewCheckpointRequestControl(5, 1, false))
	time.Sleep(100 * time.Millisecond)

	rt.Stop()
	_ = rt.Wait()

	if len(checkpointEpochs) != 1 || checkpointEpochs[0] != 5 {
		t.Errorf("expected checkpoint epoch 5, got %v", checkpointEpochs)
	}

	// Verify barrier was injected into output.
	msg, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !msg.IsSignal() || msg.Signal.SignalType != SignalTypeBarrier || msg.Signal.Barrier.Epoch != 5 {
		t.Error("expected barrier with epoch 5 in output")
	}
}

func TestNodeRuntime_ContextCancellation(t *testing.T) {
	node := newMockNode("test")

	input := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
	})

	ctx, cancel := context.WithCancel(context.Background())

	rt.Start(ctx)

	// Cancel the context.
	cancel()

	err := rt.Wait()
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestNodeRuntime_GracefulShutdown(t *testing.T) {
	node := newMockNode("test")

	// Slow processor to test in-flight completion.
	var processed atomic.Int64
	node.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		time.Sleep(10 * time.Millisecond)
		processed.Add(1)
		return nil
	}

	input := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send some messages.
	for i := 0; i < 5; i++ {
		_ = input.Send(ctx, NewDataMessage("key", i, time.Now()))
	}

	// Close input — runtime should process remaining messages then exit.
	input.Close()
	_ = rt.Wait()

	if processed.Load() != 5 {
		t.Errorf("expected 5 processed messages, got %d", processed.Load())
	}
}

func TestNodeRuntime_BarrierThenStopCausesShutdown(t *testing.T) {
	node := newMockNode("test")

	input := NewBuffer(16)
	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send data then a barrier with ThenStop=true.
	_ = input.Send(ctx, NewDataMessage("key", "value", time.Now()))
	barrier := NewBarrierSignal(1, 1, true)
	_ = input.Send(ctx, NewSignalMessage(barrier))

	err := rt.Wait()
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	if node.processCount.Load() != 1 {
		t.Errorf("expected 1 process call, got %d", node.processCount.Load())
	}
	if node.checkpointCount.Load() != 1 {
		t.Errorf("expected 1 checkpoint call, got %d", node.checkpointCount.Load())
	}
}

func TestNodeRuntime_OutputForwarding(t *testing.T) {
	node := newMockNode("test")
	node.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		// Emit a transformed message.
		return emit.Emit(ctx, NewDataMessage(msg.Key+"-out", msg.Value, msg.EventTime))
	}

	input := NewBuffer(16)
	output1 := NewBuffer(16)
	output2 := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output1, output2},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	_ = input.Send(ctx, NewDataMessage("k", "v", time.Now()))
	input.Close()
	_ = rt.Wait()

	// Both outputs should have the emitted message.
	msg1, err := output1.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg2, err := output2.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if msg1.Data.Key != "k-out" || msg2.Data.Key != "k-out" {
		t.Error("expected emitted message on both outputs")
	}
}

func TestNodeRuntime_DrainsDrainableOperatorBeforeStop(t *testing.T) {
	// Use a ConcurrentOperator with slow workers to verify drain happens
	// before stop is forwarded downstream.
	var processed atomic.Int64
	inner := &testDrainableInner{
		processFn: func(ctx context.Context, msg DataMessage, collector Collector) error {
			time.Sleep(30 * time.Millisecond)
			processed.Add(1)
			return collector.Emit(msg)
		},
	}

	concurrency := 4
	concOp := NewConcurrentOperator(inner, concurrency)
	opNode := NewOperatorNode("drain-test", concOp)

	input := NewBuffer(16)
	output := NewBuffer(64)
	opNode.SetOutputs([]*Buffer{output})

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     opNode,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt.Start(ctx)

	// Send messages that will be in-flight when stop arrives.
	numMsgs := concurrency
	for i := 0; i < numMsgs; i++ {
		_ = input.Send(ctx, NewDataMessage("k", i, time.Now()))
	}

	// Small delay to let workers start, then send stop.
	time.Sleep(5 * time.Millisecond)
	_ = input.Send(ctx, NewSignalMessage(NewStopSignal()))

	err := rt.Wait()
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	// All messages should have been processed (drain happened before stop forwarded).
	if count := processed.Load(); count != int64(numMsgs) {
		t.Errorf("expected %d processed messages after drain, got %d", numMsgs, count)
	}
}

func TestNodeRuntime_SkipsDrainForNonDrainableOperator(t *testing.T) {
	// Plain mockNode (not Drainable) — stop should forward normally.
	node := newMockNode("non-drainable")

	input := NewBuffer(16)
	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input},
		Outputs:  []*Buffer{output},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rt.Start(ctx)

	_ = input.Send(ctx, NewSignalMessage(NewStopSignal()))

	err := rt.Wait()
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify stop was forwarded to output.
	fwd, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !fwd.IsSignal() || fwd.Signal.SignalType != SignalTypeStop {
		t.Error("expected stop signal forwarded to output")
	}
}

// testDrainableInner is a simple Operator for use in drain tests.
type testDrainableInner struct {
	BaseOperator
	processFn func(ctx context.Context, msg DataMessage, collector Collector) error
}

func (d *testDrainableInner) ProcessMessage(ctx context.Context, msg DataMessage, collector Collector) error {
	if d.processFn != nil {
		return d.processFn(ctx, msg, collector)
	}
	return nil
}

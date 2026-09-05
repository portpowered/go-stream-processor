package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Unit tests for BarrierAligner ---

func TestBarrierAligner_SingleInput_PassThrough(t *testing.T) {
	ba := NewBarrierAligner(1)

	barrier := &BarrierSignal{Epoch: 1, MinEpoch: 1, Timestamp: time.Now()}
	toProcess, aligned := ba.OnMessage(indexedMessage{
		InputIndex: 0,
		Msg:        NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}),
	})

	if aligned == nil {
		t.Fatal("expected immediate alignment for single-input node")
	}
	if aligned.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", aligned.Epoch)
	}
	if len(toProcess) != 0 {
		t.Errorf("expected no buffered messages, got %d", len(toProcess))
	}
}

func TestBarrierAligner_TwoInputs_Alignment(t *testing.T) {
	ba := NewBarrierAligner(2)

	barrier := &BarrierSignal{Epoch: 5, MinEpoch: 1, Timestamp: time.Now()}

	// First input sends barrier — not aligned yet.
	toProcess, aligned := ba.OnMessage(indexedMessage{
		InputIndex: 0,
		Msg:        NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}),
	})
	if aligned != nil {
		t.Fatal("should not be aligned after first barrier")
	}
	if len(toProcess) != 0 {
		t.Errorf("no messages to process yet")
	}
	if !ba.IsAligning() {
		t.Fatal("should be aligning")
	}

	// Data from barriered input 0 should be buffered.
	toProcess, aligned = ba.OnMessage(indexedMessage{
		InputIndex: 0,
		Msg:        NewDataMessage("buffered", "val", time.Now()),
	})
	if aligned != nil {
		t.Fatal("should not be aligned yet")
	}
	if len(toProcess) != 0 {
		t.Error("data from barriered input should be buffered")
	}

	// Data from non-barriered input 1 should pass through.
	toProcess, aligned = ba.OnMessage(indexedMessage{
		InputIndex: 1,
		Msg:        NewDataMessage("passthrough", "val", time.Now()),
	})
	if aligned != nil {
		t.Fatal("should not be aligned yet")
	}
	if len(toProcess) != 1 {
		t.Fatalf("expected 1 passthrough message, got %d", len(toProcess))
	}
	if toProcess[0].Data.Key != "passthrough" {
		t.Errorf("expected key 'passthrough', got %q", toProcess[0].Data.Key)
	}

	// Second input sends barrier — alignment complete!
	toProcess, aligned = ba.OnMessage(indexedMessage{
		InputIndex: 1,
		Msg:        NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}),
	})
	if aligned == nil {
		t.Fatal("should be aligned after all inputs have sent barrier")
	}
	if aligned.Epoch != 5 {
		t.Errorf("expected epoch 5, got %d", aligned.Epoch)
	}
	// Buffered message from input 0 should be released.
	if len(toProcess) != 1 {
		t.Fatalf("expected 1 buffered message, got %d", len(toProcess))
	}
	if toProcess[0].Data.Key != "buffered" {
		t.Errorf("expected key 'buffered', got %q", toProcess[0].Data.Key)
	}
	if ba.IsAligning() {
		t.Error("should no longer be aligning")
	}
}

func TestBarrierAligner_ThreeInputs(t *testing.T) {
	ba := NewBarrierAligner(3)

	barrier := &BarrierSignal{Epoch: 10, MinEpoch: 1, Timestamp: time.Now()}

	// Send barriers from inputs 0, 1, 2 with data interleaved.
	ba.OnMessage(indexedMessage{InputIndex: 0, Msg: NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier})})
	ba.OnMessage(indexedMessage{InputIndex: 0, Msg: NewDataMessage("buf0", nil, time.Now())})
	ba.OnMessage(indexedMessage{InputIndex: 1, Msg: NewDataMessage("pass1", nil, time.Now())})
	ba.OnMessage(indexedMessage{InputIndex: 1, Msg: NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier})})
	ba.OnMessage(indexedMessage{InputIndex: 1, Msg: NewDataMessage("buf1", nil, time.Now())})

	// Third barrier triggers alignment.
	toProcess, aligned := ba.OnMessage(indexedMessage{
		InputIndex: 2,
		Msg:        NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}),
	})

	if aligned == nil {
		t.Fatal("should be aligned")
	}
	if aligned.Epoch != 10 {
		t.Errorf("expected epoch 10, got %d", aligned.Epoch)
	}
	// Should have 2 buffered messages (buf0 from input 0, buf1 from input 1).
	if len(toProcess) != 2 {
		t.Fatalf("expected 2 buffered messages, got %d", len(toProcess))
	}
}

// --- Integration tests using NodeRuntime ---

// forwardingProcessFunc creates a processFunc that forwards messages via emit.
func forwardingProcessFunc() func(ctx context.Context, msg DataMessage, emit Emitter) error {
	return func(ctx context.Context, msg DataMessage, emit Emitter) error {
		return emit.Emit(ctx, NewDataMessage(msg.Key, msg.Value, msg.EventTime))
	}
}

// TestBarrierPropagation_LinearGraph tests barrier propagation through A->B->C.
func TestBarrierPropagation_LinearGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create nodes.
	nodeA := newMockNode("A")
	nodeB := newMockNode("B")
	nodeC := newMockNode("C")

	// B and C forward messages so downstream nodes receive them.
	nodeB.processFunc = forwardingProcessFunc()
	nodeC.processFunc = forwardingProcessFunc()

	// Buffers: A->B, B->C.
	bufAB := NewBuffer(16)
	bufBC := NewBuffer(16)
	sinkOutput := NewBuffer(16)

	// Create runtimes.
	rtA := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeA,
		NodeType: NodeTypeSource,
		Outputs:  []*Buffer{bufAB},
	})
	rtB := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeB,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAB},
		Outputs:  []*Buffer{bufBC},
	})
	rtC := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeC,
		NodeType: NodeTypeSink,
		Inputs:   []*Buffer{bufBC},
		Outputs:  []*Buffer{sinkOutput},
	})

	// Start runtimes.
	rtB.Start(ctx)
	rtC.Start(ctx)
	rtA.Start(ctx)

	// Send data then barrier from source A.
	_ = bufAB.Send(ctx, NewDataMessage("d1", "v1", time.Now()))
	_ = bufAB.Send(ctx, NewDataMessage("d2", "v2", time.Now()))
	barrier := NewBarrierSignal(1, 1, false)
	_ = bufAB.Send(ctx, NewSignalMessage(barrier))
	_ = bufAB.Send(ctx, NewDataMessage("d3", "v3", time.Now()))

	// Close to drain.
	bufAB.Close()

	_ = rtB.Wait()

	// Check B processed all 3 data messages and checkpointed once.
	if nodeB.processCount.Load() != 3 {
		t.Errorf("B: expected 3 process calls, got %d", nodeB.processCount.Load())
	}
	if nodeB.checkpointCount.Load() != 1 {
		t.Errorf("B: expected 1 checkpoint, got %d", nodeB.checkpointCount.Load())
	}

	// Close B->C buffer to drain C.
	bufBC.Close()
	_ = rtC.Wait()

	// C should also have checkpointed once.
	if nodeC.checkpointCount.Load() != 1 {
		t.Errorf("C: expected 1 checkpoint, got %d", nodeC.checkpointCount.Load())
	}
	if nodeC.processCount.Load() != 3 {
		t.Errorf("C: expected 3 process calls, got %d", nodeC.processCount.Load())
	}

	// Verify barrier appeared in sink output.
	sinkOutput.Close()
	var barrierCount int
	for {
		msg, err := sinkOutput.Recv(ctx)
		if err != nil {
			break
		}
		if msg.IsSignal() && msg.Signal.SignalType == SignalTypeBarrier {
			barrierCount++
			if msg.Signal.Barrier.Epoch != 1 {
				t.Errorf("expected barrier epoch 1, got %d", msg.Signal.Barrier.Epoch)
			}
		}
	}
	// C has outputs so it forwards the barrier.
	if barrierCount != 1 {
		t.Errorf("expected 1 barrier in sink output, got %d", barrierCount)
	}

	rtA.Stop()
	_ = rtA.Wait()
}

// TestBarrierAlignment_DiamondGraph tests barrier alignment at merge node D
// in a diamond topology: A->B, A->C, B->D, C->D.
func TestBarrierAlignment_DiamondGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track checkpoint order at D to verify alignment.
	var dCheckpointEpochs []uint64
	var dCheckpointMu sync.Mutex

	nodeB := newMockNode("B")
	nodeC := newMockNode("C")
	nodeD := newMockNode("D")

	// B and C forward data messages so D receives them.
	nodeB.processFunc = forwardingProcessFunc()
	nodeC.processFunc = forwardingProcessFunc()

	nodeD.checkpointFunc = func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
		dCheckpointMu.Lock()
		dCheckpointEpochs = append(dCheckpointEpochs, barrier.Epoch)
		dCheckpointMu.Unlock()
		return nil, nil
	}

	// Track data messages processed at D and their order relative to checkpoint.
	var dMessages []string
	var dMsgMu sync.Mutex
	nodeD.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		dMsgMu.Lock()
		dMessages = append(dMessages, msg.Key)
		dMsgMu.Unlock()
		return nil
	}

	// Buffers:
	// A->B (bufAB), A->C (bufAC), B->D (bufBD), C->D (bufCD)
	bufAB := NewBuffer(16)
	bufAC := NewBuffer(16)
	bufBD := NewBuffer(16)
	bufCD := NewBuffer(16)
	sinkOutput := NewBuffer(16)

	rtB := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeB,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAB},
		Outputs:  []*Buffer{bufBD},
	})
	rtC := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeC,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAC},
		Outputs:  []*Buffer{bufCD},
	})
	rtD := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeD,
		NodeType: NodeTypeSink,
		Inputs:   []*Buffer{bufBD, bufCD},
		Outputs:  []*Buffer{sinkOutput},
	})

	// Start runtimes.
	rtD.Start(ctx)
	rtB.Start(ctx)
	rtC.Start(ctx)

	// Simulate source A sending data and barrier to both branches.
	barrier := NewBarrierSignal(7, 1, false)

	// Send pre-barrier data on both paths.
	_ = bufAB.Send(ctx, NewDataMessage("B-pre", "v", time.Now()))
	_ = bufAC.Send(ctx, NewDataMessage("C-pre", "v", time.Now()))

	// Send barrier on both paths.
	_ = bufAB.Send(ctx, NewSignalMessage(barrier))
	_ = bufAC.Send(ctx, NewSignalMessage(barrier))

	// Send post-barrier data on both paths.
	_ = bufAB.Send(ctx, NewDataMessage("B-post", "v", time.Now()))
	_ = bufAC.Send(ctx, NewDataMessage("C-post", "v", time.Now()))

	// Close source buffers to drain.
	bufAB.Close()
	bufAC.Close()

	_ = rtB.Wait()
	_ = rtC.Wait()

	// Close intermediate buffers.
	bufBD.Close()
	bufCD.Close()

	_ = rtD.Wait()

	// Verify D checkpointed exactly once for epoch 7.
	dCheckpointMu.Lock()
	defer dCheckpointMu.Unlock()
	if len(dCheckpointEpochs) != 1 {
		t.Fatalf("D: expected 1 checkpoint, got %d: %v", len(dCheckpointEpochs), dCheckpointEpochs)
	}
	if dCheckpointEpochs[0] != 7 {
		t.Errorf("D: expected checkpoint epoch 7, got %d", dCheckpointEpochs[0])
	}

	// Verify D processed all 4 data messages.
	dMsgMu.Lock()
	defer dMsgMu.Unlock()
	if len(dMessages) != 4 {
		t.Errorf("D: expected 4 data messages, got %d: %v", len(dMessages), dMessages)
	}

	// Verify barrier was forwarded to sink output.
	sinkOutput.Close()
	var barrierCount int
	for {
		msg, err := sinkOutput.Recv(ctx)
		if err != nil {
			break
		}
		if msg.IsSignal() && msg.Signal.SignalType == SignalTypeBarrier {
			barrierCount++
		}
	}
	if barrierCount != 1 {
		t.Errorf("expected 1 barrier in sink output, got %d", barrierCount)
	}
}

// TestBarrierAlignment_BufferedDataOrder verifies that during alignment,
// data from already-barriered inputs is buffered and released after alignment,
// while data from non-barriered inputs continues processing.
func TestBarrierAlignment_BufferedDataOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var processedOrder []string
	var mu sync.Mutex

	nodeD := newMockNode("D")
	nodeD.processFunc = func(ctx context.Context, msg DataMessage, emit Emitter) error {
		mu.Lock()
		processedOrder = append(processedOrder, msg.Key)
		mu.Unlock()
		return nil
	}

	// D has 2 inputs.
	input0 := NewBuffer(16)
	input1 := NewBuffer(16)

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeD,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input0, input1},
		Outputs:  []*Buffer{NewBuffer(16)},
	})

	rt.Start(ctx)

	barrier := &BarrierSignal{Epoch: 1, MinEpoch: 1, Timestamp: time.Now()}

	// Send data on input 0, then barrier on input 0.
	_ = input0.Send(ctx, NewDataMessage("0-before", nil, time.Now()))
	_ = input0.Send(ctx, NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}))
	// Data from input 0 after barrier — should be buffered.
	_ = input0.Send(ctx, NewDataMessage("0-after-barrier", nil, time.Now()))

	// Give runtime time to process input 0's messages.
	time.Sleep(100 * time.Millisecond)

	// Send data on input 1 — should be processed immediately (not barriered yet).
	_ = input1.Send(ctx, NewDataMessage("1-before", nil, time.Now()))

	// Give runtime time to process.
	time.Sleep(50 * time.Millisecond)

	// Send barrier on input 1 — triggers alignment.
	_ = input1.Send(ctx, NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}))

	// Send post-alignment data.
	_ = input1.Send(ctx, NewDataMessage("1-after", nil, time.Now()))

	time.Sleep(100 * time.Millisecond)

	// Close inputs.
	input0.Close()
	input1.Close()
	_ = rt.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Verify all messages were processed.
	if len(processedOrder) != 4 {
		t.Fatalf("expected 4 messages, got %d: %v", len(processedOrder), processedOrder)
	}

	// "0-before" must come before the checkpoint (before barrier).
	// "1-before" must come before the checkpoint (non-barriered input).
	// "0-after-barrier" was buffered and released after alignment.
	// "1-after" is post-alignment.
	// The exact interleaving depends on goroutine scheduling, but
	// "0-before" and "1-before" must both appear before "0-after-barrier".
	afterBarrierIdx := -1
	for i, key := range processedOrder {
		if key == "0-after-barrier" {
			afterBarrierIdx = i
			break
		}
	}
	if afterBarrierIdx == -1 {
		t.Fatal("0-after-barrier not found in processed order")
	}

	// Verify pre-barrier messages came before the buffered message.
	for i, key := range processedOrder[:afterBarrierIdx] {
		if key != "0-before" && key != "1-before" {
			t.Errorf("expected pre-barrier message at index %d, got %q", i, key)
		}
	}
}

// TestBarrierThenStop_DiamondGraph tests that a barrier with ThenStop=true
// causes clean shutdown of all nodes in a diamond graph.
func TestBarrierThenStop_DiamondGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodeB := newMockNode("B")
	nodeC := newMockNode("C")
	nodeD := newMockNode("D")

	// B and C forward data messages so D receives them.
	nodeB.processFunc = forwardingProcessFunc()
	nodeC.processFunc = forwardingProcessFunc()

	bufAB := NewBuffer(16)
	bufAC := NewBuffer(16)
	bufBD := NewBuffer(16)
	bufCD := NewBuffer(16)

	rtB := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeB,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAB},
		Outputs:  []*Buffer{bufBD},
	})
	rtC := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeC,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAC},
		Outputs:  []*Buffer{bufCD},
	})
	rtD := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeD,
		NodeType: NodeTypeSink,
		Inputs:   []*Buffer{bufBD, bufCD},
	})

	rtD.Start(ctx)
	rtB.Start(ctx)
	rtC.Start(ctx)

	// Send data then barrier with ThenStop=true.
	_ = bufAB.Send(ctx, NewDataMessage("b-data", nil, time.Now()))
	_ = bufAC.Send(ctx, NewDataMessage("c-data", nil, time.Now()))

	barrier := NewBarrierSignal(1, 1, true)
	_ = bufAB.Send(ctx, NewSignalMessage(barrier))
	_ = bufAC.Send(ctx, NewSignalMessage(barrier))

	// All nodes should shut down gracefully after the barrier propagates.
	// B and C shut down because of ThenStop after forwarding.
	errB := rtB.Wait()
	errC := rtC.Wait()

	if errB != nil && errB != context.Canceled {
		t.Errorf("B unexpected error: %v", errB)
	}
	if errC != nil && errC != context.Canceled {
		t.Errorf("C unexpected error: %v", errC)
	}

	// D should also shut down after receiving aligned barriers with ThenStop.
	errD := rtD.Wait()
	if errD != nil && errD != context.Canceled {
		t.Errorf("D unexpected error: %v", errD)
	}

	// Verify all nodes checkpointed.
	if nodeB.checkpointCount.Load() != 1 {
		t.Errorf("B: expected 1 checkpoint, got %d", nodeB.checkpointCount.Load())
	}
	if nodeC.checkpointCount.Load() != 1 {
		t.Errorf("C: expected 1 checkpoint, got %d", nodeC.checkpointCount.Load())
	}
	if nodeD.checkpointCount.Load() != 1 {
		t.Errorf("D: expected 1 checkpoint, got %d", nodeD.checkpointCount.Load())
	}

	// Verify data was processed before shutdown.
	if nodeB.processCount.Load() != 1 {
		t.Errorf("B: expected 1 process call, got %d", nodeB.processCount.Load())
	}
	if nodeC.processCount.Load() != 1 {
		t.Errorf("C: expected 1 process call, got %d", nodeC.processCount.Load())
	}
	if nodeD.processCount.Load() != 2 {
		t.Errorf("D: expected 2 process calls, got %d", nodeD.processCount.Load())
	}
}

// TestBarrierAlignment_MultipleEpochs verifies that the aligner handles
// sequential barrier epochs correctly.
func TestBarrierAlignment_MultipleEpochs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var checkpointEpochs []uint64
	var mu sync.Mutex

	node := newMockNode("merge")
	node.checkpointFunc = func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
		mu.Lock()
		checkpointEpochs = append(checkpointEpochs, barrier.Epoch)
		mu.Unlock()
		return nil, nil
	}

	input0 := NewBuffer(16)
	input1 := NewBuffer(16)
	output := NewBuffer(32)

	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     node,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{input0, input1},
		Outputs:  []*Buffer{output},
	})

	rt.Start(ctx)

	// Send 3 epochs of barriers, waiting for each alignment to complete
	// before sending the next to avoid interleaving barriers from different epochs.
	for epoch := uint64(1); epoch <= 3; epoch++ {
		barrier := &BarrierSignal{Epoch: epoch, MinEpoch: 1, Timestamp: time.Now()}
		_ = input0.Send(ctx, NewDataMessage(fmt.Sprintf("e%d-0", epoch), nil, time.Now()))
		_ = input1.Send(ctx, NewDataMessage(fmt.Sprintf("e%d-1", epoch), nil, time.Now()))
		_ = input0.Send(ctx, NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}))
		_ = input1.Send(ctx, NewSignalMessage(SignalMessage{SignalType: SignalTypeBarrier, Barrier: barrier}))
		// Wait for alignment to complete before sending next epoch.
		time.Sleep(100 * time.Millisecond)
	}

	input0.Close()
	input1.Close()
	_ = rt.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(checkpointEpochs) != 3 {
		t.Fatalf("expected 3 checkpoints, got %d: %v", len(checkpointEpochs), checkpointEpochs)
	}
	for i, epoch := range checkpointEpochs {
		if epoch != uint64(i+1) {
			t.Errorf("checkpoint %d: expected epoch %d, got %d", i, i+1, epoch)
		}
	}
}

// TestBarrierPropagation_SourceInjectsBarrier tests that source nodes inject
// barriers via checkpoint request control messages.
func TestBarrierPropagation_SourceInjectsBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodeA := newMockNode("source")
	var capturedEpoch atomic.Uint64
	nodeA.checkpointFunc = func(ctx context.Context, barrier BarrierSignal) ([]byte, error) {
		capturedEpoch.Store(barrier.Epoch)
		return []byte("source-state"), nil
	}

	output := NewBuffer(16)
	rt := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeA,
		NodeType: NodeTypeSource,
		Outputs:  []*Buffer{output},
	})

	rt.Start(ctx)

	// Controller sends checkpoint request to source.
	_ = rt.SendControl(ctx, NewCheckpointRequestControl(42, 1, false))
	time.Sleep(100 * time.Millisecond)

	rt.Stop()
	_ = rt.Wait()

	if capturedEpoch.Load() != 42 {
		t.Errorf("expected epoch 42, got %d", capturedEpoch.Load())
	}

	// Verify barrier was injected into output.
	msg, err := output.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !msg.IsSignal() || msg.Signal.SignalType != SignalTypeBarrier || msg.Signal.Barrier.Epoch != 42 {
		t.Error("expected barrier with epoch 42 in output")
	}
}

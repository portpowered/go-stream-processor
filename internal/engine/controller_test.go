package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestController_CheckpointCompletionTracking(t *testing.T) {
	var completedEpoch uint64
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B", "C"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"C"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			completedEpoch = epoch
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Report completions from all three nodes.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "C", Epoch: 1}

	// Give the controller time to process.
	time.Sleep(50 * time.Millisecond)

	info, ok := ctrl.GetCheckpoint(1)
	if !ok {
		t.Fatal("expected checkpoint 1 to exist")
	}
	if info.Status != CheckpointComplete {
		t.Errorf("expected checkpoint complete, got %d", info.Status)
	}
	if completedEpoch != 1 {
		t.Errorf("expected OnCheckpointComplete with epoch 1, got %d", completedEpoch)
	}
}

func TestController_PartialCompletionNotComplete(t *testing.T) {
	var completed bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B", "C"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"C"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			completed = true
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Only report from 2 of 3 nodes.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1}

	time.Sleep(50 * time.Millisecond)

	info, ok := ctrl.GetCheckpoint(1)
	if !ok {
		t.Fatal("expected checkpoint 1 to exist")
	}
	if info.Status != CheckpointInProgress {
		t.Error("expected checkpoint still in progress")
	}
	if completed {
		t.Error("should not have called OnCheckpointComplete yet")
	}
}

func TestController_QuiescenceDetection_5PlusNodes(t *testing.T) {
	// Test: complete graph quiescence with 5+ nodes.
	// Graph: S1 -> A -> B -> C -> K (sink)
	//        S2 -> A
	nodeIDs := []string{"S1", "S2", "A", "B", "C", "K"}
	sourceIDs := []string{"S1", "S2"}
	sinkIDs := []string{"K"}

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   nodeIDs,
		SourceIDs: sourceIDs,
		SinkIDs:   sinkIDs,
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Initiate a checkpoint manually (simulate).
	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch:          1,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// All nodes complete the checkpoint.
	for _, id := range nodeIDs {
		ctrl.CompletionChan() <- NodeCompletion{NodeID: id, Epoch: 1}
	}

	time.Sleep(50 * time.Millisecond)

	// Not quiescent yet — nodes haven't stopped.
	if quiescent.Load() {
		t.Error("should not be quiescent before nodes stop")
	}

	// All nodes report stopping.
	for _, id := range nodeIDs {
		ctrl.StoppedChan() <- NodeStopped{NodeID: id}
	}

	time.Sleep(50 * time.Millisecond)

	if !quiescent.Load() {
		t.Error("expected quiescence after all nodes stopped and checkpoint completed")
	}
	if !ctrl.IsQuiescent() {
		t.Error("IsQuiescent() should return true")
	}
}

func TestController_QuiescenceWithAmplification(t *testing.T) {
	// Test: quiescence with a node that produces multiple outputs per input.
	// Graph: S -> Amplifier -> Sink
	// Amplifier emits 3 messages per input. Quiescence should still work.
	nodeIDs := []string{"S", "Amp", "Sink"}

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   nodeIDs,
		SourceIDs: []string{"S"},
		SinkIDs:   []string{"Sink"},
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch:          1,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// All nodes complete checkpoint.
	for _, id := range nodeIDs {
		ctrl.CompletionChan() <- NodeCompletion{NodeID: id, Epoch: 1}
	}
	// All nodes stop.
	for _, id := range nodeIDs {
		ctrl.StoppedChan() <- NodeStopped{NodeID: id}
	}

	time.Sleep(50 * time.Millisecond)

	if !quiescent.Load() {
		t.Error("expected quiescence even with amplification")
	}
}

func TestController_NoPrematureQuiescence(t *testing.T) {
	// Test: no premature quiescence when messages are still in-flight.
	// Quiescence requires both: all nodes stopped AND at least one completed checkpoint.
	nodeIDs := []string{"S", "A", "Sink"}

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   nodeIDs,
		SourceIDs: []string{"S"},
		SinkIDs:   []string{"Sink"},
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Scenario 1: Nodes stopped but no checkpoint completed.
	for _, id := range nodeIDs {
		ctrl.StoppedChan() <- NodeStopped{NodeID: id}
	}

	time.Sleep(50 * time.Millisecond)

	if quiescent.Load() {
		t.Error("should not be quiescent without completed checkpoint")
	}

	// Scenario 2: Checkpoint in progress but incomplete.
	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch:          1,
		Status:         CheckpointInProgress,
		StartedAt:      time.Now(),
		CompletedNodes: make(map[string]bool),
		NodeStates:     make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// Only 2 of 3 nodes complete.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "S", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}

	time.Sleep(50 * time.Millisecond)

	if quiescent.Load() {
		t.Error("should not be quiescent with incomplete checkpoint")
	}

	// Complete the last node.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "Sink", Epoch: 1}

	time.Sleep(50 * time.Millisecond)

	if !quiescent.Load() {
		t.Error("expected quiescence after final node completes checkpoint")
	}
}

func TestController_IntegrationLinearGraph(t *testing.T) {
	// Integration test: linear graph A -> B -> C -> D -> E (5 nodes)
	// with actual NodeRuntimes wired to report to the controller.
	nodeA := newMockNode("A")
	nodeB := newMockNode("B")
	nodeC := newMockNode("C")
	nodeD := newMockNode("D")
	nodeE := newMockNode("E")

	// Forward data messages (passthrough nodes).
	passthrough := func(ctx context.Context, msg DataMessage, emit Emitter) error {
		return emit.Emit(ctx, NewDataMessage(msg.Key, msg.Value, msg.EventTime))
	}
	nodeB.processFunc = passthrough
	nodeC.processFunc = passthrough
	nodeD.processFunc = passthrough

	var quiescent atomic.Bool
	var checkpointDone atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B", "C", "D", "E"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"E"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			checkpointDone.Store(true)
		},
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	// Create buffers for the linear chain.
	bufAB := NewBuffer(16)
	bufBC := NewBuffer(16)
	bufCD := NewBuffer(16)
	bufDE := NewBuffer(16)

	completionCh := ctrl.CompletionChan()
	stoppedCh := ctrl.StoppedChan()

	rtA := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeA, NodeType: NodeTypeSource,
		Outputs:      []*Buffer{bufAB},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtB := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeB, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufAB}, Outputs: []*Buffer{bufBC},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtC := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeC, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufBC}, Outputs: []*Buffer{bufCD},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtD := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeD, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufCD}, Outputs: []*Buffer{bufDE},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtE := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeE, NodeType: NodeTypeSink,
		Inputs:       []*Buffer{bufDE},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl.Start(ctx)

	// Start all runtimes.
	rtA.Start(ctx)
	rtB.Start(ctx)
	rtC.Start(ctx)
	rtD.Start(ctx)
	rtE.Start(ctx)

	// Send some data through the source.
	for i := 0; i < 3; i++ {
		_ = bufAB.Send(ctx, NewDataMessage("k", i, time.Now()))
	}

	// Initiate a checkpoint via the controller.
	if err := ctrl.InitiateCheckpoint(ctx, 1, false, []*NodeRuntime{rtA}); err != nil {
		t.Fatal(err)
	}

	// Wait for checkpoint to complete.
	deadline := time.After(3 * time.Second)
	for !checkpointDone.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for checkpoint completion")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify checkpoint is complete.
	info, ok := ctrl.GetCheckpoint(1)
	if !ok {
		t.Fatal("checkpoint 1 not found")
	}
	if info.Status != CheckpointComplete {
		t.Error("expected checkpoint complete")
	}
	if len(info.CompletedNodes) != 5 {
		t.Errorf("expected 5 completed nodes, got %d", len(info.CompletedNodes))
	}

	// Now send stop signal to the downstream nodes and stop the source.
	_ = bufAB.Send(ctx, NewSignalMessage(NewStopSignal()))
	rtA.Stop() // Source has no inputs, so stop it explicitly.

	// Wait for quiescence.
	deadline = time.After(3 * time.Second)
	for !quiescent.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for quiescence")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Wait for all runtimes to exit.
	var wg sync.WaitGroup
	for _, rt := range []*NodeRuntime{rtA, rtB, rtC, rtD, rtE} {
		wg.Add(1)
		go func(r *NodeRuntime) {
			defer wg.Done()
			_ = r.Wait()
		}(rt)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for runtimes to exit")
	}

	ctrl.Stop()

	// Verify data was processed.
	if nodeE.processCount.Load() != 3 {
		t.Errorf("expected sink to process 3 messages, got %d", nodeE.processCount.Load())
	}
}

func TestController_IntegrationDiamondGraphQuiescence(t *testing.T) {
	// Diamond graph: S -> B, S -> C, B -> D, C -> D
	// Tests quiescence with barrier alignment at merge node D.
	nodeS := newMockNode("S")
	nodeB := newMockNode("B")
	nodeC := newMockNode("C")
	nodeD := newMockNode("D")
	nodeSink := newMockNode("Sink")

	passthrough := func(ctx context.Context, msg DataMessage, emit Emitter) error {
		return emit.Emit(ctx, NewDataMessage(msg.Key, msg.Value, msg.EventTime))
	}
	nodeB.processFunc = passthrough
	nodeC.processFunc = passthrough
	nodeD.processFunc = passthrough

	var quiescent atomic.Bool
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"S", "B", "C", "D", "Sink"},
		SourceIDs: []string{"S"},
		SinkIDs:   []string{"Sink"},
		OnQuiescence: func() {
			quiescent.Store(true)
		},
	})

	// Buffers.
	bufSB := NewBuffer(16)
	bufSC := NewBuffer(16)
	bufBD := NewBuffer(16)
	bufCD := NewBuffer(16)
	bufDSink := NewBuffer(16)

	completionCh := ctrl.CompletionChan()
	stoppedCh := ctrl.StoppedChan()

	rtS := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeS, NodeType: NodeTypeSource,
		Outputs:      []*Buffer{bufSB, bufSC},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtB := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeB, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufSB}, Outputs: []*Buffer{bufBD},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtC := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeC, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufSC}, Outputs: []*Buffer{bufCD},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtD := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeD, NodeType: NodeTypeOperator,
		Inputs: []*Buffer{bufBD, bufCD}, Outputs: []*Buffer{bufDSink},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})
	rtSink := NewNodeRuntime(NodeRuntimeConfig{
		Node: nodeSink, NodeType: NodeTypeSink,
		Inputs:       []*Buffer{bufDSink},
		CompletionCh: completionCh, StoppedCh: stoppedCh,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl.Start(ctx)

	rtS.Start(ctx)
	rtB.Start(ctx)
	rtC.Start(ctx)
	rtD.Start(ctx)
	rtSink.Start(ctx)

	// Send data through the source (broadcasts to both outputs).
	for i := 0; i < 2; i++ {
		msg := NewDataMessage("k", i, time.Now())
		_ = bufSB.Send(ctx, msg)
		_ = bufSC.Send(ctx, msg)
	}

	// Initiate a checkpoint with then_stop to trigger quiescence.
	if err := ctrl.InitiateCheckpoint(ctx, 1, true, []*NodeRuntime{rtS}); err != nil {
		t.Fatal(err)
	}

	// Wait for quiescence.
	deadline := time.After(3 * time.Second)
	for !quiescent.Load() {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for quiescence")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Wait for all runtimes.
	for _, rt := range []*NodeRuntime{rtS, rtB, rtC, rtD, rtSink} {
		_ = rt.Wait()
	}

	ctrl.Stop()

	// Node D should have processed 4 messages (2 from B, 2 from C).
	if nodeD.processCount.Load() != 4 {
		t.Errorf("expected D to process 4 messages, got %d", nodeD.processCount.Load())
	}
	// Sink should have received the processed messages.
	if nodeSink.processCount.Load() != 4 {
		t.Errorf("expected Sink to process 4 messages, got %d", nodeSink.processCount.Load())
	}
}

func TestController_MultipleCheckpointEpochs(t *testing.T) {
	var completedEpochs []uint64
	var mu sync.Mutex
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"B"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			mu.Lock()
			completedEpochs = append(completedEpochs, epoch)
			mu.Unlock()
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Complete epoch 1.
	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch: 1, Status: CheckpointInProgress,
		StartedAt: time.Now(), CompletedNodes: make(map[string]bool), NodeStates: make(map[string][]byte),
	}
	ctrl.checkpoints[2] = &CheckpointInfo{
		Epoch: 2, Status: CheckpointInProgress,
		StartedAt: time.Now(), CompletedNodes: make(map[string]bool), NodeStates: make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 2}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 2}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(completedEpochs) != 2 {
		t.Fatalf("expected 2 completed epochs, got %d", len(completedEpochs))
	}

	info1, _ := ctrl.GetCheckpoint(1)
	info2, _ := ctrl.GetCheckpoint(2)
	if info1.Status != CheckpointComplete || info2.Status != CheckpointComplete {
		t.Error("expected both checkpoints complete")
	}
}

func TestController_NodeStatesPopulated(t *testing.T) {
	completed := make(chan *CheckpointInfo, 1)
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B", "C"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"C"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			// Copy the info to inspect after callback.
			cp := *info
			cp.NodeStates = make(map[string][]byte, len(info.NodeStates))
			for k, v := range info.NodeStates {
				cp.NodeStates[k] = v
			}
			completed <- &cp
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	// Report completions with state bytes from all nodes.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1, State: []byte("state-A")}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1, State: nil}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "C", Epoch: 1, State: []byte("state-C")}

	var capturedInfo *CheckpointInfo
	select {
	case capturedInfo = <-completed:
	case <-ctx.Done():
		t.Fatal("expected OnCheckpointComplete to be called")
	}

	// Verify NodeStates has entries for all nodes.
	if len(capturedInfo.NodeStates) != 3 {
		t.Fatalf("expected 3 NodeStates entries, got %d", len(capturedInfo.NodeStates))
	}

	// Node A: non-nil state.
	if got := string(capturedInfo.NodeStates["A"]); got != "state-A" {
		t.Errorf("expected NodeStates[A] = %q, got %q", "state-A", got)
	}

	// Node B: nil state (entry present but nil).
	if state, ok := capturedInfo.NodeStates["B"]; !ok {
		t.Error("expected NodeStates[B] to exist (not omitted)")
	} else if state != nil {
		t.Errorf("expected NodeStates[B] = nil, got %v", state)
	}

	// Node C: non-nil state.
	if got := string(capturedInfo.NodeStates["C"]); got != "state-C" {
		t.Errorf("expected NodeStates[C] = %q, got %q", "state-C", got)
	}

	// Verify via GetCheckpoint too.
	info, ok := ctrl.GetCheckpoint(1)
	if !ok {
		t.Fatal("expected checkpoint 1 to exist")
	}
	if len(info.NodeStates) != 3 {
		t.Errorf("GetCheckpoint: expected 3 NodeStates entries, got %d", len(info.NodeStates))
	}
}

func TestController_DuplicateCompletionIgnored(t *testing.T) {
	var callCount atomic.Int64
	ctrl := NewController(ControllerConfig{
		NodeIDs:   []string{"A", "B"},
		SourceIDs: []string{"A"},
		SinkIDs:   []string{"B"},
		OnCheckpointComplete: func(epoch uint64, info *CheckpointInfo) {
			callCount.Add(1)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ctrl.Start(ctx)
	defer ctrl.Stop()

	ctrl.mu.Lock()
	ctrl.checkpoints[1] = &CheckpointInfo{
		Epoch: 1, Status: CheckpointInProgress,
		StartedAt: time.Now(), CompletedNodes: make(map[string]bool), NodeStates: make(map[string][]byte),
	}
	ctrl.mu.Unlock()

	// Complete, then send duplicate.
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "B", Epoch: 1}
	ctrl.CompletionChan() <- NodeCompletion{NodeID: "A", Epoch: 1} // duplicate

	time.Sleep(50 * time.Millisecond)

	if callCount.Load() != 1 {
		t.Errorf("expected OnCheckpointComplete called once, got %d", callCount.Load())
	}
}

package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- WatermarkHolder unit tests ---

func TestWatermarkHolder_SingleInput_Advances(t *testing.T) {
	wh := NewWatermarkHolder(1)

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)

	// First watermark should advance.
	result := wh.Update(0, WatermarkSignal{EventTime: t1})
	if result == nil {
		t.Fatal("expected watermark advancement, got nil")
	}
	if !result.EventTime.Equal(t1) {
		t.Fatalf("expected %v, got %v", t1, result.EventTime)
	}

	// Second watermark with later time should advance.
	result = wh.Update(0, WatermarkSignal{EventTime: t2})
	if result == nil {
		t.Fatal("expected watermark advancement, got nil")
	}
	if !result.EventTime.Equal(t2) {
		t.Fatalf("expected %v, got %v", t2, result.EventTime)
	}
}

func TestWatermarkHolder_SingleInput_NoRegression(t *testing.T) {
	wh := NewWatermarkHolder(1)

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) // earlier

	wh.Update(0, WatermarkSignal{EventTime: t1})

	// Watermark going backwards should not advance.
	result := wh.Update(0, WatermarkSignal{EventTime: t2})
	if result != nil {
		t.Fatalf("expected no advancement for earlier watermark, got %v", result.EventTime)
	}

	// Current should still be t1.
	if !wh.Current().Equal(t1) {
		t.Fatalf("expected current %v, got %v", t1, wh.Current())
	}
}

func TestWatermarkHolder_MultiInput_MinTracking(t *testing.T) {
	wh := NewWatermarkHolder(2)

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)

	// First input reports — should not advance yet (waiting for all inputs).
	result := wh.Update(0, WatermarkSignal{EventTime: t2})
	if result != nil {
		t.Fatal("expected no advancement before all inputs report")
	}

	// Second input reports with earlier time — should advance to min(t2, t1) = t1.
	result = wh.Update(1, WatermarkSignal{EventTime: t1})
	if result == nil {
		t.Fatal("expected watermark advancement after all inputs report")
	}
	if !result.EventTime.Equal(t1) {
		t.Fatalf("expected min watermark %v, got %v", t1, result.EventTime)
	}

	// Input 1 advances to t3 — output should stay at t2 (held back by input 0).
	result = wh.Update(1, WatermarkSignal{EventTime: t3})
	if result == nil {
		t.Fatal("expected watermark advancement")
	}
	if !result.EventTime.Equal(t2) {
		t.Fatalf("expected min watermark %v, got %v", t2, result.EventTime)
	}

	// Input 0 advances to t3 — output should now advance to t3.
	result = wh.Update(0, WatermarkSignal{EventTime: t3})
	if result == nil {
		t.Fatal("expected watermark advancement")
	}
	if !result.EventTime.Equal(t3) {
		t.Fatalf("expected min watermark %v, got %v", t3, result.EventTime)
	}
}

func TestWatermarkHolder_MultiInput_HeldBySlowInput(t *testing.T) {
	wh := NewWatermarkHolder(3)

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t5 := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	t10 := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)

	// Two fast inputs, one slow.
	wh.Update(0, WatermarkSignal{EventTime: t10})
	wh.Update(1, WatermarkSignal{EventTime: t5})
	result := wh.Update(2, WatermarkSignal{EventTime: t1}) // slow input
	if result == nil {
		t.Fatal("expected watermark advancement")
	}
	if !result.EventTime.Equal(t1) {
		t.Fatalf("expected watermark held back to %v by slow input, got %v", t1, result.EventTime)
	}
}

func TestWatermarkHolder_IdleInput_ExcludedFromMin(t *testing.T) {
	wh := NewWatermarkHolder(2)

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t5 := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)

	// Input 0 reports event time.
	wh.Update(0, WatermarkSignal{EventTime: t5})

	// Input 1 goes idle (with an earlier time) — should not hold back the watermark.
	result := wh.Update(1, WatermarkSignal{EventTime: t1, Idle: true})
	if result == nil {
		t.Fatal("expected watermark advancement")
	}
	// Idle input excluded from min, so output should be t5.
	if !result.EventTime.Equal(t5) {
		t.Fatalf("expected %v (idle input excluded), got %v", t5, result.EventTime)
	}
}

func TestWatermarkHolder_AllIdle_AdvancesToMax(t *testing.T) {
	wh := NewWatermarkHolder(2)

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t5 := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)

	wh.Update(0, WatermarkSignal{EventTime: t1, Idle: true})
	result := wh.Update(1, WatermarkSignal{EventTime: t5, Idle: true})
	if result == nil {
		t.Fatal("expected watermark advancement when all idle")
	}
	if !result.EventTime.Equal(t5) {
		t.Fatalf("expected max %v when all idle, got %v", t5, result.EventTime)
	}
	if !result.Idle {
		t.Fatal("expected idle flag when all inputs idle")
	}
}

func TestWatermarkHolder_InvalidIndex(t *testing.T) {
	wh := NewWatermarkHolder(2)
	result := wh.Update(-1, WatermarkSignal{EventTime: time.Now()})
	if result != nil {
		t.Fatal("expected nil for invalid index")
	}
	result = wh.Update(5, WatermarkSignal{EventTime: time.Now()})
	if result != nil {
		t.Fatal("expected nil for out-of-range index")
	}
}

// --- Integration tests with NodeRuntime ---

// watermarkRecorder is a test node that records watermark callbacks.
type watermarkRecorder struct {
	id         string
	mu         sync.Mutex
	watermarks []WatermarkSignal
	data       []DataMessage
}

func (n *watermarkRecorder) ID() string { return n.id }
func (n *watermarkRecorder) ProcessMessage(_ context.Context, msg DataMessage, _ Emitter) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.data = append(n.data, msg)
	return nil
}
func (n *watermarkRecorder) HandleWatermark(_ context.Context, wm WatermarkSignal, _ Emitter) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.watermarks = append(n.watermarks, wm)
	return nil
}
func (n *watermarkRecorder) HandleCheckpoint(_ context.Context, _ BarrierSignal) ([]byte, error) {
	return nil, nil
}

func (n *watermarkRecorder) getWatermarks() []WatermarkSignal {
	n.mu.Lock()
	defer n.mu.Unlock()
	cp := make([]WatermarkSignal, len(n.watermarks))
	copy(cp, n.watermarks)
	return cp
}

func TestWatermark_LinearGraph_Propagation(t *testing.T) {
	// Linear graph: Source -> A -> B -> Sink
	// Source emits watermarks, verify they propagate through the graph.

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)

	nodeA := &watermarkRecorder{id: "A"}
	nodeB := &watermarkRecorder{id: "B"}

	// Buffers: source->A, A->B, B->sink
	bufSourceA := NewBuffer(16)
	bufAB := NewBuffer(16)
	bufBSink := NewBuffer(16)

	runtimeA := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeA,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufSourceA},
		Outputs:  []*Buffer{bufAB},
	})

	runtimeB := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeB,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufAB},
		Outputs:  []*Buffer{bufBSink},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runtimeA.Start(ctx)
	runtimeB.Start(ctx)

	// Send watermark t1 from source.
	wm1 := NewSignalMessage(NewWatermarkSignal(t1))
	if err := bufSourceA.Send(ctx, wm1); err != nil {
		t.Fatal(err)
	}

	// Send watermark t2 from source.
	wm2 := NewSignalMessage(NewWatermarkSignal(t2))
	if err := bufSourceA.Send(ctx, wm2); err != nil {
		t.Fatal(err)
	}

	// Send stop signal so nodes terminate.
	stop := NewSignalMessage(NewStopSignal())
	if err := bufSourceA.Send(ctx, stop); err != nil {
		t.Fatal(err)
	}

	runtimeA.Wait()
	runtimeB.Wait()

	// Node A should have received watermark callbacks for t1 and t2.
	aWMs := nodeA.getWatermarks()
	if len(aWMs) != 2 {
		t.Fatalf("node A: expected 2 watermark callbacks, got %d", len(aWMs))
	}
	if !aWMs[0].EventTime.Equal(t1) {
		t.Fatalf("node A watermark[0]: expected %v, got %v", t1, aWMs[0].EventTime)
	}
	if !aWMs[1].EventTime.Equal(t2) {
		t.Fatalf("node A watermark[1]: expected %v, got %v", t2, aWMs[1].EventTime)
	}

	// Node B should also have received watermarks (forwarded by A).
	bWMs := nodeB.getWatermarks()
	if len(bWMs) != 2 {
		t.Fatalf("node B: expected 2 watermark callbacks, got %d", len(bWMs))
	}
	if !bWMs[0].EventTime.Equal(t1) {
		t.Fatalf("node B watermark[0]: expected %v, got %v", t1, bWMs[0].EventTime)
	}
	if !bWMs[1].EventTime.Equal(t2) {
		t.Fatalf("node B watermark[1]: expected %v, got %v", t2, bWMs[1].EventTime)
	}
}

func TestWatermark_MergeNode_HeldBySlowInput(t *testing.T) {
	// Diamond graph:
	//   Source -> A (fast) -\
	//                        -> D (merge node)
	//   Source -> B (slow) -/
	//
	// D has two inputs. The output watermark should be held back by the
	// slower input (B).

	t1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	t5 := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	t10 := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)

	nodeD := &watermarkRecorder{id: "D"}

	// Two input buffers for node D.
	bufFastD := NewBuffer(16) // input 0 (fast)
	bufSlowD := NewBuffer(16) // input 1 (slow)
	bufDSink := NewBuffer(16) // output

	runtimeD := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeD,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufFastD, bufSlowD},
		Outputs:  []*Buffer{bufDSink},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runtimeD.Start(ctx)

	// Fast input sends watermark t10.
	wm10 := NewSignalMessage(NewWatermarkSignal(t10))
	if err := bufFastD.Send(ctx, wm10); err != nil {
		t.Fatal(err)
	}

	// Give runtime a moment to process.
	time.Sleep(50 * time.Millisecond)

	// Node D should NOT have received a watermark yet (waiting for slow input).
	dWMs := nodeD.getWatermarks()
	if len(dWMs) != 0 {
		t.Fatalf("node D: expected 0 watermarks before slow input reports, got %d", len(dWMs))
	}

	// Slow input sends watermark t1.
	wm1 := NewSignalMessage(NewWatermarkSignal(t1))
	if err := bufSlowD.Send(ctx, wm1); err != nil {
		t.Fatal(err)
	}

	// Give runtime a moment to process.
	time.Sleep(50 * time.Millisecond)

	// Node D should now have received watermark t1 (min of t10, t1).
	dWMs = nodeD.getWatermarks()
	if len(dWMs) != 1 {
		t.Fatalf("node D: expected 1 watermark, got %d", len(dWMs))
	}
	if !dWMs[0].EventTime.Equal(t1) {
		t.Fatalf("node D: expected watermark held back to %v, got %v", t1, dWMs[0].EventTime)
	}

	// Slow input advances to t5.
	wm5 := NewSignalMessage(NewWatermarkSignal(t5))
	if err := bufSlowD.Send(ctx, wm5); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Output watermark should advance to t5 (min of t10, t5).
	dWMs = nodeD.getWatermarks()
	if len(dWMs) != 2 {
		t.Fatalf("node D: expected 2 watermarks, got %d", len(dWMs))
	}
	if !dWMs[1].EventTime.Equal(t5) {
		t.Fatalf("node D: expected watermark %v, got %v", t5, dWMs[1].EventTime)
	}

	// Verify watermark was forwarded to output buffer.
	// Read from the sink buffer to check the forwarded watermarks.
	var sinkWMs []WatermarkSignal
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout reading from sink buffer")
		default:
		}
		if bufDSink.Len() == 0 {
			break
		}
		msg, err := bufDSink.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.IsSignal() && msg.Signal != nil && msg.Signal.SignalType == SignalTypeWatermark {
			sinkWMs = append(sinkWMs, *msg.Signal.Watermark)
		}
	}

	if len(sinkWMs) != 2 {
		t.Fatalf("sink: expected 2 forwarded watermarks, got %d", len(sinkWMs))
	}
	if !sinkWMs[0].EventTime.Equal(t1) {
		t.Fatalf("sink watermark[0]: expected %v, got %v", t1, sinkWMs[0].EventTime)
	}
	if !sinkWMs[1].EventTime.Equal(t5) {
		t.Fatalf("sink watermark[1]: expected %v, got %v", t5, sinkWMs[1].EventTime)
	}

	// Clean up.
	cancel()
	runtimeD.Wait()
}

func TestWatermark_IdleSource_DoesNotHoldBack(t *testing.T) {
	// Two inputs to a merge node. One goes idle — the non-idle input's
	// watermark should advance the output.

	t5 := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	t10 := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)

	nodeM := &watermarkRecorder{id: "M"}

	bufA := NewBuffer(16) // input 0
	bufB := NewBuffer(16) // input 1 (will go idle)
	bufOut := NewBuffer(16)

	runtimeM := NewNodeRuntime(NodeRuntimeConfig{
		Node:     nodeM,
		NodeType: NodeTypeOperator,
		Inputs:   []*Buffer{bufA, bufB},
		Outputs:  []*Buffer{bufOut},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runtimeM.Start(ctx)

	// Input 0 sends watermark t5.
	if err := bufA.Send(ctx, NewSignalMessage(NewWatermarkSignal(t5))); err != nil {
		t.Fatal(err)
	}

	// Input 1 goes idle.
	if err := bufB.Send(ctx, NewSignalMessage(NewIdleWatermarkSignal())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Output watermark should be t5 (idle input excluded from min).
	wms := nodeM.getWatermarks()
	if len(wms) != 1 {
		t.Fatalf("expected 1 watermark, got %d", len(wms))
	}
	if !wms[0].EventTime.Equal(t5) {
		t.Fatalf("expected %v, got %v", t5, wms[0].EventTime)
	}

	// Input 0 advances to t10, input 1 still idle.
	if err := bufA.Send(ctx, NewSignalMessage(NewWatermarkSignal(t10))); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	wms = nodeM.getWatermarks()
	if len(wms) != 2 {
		t.Fatalf("expected 2 watermarks, got %d", len(wms))
	}
	if !wms[1].EventTime.Equal(t10) {
		t.Fatalf("expected %v, got %v", t10, wms[1].EventTime)
	}

	cancel()
	runtimeM.Wait()
}

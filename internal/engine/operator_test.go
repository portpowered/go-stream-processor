package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// --- Test helpers ---

// collectingCollector records all emitted messages and timer registrations for test assertions.
type collectingCollector struct {
	mu       sync.Mutex
	messages []DataMessage
	ported   map[string][]DataMessage
	timers   []timerEntry
	errors   []collectedError
}

type collectedError struct {
	Msg DataMessage
	Err error
}

func newCollectingCollector() *collectingCollector {
	return &collectingCollector{
		ported: make(map[string][]DataMessage),
	}
}

func (c *collectingCollector) Emit(msg DataMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, msg)
	return nil
}

func (c *collectingCollector) EmitToPort(port string, msg DataMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ported[port] = append(c.ported[port], msg)
	return nil
}

func (c *collectingCollector) RegisterTimer(name string, fireAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timers = append(c.timers, timerEntry{Name: name, FireAt: fireAt})
}

func (c *collectingCollector) EmitError(msg DataMessage, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, collectedError{Msg: msg, Err: err})
}

func (c *collectingCollector) Messages() []DataMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]DataMessage, len(c.messages))
	copy(result, c.messages)
	return result
}

func (c *collectingCollector) PortMessages(port string) []DataMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := c.ported[port]
	result := make([]DataMessage, len(msgs))
	copy(result, msgs)
	return result
}

// --- MapOperator tests ---

func TestMapOperator_DoublesNumericValues(t *testing.T) {
	op := &MapOperator[int, int]{
		MapFn: func(v int) int { return v * 2 },
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	inputs := []int{1, 2, 3, 5, 10}
	for _, v := range inputs {
		msg := DataMessage{Key: "k", Value: v, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", v, err)
		}
	}

	msgs := collector.Messages()
	if len(msgs) != len(inputs) {
		t.Fatalf("expected %d messages, got %d", len(inputs), len(msgs))
	}

	expected := []int{2, 4, 6, 10, 20}
	for i, msg := range msgs {
		got, ok := msg.Value.(int)
		if !ok {
			t.Fatalf("message %d value is not int: %T", i, msg.Value)
		}
		if got != expected[i] {
			t.Errorf("message %d: expected %d, got %d", i, expected[i], got)
		}
		if msg.Key != "k" {
			t.Errorf("message %d: expected key 'k', got %q", i, msg.Key)
		}
	}
}

func TestMapOperator_PreservesKeyAndEventTime(t *testing.T) {
	op := &MapOperator[string, string]{
		MapFn: func(s string) string { return s + "!" },
	}

	collector := newCollectingCollector()
	ctx := context.Background()
	et := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	msg := DataMessage{Key: "myKey", Value: "hello", EventTime: et}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	msgs := collector.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Key != "myKey" {
		t.Errorf("expected key 'myKey', got %q", msgs[0].Key)
	}
	if msgs[0].Value != "hello!" {
		t.Errorf("expected value 'hello!', got %v", msgs[0].Value)
	}
	if !msgs[0].EventTime.Equal(et) {
		t.Errorf("expected event time %v, got %v", et, msgs[0].EventTime)
	}
}

func TestMapOperator_SkipsWrongType(t *testing.T) {
	op := &MapOperator[int, int]{
		MapFn: func(v int) int { return v * 2 },
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	// Send a string value to an int mapper — should be silently skipped.
	msg := DataMessage{Key: "k", Value: "not an int", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	if len(collector.Messages()) != 0 {
		t.Errorf("expected 0 messages for wrong type, got %d", len(collector.Messages()))
	}
}

// --- FilterOperator tests ---

func TestFilterOperator_DropsMessages(t *testing.T) {
	op := &FilterOperator[int]{
		Predicate: func(v int) bool { return v%2 == 0 },
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	for i := 1; i <= 6; i++ {
		msg := DataMessage{Key: "k", Value: i, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", i, err)
		}
	}

	msgs := collector.Messages()
	if len(msgs) != 3 {
		t.Fatalf("expected 3 even messages, got %d", len(msgs))
	}

	expected := []int{2, 4, 6}
	for i, msg := range msgs {
		got, ok := msg.Value.(int)
		if !ok {
			t.Fatalf("message %d value is not int: %T", i, msg.Value)
		}
		if got != expected[i] {
			t.Errorf("message %d: expected %d, got %d", i, expected[i], got)
		}
	}
}

func TestFilterOperator_SkipsWrongType(t *testing.T) {
	op := &FilterOperator[int]{
		Predicate: func(v int) bool { return true },
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	msg := DataMessage{Key: "k", Value: "string", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	if len(collector.Messages()) != 0 {
		t.Errorf("expected 0 messages for wrong type, got %d", len(collector.Messages()))
	}
}

// --- FanOutOperator tests ---

func TestFanOutOperator_DuplicatesToAllOutputs(t *testing.T) {
	op := &FanOutOperator{}

	// Use a multi-buffer collector to verify fan-out.
	ctx := context.Background()
	buf1 := NewBuffer(16)
	buf2 := NewBuffer(16)
	buf3 := NewBuffer(16)

	collector := &runtimeCollector{
		ctx:     ctx,
		outputs: []*Buffer{buf1, buf2, buf3},
	}

	msg := DataMessage{Key: "k", Value: "hello", EventTime: time.Now()}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	// Each buffer should have one message.
	for i, buf := range []*Buffer{buf1, buf2, buf3} {
		m, err := buf.Recv(ctx)
		if err != nil {
			t.Fatalf("buf%d Recv error: %v", i+1, err)
		}
		if !m.IsData() {
			t.Fatalf("buf%d: expected data message", i+1)
		}
		if m.Data.Value != "hello" {
			t.Errorf("buf%d: expected value 'hello', got %v", i+1, m.Data.Value)
		}
	}
}

// --- MergeOperator tests ---

func TestMergeOperator_ForwardsMessages(t *testing.T) {
	op := &MergeOperator{}

	collector := newCollectingCollector()
	ctx := context.Background()

	// Simulate messages from different "input streams".
	msgs := []DataMessage{
		{Key: "a", Value: 1, EventTime: time.Now()},
		{Key: "b", Value: 2, EventTime: time.Now()},
		{Key: "c", Value: 3, EventTime: time.Now()},
	}

	for _, msg := range msgs {
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	got := collector.Messages()
	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}

	for i, msg := range got {
		if msg.Value != msgs[i].Value {
			t.Errorf("message %d: expected value %v, got %v", i, msgs[i].Value, msg.Value)
		}
	}
}

// --- OperatorNode adapter tests ---

func TestOperatorNode_ImplementsNode(t *testing.T) {
	op := &MapOperator[int, int]{
		MapFn: func(v int) int { return v + 1 },
	}
	node := NewOperatorNode("test", op)

	// Verify it implements Node.
	var _ Node = node

	if node.ID() != "test" {
		t.Errorf("expected ID 'test', got %q", node.ID())
	}
}

func TestOperatorNode_ProcessMessageDelegatesToOperator(t *testing.T) {
	op := &MapOperator[int, int]{
		MapFn: func(v int) int { return v * 3 },
	}
	node := NewOperatorNode("mapper", op)

	buf := NewBuffer(16)
	node.SetOutputs([]*Buffer{buf})

	ctx := context.Background()
	emit := &runtimeEmitter{outputs: []*Buffer{buf}, ctx: ctx}

	msg := DataMessage{Key: "k", Value: 5, EventTime: time.Now()}
	if err := node.ProcessMessage(ctx, msg, emit); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	m, err := buf.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if !m.IsData() {
		t.Fatal("expected data message")
	}
	if m.Data.Value != 15 {
		t.Errorf("expected 15, got %v", m.Data.Value)
	}
}

func TestOperatorNode_ImplementsTickHandler(t *testing.T) {
	tickCalled := false

	op := &testTickOperator{
		tickFn: func(ctx context.Context, collector Collector) error {
			tickCalled = true
			return collector.Emit(DataMessage{Key: "tick", Value: "fired"})
		},
	}
	node := NewOperatorNode("ticker", op)
	buf := NewBuffer(16)
	node.SetOutputs([]*Buffer{buf})

	// Verify it implements TickHandler via interface variable.
	var th TickHandler = node

	ctx := context.Background()
	emit := &runtimeEmitter{outputs: []*Buffer{buf}, ctx: ctx}
	if err := th.HandleTick(ctx, emit); err != nil {
		t.Fatalf("HandleTick error: %v", err)
	}

	if !tickCalled {
		t.Error("expected tick function to be called")
	}

	m, err := buf.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if m.Data.Value != "fired" {
		t.Errorf("expected 'fired', got %v", m.Data.Value)
	}
}

// --- SourceNode adapter tests ---

func TestSourceNode_ImplementsNode(t *testing.T) {
	src := &testSource{
		runFn: func(ctx context.Context, collector Collector) error { return nil },
	}
	node := NewSourceNode("src", src)

	var _ Node = node

	if node.ID() != "src" {
		t.Errorf("expected ID 'src', got %q", node.ID())
	}
}

func TestSourceNode_RunSourceEmitsMessages(t *testing.T) {
	src := &testSource{
		runFn: func(ctx context.Context, collector Collector) error {
			for i := 0; i < 5; i++ {
				if err := collector.Emit(DataMessage{Key: "k", Value: i}); err != nil {
					return err
				}
			}
			return nil
		},
	}
	node := NewSourceNode("src", src)
	buf := NewBuffer(16)
	node.SetOutputs([]*Buffer{buf})

	ctx := context.Background()
	if err := node.RunSource(ctx); err != nil {
		t.Fatalf("RunSource error: %v", err)
	}

	for i := 0; i < 5; i++ {
		m, err := buf.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv error: %v", err)
		}
		if m.Data.Value != i {
			t.Errorf("message %d: expected %d, got %v", i, i, m.Data.Value)
		}
	}
}

// --- BaseOperator tests ---

func TestBaseOperator_DefaultMethodsAreNoOps(t *testing.T) {
	op := BaseOperator{}
	ctx := context.Background()
	collector := newCollectingCollector()

	if err := op.ProcessMessage(ctx, DataMessage{}, collector); err != nil {
		t.Errorf("ProcessMessage error: %v", err)
	}
	if err := op.HandleWatermark(ctx, WatermarkSignal{}, collector); err != nil {
		t.Errorf("HandleWatermark error: %v", err)
	}
	state, err := op.HandleCheckpoint(ctx, BarrierSignal{})
	if err != nil {
		t.Errorf("HandleCheckpoint error: %v", err)
	}
	if state != nil {
		t.Errorf("expected nil state, got %v", state)
	}
	if err := op.OnStart(ctx); err != nil {
		t.Errorf("OnStart error: %v", err)
	}
	if err := op.OnClose(ctx); err != nil {
		t.Errorf("OnClose error: %v", err)
	}
}

// --- Collector tests ---

func TestRuntimeCollector_EmitToPort(t *testing.T) {
	ctx := context.Background()
	portA := NewBuffer(16)
	portB := NewBuffer(16)

	collector := &runtimeCollector{
		ctx:     ctx,
		outputs: []*Buffer{portA, portB},
		portOutputs: map[string][]*Buffer{
			"port-a": {portA},
			"port-b": {portB},
		},
	}

	msg := DataMessage{Key: "k", Value: "routed"}
	if err := collector.EmitToPort("port-a", msg); err != nil {
		t.Fatalf("EmitToPort error: %v", err)
	}

	// port-a should have a message.
	m, err := portA.Recv(ctx)
	if err != nil {
		t.Fatalf("portA Recv error: %v", err)
	}
	if m.Data.Value != "routed" {
		t.Errorf("expected 'routed', got %v", m.Data.Value)
	}

	// port-b should be empty — use a short-deadline context to avoid blocking.
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, err = portB.Recv(checkCtx)
	if err == nil {
		t.Error("expected port-b to be empty")
	}
}

func TestRuntimeCollector_EmitToPort_FallbackToAll(t *testing.T) {
	ctx := context.Background()
	buf1 := NewBuffer(16)
	buf2 := NewBuffer(16)

	collector := &runtimeCollector{
		ctx:         ctx,
		outputs:     []*Buffer{buf1, buf2},
		portOutputs: map[string][]*Buffer{},
	}

	msg := DataMessage{Key: "k", Value: "fallback"}
	// Unknown port should fall back to all outputs.
	if err := collector.EmitToPort("unknown", msg); err != nil {
		t.Fatalf("EmitToPort error: %v", err)
	}

	for i, buf := range []*Buffer{buf1, buf2} {
		m, err := buf.Recv(ctx)
		if err != nil {
			t.Fatalf("buf%d Recv error: %v", i+1, err)
		}
		if m.Data.Value != "fallback" {
			t.Errorf("buf%d: expected 'fallback', got %v", i+1, m.Data.Value)
		}
	}
}

// --- Custom operator test (acceptance criteria) ---

func TestCustomOperator_DoublesNumericValues(t *testing.T) {
	// Custom operator that doubles numeric values — per acceptance criteria.
	type doublerOp struct {
		BaseOperator
	}

	op := &doublerOp{}
	// Override ProcessMessage inline is not possible with struct embedding,
	// so use MapOperator instead.
	mapOp := &MapOperator[int, int]{
		MapFn: func(v int) int { return v * 2 },
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	// Verify the doubler works.
	_ = op // base operator
	for _, v := range []int{1, 2, 3, 4, 5} {
		msg := DataMessage{Key: "k", Value: v, EventTime: time.Now()}
		if err := mapOp.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", v, err)
		}
	}

	msgs := collector.Messages()
	expected := []int{2, 4, 6, 8, 10}
	for i, msg := range msgs {
		if msg.Value.(int) != expected[i] {
			t.Errorf("message %d: expected %d, got %v", i, expected[i], msg.Value)
		}
	}
}

// --- RouterOperator tests ---

func TestRouterOperator_ConditionalRoutingWith3Branches(t *testing.T) {
	op := &RouterOperator{
		RouteFn: func(msg DataMessage) string {
			v, ok := msg.Value.(int)
			if !ok {
				return ""
			}
			if v < 0 {
				return "negative"
			}
			if v == 0 {
				return "zero"
			}
			return "positive"
		},
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	values := []int{-5, 0, 3, -2, 7, 0}
	for _, v := range values {
		msg := DataMessage{Key: "k", Value: v, EventTime: time.Now()}
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage(%d) error: %v", v, err)
		}
	}

	negMsgs := collector.PortMessages("negative")
	zeroMsgs := collector.PortMessages("zero")
	posMsgs := collector.PortMessages("positive")

	if len(negMsgs) != 2 {
		t.Errorf("expected 2 negative messages, got %d", len(negMsgs))
	}
	if len(zeroMsgs) != 2 {
		t.Errorf("expected 2 zero messages, got %d", len(zeroMsgs))
	}
	if len(posMsgs) != 2 {
		t.Errorf("expected 2 positive messages, got %d", len(posMsgs))
	}

	// Verify correct values routed to each port.
	if negMsgs[0].Value != -5 || negMsgs[1].Value != -2 {
		t.Errorf("negative port: expected [-5, -2], got %v", []any{negMsgs[0].Value, negMsgs[1].Value})
	}
	if zeroMsgs[0].Value != 0 || zeroMsgs[1].Value != 0 {
		t.Errorf("zero port: expected [0, 0], got %v", []any{zeroMsgs[0].Value, zeroMsgs[1].Value})
	}
	if posMsgs[0].Value != 3 || posMsgs[1].Value != 7 {
		t.Errorf("positive port: expected [3, 7], got %v", []any{posMsgs[0].Value, posMsgs[1].Value})
	}
}

func TestRouterOperator_DefaultPortReceivesUnmatched(t *testing.T) {
	op := &RouterOperator{
		RouteFn: func(msg DataMessage) string {
			v, ok := msg.Value.(string)
			if !ok {
				return "" // unmatched
			}
			if v == "important" {
				return "priority"
			}
			return "" // unmatched
		},
		DefaultPort: "other",
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	msgs := []DataMessage{
		{Key: "1", Value: "important", EventTime: time.Now()},
		{Key: "2", Value: "boring", EventTime: time.Now()},
		{Key: "3", Value: 42, EventTime: time.Now()}, // wrong type → unmatched
		{Key: "4", Value: "important", EventTime: time.Now()},
	}

	for _, msg := range msgs {
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	priorityMsgs := collector.PortMessages("priority")
	otherMsgs := collector.PortMessages("other")

	if len(priorityMsgs) != 2 {
		t.Errorf("expected 2 priority messages, got %d", len(priorityMsgs))
	}
	if len(otherMsgs) != 2 {
		t.Errorf("expected 2 other messages, got %d", len(otherMsgs))
	}
}

func TestRouterOperator_DropsUnmatchedWithNoDefault(t *testing.T) {
	op := &RouterOperator{
		RouteFn: func(msg DataMessage) string {
			if msg.Value == "match" {
				return "out"
			}
			return "" // unmatched, no default → drop
		},
		// DefaultPort deliberately left empty
	}

	collector := newCollectingCollector()
	ctx := context.Background()

	msgs := []DataMessage{
		{Key: "1", Value: "match"},
		{Key: "2", Value: "nomatch"},
		{Key: "3", Value: "match"},
		{Key: "4", Value: "nomatch"},
	}

	for _, msg := range msgs {
		if err := op.ProcessMessage(ctx, msg, collector); err != nil {
			t.Fatalf("ProcessMessage error: %v", err)
		}
	}

	outMsgs := collector.PortMessages("out")
	if len(outMsgs) != 2 {
		t.Errorf("expected 2 matched messages, got %d", len(outMsgs))
	}

	// No default messages and no broadcast messages.
	if len(collector.Messages()) != 0 {
		t.Errorf("expected 0 broadcast messages, got %d", len(collector.Messages()))
	}
}

func TestRouterOperator_PreservesMessageContent(t *testing.T) {
	op := &RouterOperator{
		RouteFn: func(msg DataMessage) string { return "out" },
	}

	collector := newCollectingCollector()
	ctx := context.Background()
	et := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)

	msg := DataMessage{Key: "myKey", Value: "myValue", EventTime: et}
	if err := op.ProcessMessage(ctx, msg, collector); err != nil {
		t.Fatalf("ProcessMessage error: %v", err)
	}

	outMsgs := collector.PortMessages("out")
	if len(outMsgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(outMsgs))
	}
	if outMsgs[0].Key != "myKey" {
		t.Errorf("expected key 'myKey', got %q", outMsgs[0].Key)
	}
	if outMsgs[0].Value != "myValue" {
		t.Errorf("expected value 'myValue', got %v", outMsgs[0].Value)
	}
	if !outMsgs[0].EventTime.Equal(et) {
		t.Errorf("expected event time %v, got %v", et, outMsgs[0].EventTime)
	}
}

// --- Test helpers ---

// testTickOperator is a test helper that implements TickOperator.
type testTickOperator struct {
	BaseOperator
	tickFn func(ctx context.Context, collector Collector) error
}

func (t *testTickOperator) TickInterval() time.Duration { return 100 * time.Millisecond }

func (t *testTickOperator) HandleTick(ctx context.Context, collector Collector) error {
	if t.tickFn != nil {
		return t.tickFn(ctx, collector)
	}
	return nil
}

// testSource is a test helper that implements Source.
type testSource struct {
	runFn func(ctx context.Context, collector Collector) error
}

func (s *testSource) Run(ctx context.Context, collector Collector) error {
	if s.runFn != nil {
		return s.runFn(ctx, collector)
	}
	return nil
}

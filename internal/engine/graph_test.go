package engine

import (
	"context"
	"testing"
)

// testNode is a minimal Node implementation for testing graph construction.
type testNode struct {
	id string
}

func (n *testNode) ID() string { return n.id }
func (n *testNode) ProcessMessage(_ context.Context, _ DataMessage, _ Emitter) error {
	return nil
}
func (n *testNode) HandleWatermark(_ context.Context, _ WatermarkSignal, _ Emitter) error {
	return nil
}
func (n *testNode) HandleCheckpoint(_ context.Context, _ BarrierSignal) ([]byte, error) {
	return nil, nil
}

func newTestNode(id string) *testNode {
	return &testNode{id: id}
}

func TestNodeInterface(t *testing.T) {
	// Verify testNode satisfies the Node interface.
	var _ Node = &testNode{}

	node := newTestNode("test-1")
	if node.ID() != "test-1" {
		t.Errorf("expected ID 'test-1', got %q", node.ID())
	}

	// ProcessMessage should succeed with nil return.
	err := node.ProcessMessage(context.Background(), DataMessage{}, nil)
	if err != nil {
		t.Errorf("ProcessMessage returned unexpected error: %v", err)
	}

	// HandleWatermark should succeed with nil return.
	err = node.HandleWatermark(context.Background(), WatermarkSignal{}, nil)
	if err != nil {
		t.Errorf("HandleWatermark returned unexpected error: %v", err)
	}

	// HandleCheckpoint should succeed with nil state.
	state, err := node.HandleCheckpoint(context.Background(), BarrierSignal{})
	if err != nil {
		t.Errorf("HandleCheckpoint returned unexpected error: %v", err)
	}
	if state != nil {
		t.Errorf("expected nil state, got %v", state)
	}
}

func TestEdgeConstruction(t *testing.T) {
	t.Run("default ports", func(t *testing.T) {
		edge := NewEdge("A", "B")
		if edge.FromNode != "A" {
			t.Errorf("expected FromNode 'A', got %q", edge.FromNode)
		}
		if edge.ToNode != "B" {
			t.Errorf("expected ToNode 'B', got %q", edge.ToNode)
		}
		if edge.FromPort != DefaultPort {
			t.Errorf("expected FromPort %q, got %q", DefaultPort, edge.FromPort)
		}
		if edge.ToPort != DefaultPort {
			t.Errorf("expected ToPort %q, got %q", DefaultPort, edge.ToPort)
		}
	})

	t.Run("explicit ports", func(t *testing.T) {
		edge := NewPortEdge("A", "out-1", "B", "in-1")
		if edge.FromPort != "out-1" {
			t.Errorf("expected FromPort 'out-1', got %q", edge.FromPort)
		}
		if edge.ToPort != "in-1" {
			t.Errorf("expected ToPort 'in-1', got %q", edge.ToPort)
		}
	})
}

func TestGraphConstruction(t *testing.T) {
	t.Run("add nodes", func(t *testing.T) {
		g := NewGraph("test")
		if g.Name != "test" {
			t.Errorf("expected name 'test', got %q", g.Name)
		}

		err := g.AddNode(newTestNode("A"), NodeTypeSource)
		if err != nil {
			t.Fatalf("AddNode failed: %v", err)
		}

		err = g.AddNode(newTestNode("B"), NodeTypeSink)
		if err != nil {
			t.Fatalf("AddNode failed: %v", err)
		}

		if g.NodeCount() != 2 {
			t.Errorf("expected 2 nodes, got %d", g.NodeCount())
		}
	})

	t.Run("duplicate node ID rejected", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("A"), NodeTypeSource)
		err := g.AddNode(newTestNode("A"), NodeTypeSource)
		if err == nil {
			t.Error("expected error for duplicate node ID, got nil")
		}
	})

	t.Run("add edges", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("A"), NodeTypeSource)
		_ = g.AddNode(newTestNode("B"), NodeTypeSink)

		err := g.AddEdge(NewEdge("A", "B"))
		if err != nil {
			t.Fatalf("AddEdge failed: %v", err)
		}

		edges := g.Edges()
		if len(edges) != 1 {
			t.Fatalf("expected 1 edge, got %d", len(edges))
		}
	})

	t.Run("edge with missing source node rejected", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("B"), NodeTypeSink)

		err := g.AddEdge(NewEdge("A", "B"))
		if err == nil {
			t.Error("expected error for missing source node, got nil")
		}
	})

	t.Run("edge with missing destination node rejected", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("A"), NodeTypeSource)

		err := g.AddEdge(NewEdge("A", "B"))
		if err == nil {
			t.Error("expected error for missing destination node, got nil")
		}
	})
}

func TestGraphValidation(t *testing.T) {
	t.Run("valid linear graph", func(t *testing.T) {
		g := NewGraph("linear")
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddNode(newTestNode("op"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddEdge(NewEdge("src", "op"))
		_ = g.AddEdge(NewEdge("op", "sink"))

		err := g.Validate()
		if err != nil {
			t.Errorf("expected valid graph, got error: %v", err)
		}
	})

	t.Run("valid diamond graph", func(t *testing.T) {
		g := NewGraph("diamond")
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddNode(newTestNode("left"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("right"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddEdge(NewEdge("src", "left"))
		_ = g.AddEdge(NewEdge("src", "right"))
		_ = g.AddEdge(NewEdge("left", "sink"))
		_ = g.AddEdge(NewEdge("right", "sink"))

		err := g.Validate()
		if err != nil {
			t.Errorf("expected valid graph, got error: %v", err)
		}
	})

	t.Run("empty graph rejected", func(t *testing.T) {
		g := NewGraph("empty")
		err := g.Validate()
		if err == nil {
			t.Error("expected error for empty graph, got nil")
		}
	})

	t.Run("dangling node rejected", func(t *testing.T) {
		g := NewGraph("dangling")
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddNode(newTestNode("dangling"), NodeTypeOperator)
		_ = g.AddEdge(NewEdge("src", "sink"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for dangling node, got nil")
		}
	})

	t.Run("cycle rejected", func(t *testing.T) {
		g := NewGraph("cycle")
		_ = g.AddNode(newTestNode("A"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("B"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("C"), NodeTypeOperator)
		_ = g.AddEdge(NewEdge("A", "B"))
		_ = g.AddEdge(NewEdge("B", "C"))
		_ = g.AddEdge(NewEdge("C", "A"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for cycle, got nil")
		}
	})

	t.Run("self-loop rejected", func(t *testing.T) {
		g := NewGraph("self-loop")
		_ = g.AddNode(newTestNode("A"), NodeTypeOperator)
		_ = g.AddEdge(NewEdge("A", "A"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for self-loop, got nil")
		}
	})

	t.Run("source with incoming edge rejected", func(t *testing.T) {
		g := NewGraph("bad-source")
		_ = g.AddNode(newTestNode("A"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddEdge(NewEdge("A", "src"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for source with incoming edge, got nil")
		}
	})

	t.Run("sink with outgoing edge rejected", func(t *testing.T) {
		g := NewGraph("bad-sink")
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddNode(newTestNode("B"), NodeTypeOperator)
		_ = g.AddEdge(NewEdge("sink", "B"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for sink with outgoing edge, got nil")
		}
	})

	t.Run("operator without incoming edge rejected", func(t *testing.T) {
		g := NewGraph("no-input-op")
		_ = g.AddNode(newTestNode("op"), NodeTypeOperator)
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddEdge(NewEdge("op", "sink"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for operator without incoming edge, got nil")
		}
	})

	t.Run("operator without outgoing edge rejected", func(t *testing.T) {
		g := NewGraph("no-output-op")
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddNode(newTestNode("op"), NodeTypeOperator)
		_ = g.AddEdge(NewEdge("src", "op"))

		err := g.Validate()
		if err == nil {
			t.Error("expected error for operator without outgoing edge, got nil")
		}
	})

	t.Run("single source to single sink valid", func(t *testing.T) {
		g := NewGraph("simple")
		_ = g.AddNode(newTestNode("src"), NodeTypeSource)
		_ = g.AddNode(newTestNode("sink"), NodeTypeSink)
		_ = g.AddEdge(NewEdge("src", "sink"))

		err := g.Validate()
		if err != nil {
			t.Errorf("expected valid graph, got error: %v", err)
		}
	})

	t.Run("nodes accessor returns copy", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("A"), NodeTypeSource)
		nodes := g.Nodes()
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes["A"].ID() != "A" {
			t.Errorf("expected node ID 'A', got %q", nodes["A"].ID())
		}
	})

	t.Run("edges accessor returns copy", func(t *testing.T) {
		g := NewGraph("test")
		_ = g.AddNode(newTestNode("A"), NodeTypeSource)
		_ = g.AddNode(newTestNode("B"), NodeTypeSink)
		_ = g.AddEdge(NewEdge("A", "B"))

		edges := g.Edges()
		if len(edges) != 1 {
			t.Fatalf("expected 1 edge, got %d", len(edges))
		}
		// Modifying the copy should not affect the graph.
		edges[0].FromNode = "X"
		original := g.Edges()
		if original[0].FromNode != "A" {
			t.Error("Edges() did not return a copy")
		}
	})
}

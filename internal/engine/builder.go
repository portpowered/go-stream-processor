package engine

import (
	"log"
	"time"
)

// GraphBuilder provides a fluent API for constructing stream processing pipelines.
// It wraps the low-level Graph type with a user-friendly interface that handles
// node creation, edge wiring, buffer allocation, and validation.
type GraphBuilder struct {
	name  string
	graph *Graph

	// Track node adapters by ID for wiring during Build().
	sources   map[string]*SourceNode
	operators map[string]*OperatorNode
	sinks     map[string]*OperatorNode

	// Buffer size overrides per edge (key: "fromId->toId").
	bufferSizes map[string]int

	// Error handling configuration.
	errorConfig *ErrorConfig

	// Checkpoint manager for automatic checkpoint persistence.
	checkpointManager *CheckpointManager

	// First error encountered during building (deferred until Build()).
	err error
}

// NewGraphBuilder creates a new graph builder with the given name.
func NewGraphBuilder(name string) *GraphBuilder {
	return &GraphBuilder{
		name:        name,
		graph:       NewGraph(name),
		sources:     make(map[string]*SourceNode),
		operators:   make(map[string]*OperatorNode),
		sinks:       make(map[string]*OperatorNode),
		bufferSizes: make(map[string]int),
	}
}

// AddSource adds a source node to the graph. Sources produce data via their
// Run method and have no inputs.
func (b *GraphBuilder) AddSource(id string, source Source) *GraphBuilder {
	if b.err != nil {
		return b
	}
	node := NewSourceNode(id, source)
	if err := b.graph.AddNode(node, NodeTypeSource); err != nil {
		b.err = err
		return b
	}
	b.sources[id] = node
	return b
}

// AddOperator adds a processing node to the graph. Operators transform data
// and must have both inputs and outputs.
func (b *GraphBuilder) AddOperator(id string, op Operator) *GraphBuilder {
	if b.err != nil {
		return b
	}
	node := NewOperatorNode(id, op)
	if err := b.graph.AddNode(node, NodeTypeOperator); err != nil {
		b.err = err
		return b
	}
	b.operators[id] = node
	return b
}

// AddSink adds a sink node to the graph. Sinks consume data and have no outputs.
func (b *GraphBuilder) AddSink(id string, sink Operator) *GraphBuilder {
	if b.err != nil {
		return b
	}
	node := NewOperatorNode(id, sink)
	if err := b.graph.AddNode(node, NodeTypeSink); err != nil {
		b.err = err
		return b
	}
	b.sinks[id] = node
	return b
}

// Connect connects two nodes using the default port on both sides.
func (b *GraphBuilder) Connect(fromID, toID string) *GraphBuilder {
	if b.err != nil {
		return b
	}
	if err := b.graph.AddEdge(NewEdge(fromID, toID)); err != nil {
		b.err = err
	}
	return b
}

// ConnectPort connects two nodes using explicit port names.
func (b *GraphBuilder) ConnectPort(fromID, fromPort, toID, toPort string) *GraphBuilder {
	if b.err != nil {
		return b
	}
	if err := b.graph.AddEdge(NewPortEdge(fromID, fromPort, toID, toPort)); err != nil {
		b.err = err
	}
	return b
}

// SetBufferSize configures the buffer capacity for the edge between two nodes.
func (b *GraphBuilder) SetBufferSize(fromID, toID string, size int) *GraphBuilder {
	if b.err != nil {
		return b
	}
	key := fromID + "->" + toID
	b.bufferSizes[key] = size
	return b
}

// SetErrorConfig configures how the pipeline handles errors from operators.
// If not set, the default strategy is SkipAndLog.
func (b *GraphBuilder) SetErrorConfig(cfg ErrorConfig) *GraphBuilder {
	if b.err != nil {
		return b
	}
	b.errorConfig = &cfg
	return b
}

// SetCheckpointManager configures automatic checkpoint persistence. When set,
// Build() wires the Controller's OnCheckpointComplete callback to collect node
// state via the manager and persist the checkpoint to the backend.
func (b *GraphBuilder) SetCheckpointManager(mgr *CheckpointManager) *GraphBuilder {
	if b.err != nil {
		return b
	}
	b.checkpointManager = mgr
	return b
}

// Build validates the graph and returns an executable Pipeline.
// Returns an error if the graph is invalid or if any previous builder
// operation failed.
func (b *GraphBuilder) Build() (*Pipeline, error) {
	if b.err != nil {
		return nil, b.err
	}

	if err := b.graph.Validate(); err != nil {
		return nil, err
	}

	// Create buffers for each edge and wire up node inputs/outputs.
	nodeInputs := make(map[string][]*Buffer)
	nodeOutputs := make(map[string][]*Buffer)
	nodePortOutputs := make(map[string]map[string][]*Buffer)
	var allBuffers []*Buffer

	for _, edge := range b.graph.Edges() {
		size := DefaultBufferCapacity
		key := edge.FromNode + "->" + edge.ToNode
		if s, ok := b.bufferSizes[key]; ok {
			size = s
		}
		buf := NewBuffer(size)
		allBuffers = append(allBuffers, buf)

		nodeOutputs[edge.FromNode] = append(nodeOutputs[edge.FromNode], buf)
		nodeInputs[edge.ToNode] = append(nodeInputs[edge.ToNode], buf)

		// Track port-based outputs for EmitToPort routing.
		if nodePortOutputs[edge.FromNode] == nil {
			nodePortOutputs[edge.FromNode] = make(map[string][]*Buffer)
		}
		nodePortOutputs[edge.FromNode][edge.FromPort] = append(
			nodePortOutputs[edge.FromNode][edge.FromPort], buf)
	}

	// Wire outputs to source nodes.
	for id, src := range b.sources {
		src.SetOutputs(nodeOutputs[id])
		if po, ok := nodePortOutputs[id]; ok {
			src.SetPortOutputs(po)
		}
	}

	// Wire outputs to operator nodes.
	for id, op := range b.operators {
		op.SetOutputs(nodeOutputs[id])
		if po, ok := nodePortOutputs[id]; ok {
			op.SetPortOutputs(po)
		}
	}

	// Wire outputs to sink nodes (sinks have no outputs, but set empty slice).
	for id, sink := range b.sinks {
		sink.SetOutputs(nodeOutputs[id])
		if po, ok := nodePortOutputs[id]; ok {
			sink.SetPortOutputs(po)
		}
	}

	// Collect node IDs by type for the controller.
	var allNodeIDs, sourceIDs, sinkIDs []string
	for id := range b.sources {
		allNodeIDs = append(allNodeIDs, id)
		sourceIDs = append(sourceIDs, id)
	}
	for id := range b.operators {
		allNodeIDs = append(allNodeIDs, id)
	}
	for id := range b.sinks {
		allNodeIDs = append(allNodeIDs, id)
		sinkIDs = append(sinkIDs, id)
	}

	// Wire OnCheckpointComplete if a CheckpointManager is configured.
	var onCheckpointComplete func(epoch uint64, info *CheckpointInfo)
	if b.checkpointManager != nil {
		mgr := b.checkpointManager
		onCheckpointComplete = func(epoch uint64, info *CheckpointInfo) {
			for nodeID, state := range info.NodeStates {
				mgr.CollectState(NodeCompletion{
					NodeID: nodeID,
					Epoch:  epoch,
					State:  state,
				})
			}
			if err := mgr.SaveCheckpoint(epoch); err != nil {
				log.Printf("[esp] checkpoint save failed for epoch %d: %v", epoch, err)
			}
		}
	}

	// Create controller.
	controller := NewController(ControllerConfig{
		NodeIDs:              allNodeIDs,
		SourceIDs:            sourceIDs,
		SinkIDs:              sinkIDs,
		OnCheckpointComplete: onCheckpointComplete,
	})

	completionCh := controller.CompletionChan()
	stoppedCh := controller.StoppedChan()

	// Set up error handling.
	errCfg := ErrorConfig{Strategy: ErrorStrategySkipAndLog}
	if b.errorConfig != nil {
		errCfg = *b.errorConfig
	}
	errCounts := newErrorCounts()

	// Create dead-letter sink infrastructure if configured.
	var dlBuffer *Buffer
	var dlRuntime *NodeRuntime
	var dlNode *OperatorNode
	if errCfg.Strategy == ErrorStrategyDeadLetter && errCfg.DeadLetterSink != nil {
		dlBuffer = NewBuffer(DefaultBufferCapacity)
		allBuffers = append(allBuffers, dlBuffer)
		dlNode = NewOperatorNode("__dead_letter__", errCfg.DeadLetterSink)
		dlNode.SetOutputs(nil)
		dlRuntime = NewNodeRuntime(NodeRuntimeConfig{
			Node:     dlNode,
			NodeType: NodeTypeSink,
			Inputs:   []*Buffer{dlBuffer},
			Outputs:  nil,
		})
	}

	onError := makeErrorHandler(errCfg, errCounts, dlBuffer)

	// Wire error handler to all node adapters (for Collector.EmitError).
	for _, src := range b.sources {
		src.SetOnError(onError)
	}
	for _, op := range b.operators {
		op.SetOnError(onError)
	}
	for _, sink := range b.sinks {
		sink.SetOnError(onError)
	}

	// Create NodeRuntimes for all nodes.
	var runtimes []*NodeRuntime
	var sourceRuntimes []*NodeRuntime
	var sourceNodes []*SourceNode
	sourceOutputMap := make(map[string][]*Buffer)

	for id, src := range b.sources {
		rt := NewNodeRuntime(NodeRuntimeConfig{
			Node:         src,
			NodeType:     NodeTypeSource,
			Inputs:       nil, // sources have no inputs
			Outputs:      nodeOutputs[id],
			CompletionCh: completionCh,
			StoppedCh:    stoppedCh,
			OnError:      onError,
		})
		runtimes = append(runtimes, rt)
		sourceRuntimes = append(sourceRuntimes, rt)
		sourceNodes = append(sourceNodes, src)
		sourceOutputMap[id] = nodeOutputs[id]
	}

	for id, op := range b.operators {
		var tickInterval time.Duration
		if tickOp, ok := op.Operator().(TickOperator); ok {
			tickInterval = tickOp.TickInterval()
		}
		rt := NewNodeRuntime(NodeRuntimeConfig{
			Node:         op,
			NodeType:     NodeTypeOperator,
			Inputs:       nodeInputs[id],
			Outputs:      nodeOutputs[id],
			TickInterval: tickInterval,
			CompletionCh: completionCh,
			StoppedCh:    stoppedCh,
			OnError:      onError,
		})
		runtimes = append(runtimes, rt)
	}

	for id, sink := range b.sinks {
		rt := NewNodeRuntime(NodeRuntimeConfig{
			Node:         sink,
			NodeType:     NodeTypeSink,
			Inputs:       nodeInputs[id],
			Outputs:      nil, // sinks have no outputs
			CompletionCh: completionCh,
			StoppedCh:    stoppedCh,
			OnError:      onError,
		})
		runtimes = append(runtimes, rt)
	}

	return &Pipeline{
		name:           b.name,
		runtimes:       runtimes,
		sourceRuntimes: sourceRuntimes,
		sourceNodes:    sourceNodes,
		sourceOutputs:  sourceOutputMap,
		controller:     controller,
		buffers:        allBuffers,
		errCounts:      errCounts,
		dlBuffer:       dlBuffer,
		dlRuntime:      dlRuntime,
	}, nil
}

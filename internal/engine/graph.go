package engine

import "fmt"

// DefaultPort is the port name used when no explicit port is specified.
const DefaultPort = "default"

// Edge represents a directed connection between two nodes in the graph.
// Each edge connects a specific output port of the source node to a specific
// input port of the destination node.
type Edge struct {
	// FromNode is the ID of the source node.
	FromNode string
	// FromPort is the output port name on the source node.
	FromPort string
	// ToNode is the ID of the destination node.
	ToNode string
	// ToPort is the input port name on the destination node.
	ToPort string
}

// NewEdge creates an edge using default ports on both sides.
func NewEdge(fromNode, toNode string) Edge {
	return Edge{
		FromNode: fromNode,
		FromPort: DefaultPort,
		ToNode:   toNode,
		ToPort:   DefaultPort,
	}
}

// NewPortEdge creates an edge with explicit port names.
func NewPortEdge(fromNode, fromPort, toNode, toPort string) Edge {
	return Edge{
		FromNode: fromNode,
		FromPort: fromPort,
		ToNode:   toNode,
		ToPort:   toPort,
	}
}

// nodeEntry holds a node and its metadata within the graph.
type nodeEntry struct {
	Node     Node
	NodeType NodeType
}

// Graph is a container for nodes and edges that form a directed acyclic graph
// (DAG). The graph validates its topology before execution to ensure there are
// no cycles, all ports are connected, and no nodes are dangling.
type Graph struct {
	// Name is a human-readable identifier for this graph.
	Name string
	// nodes maps node IDs to their entries.
	nodes map[string]nodeEntry
	// edges holds all connections between nodes.
	edges []Edge
}

// NewGraph creates a new empty graph with the given name.
func NewGraph(name string) *Graph {
	return &Graph{
		Name:  name,
		nodes: make(map[string]nodeEntry),
	}
}

// AddNode registers a node in the graph with the given type classification.
// Returns an error if a node with the same ID already exists.
func (g *Graph) AddNode(node Node, nodeType NodeType) error {
	id := node.ID()
	if _, exists := g.nodes[id]; exists {
		return fmt.Errorf("node %q already exists in graph", id)
	}
	g.nodes[id] = nodeEntry{Node: node, NodeType: nodeType}
	return nil
}

// AddEdge adds a directed connection between two nodes.
// Returns an error if either node does not exist in the graph.
func (g *Graph) AddEdge(edge Edge) error {
	if _, exists := g.nodes[edge.FromNode]; !exists {
		return fmt.Errorf("source node %q not found in graph", edge.FromNode)
	}
	if _, exists := g.nodes[edge.ToNode]; !exists {
		return fmt.Errorf("destination node %q not found in graph", edge.ToNode)
	}
	g.edges = append(g.edges, edge)
	return nil
}

// Nodes returns a copy of the node map.
func (g *Graph) Nodes() map[string]Node {
	result := make(map[string]Node, len(g.nodes))
	for id, entry := range g.nodes {
		result[id] = entry.Node
	}
	return result
}

// Edges returns a copy of the edge slice.
func (g *Graph) Edges() []Edge {
	result := make([]Edge, len(g.edges))
	copy(result, g.edges)
	return result
}

// NodeCount returns the number of nodes in the graph.
func (g *Graph) NodeCount() int {
	return len(g.nodes)
}

// Validate checks the graph for structural correctness. It returns an error
// if any of the following are true:
//   - The graph contains a cycle (only DAGs are allowed)
//   - A node has no edges (dangling node)
//   - A source node has incoming edges
//   - A sink node has outgoing edges
func (g *Graph) Validate() error {
	if len(g.nodes) == 0 {
		return fmt.Errorf("graph %q has no nodes", g.Name)
	}

	// Check for dangling nodes (nodes with no edges at all).
	connectedNodes := make(map[string]bool)
	for _, edge := range g.edges {
		connectedNodes[edge.FromNode] = true
		connectedNodes[edge.ToNode] = true
	}
	for id := range g.nodes {
		if !connectedNodes[id] && len(g.nodes) > 1 {
			return fmt.Errorf("node %q is dangling (no edges)", id)
		}
	}

	// Check source/sink port constraints.
	incomingEdges := make(map[string]int)
	outgoingEdges := make(map[string]int)
	for _, edge := range g.edges {
		outgoingEdges[edge.FromNode]++
		incomingEdges[edge.ToNode]++
	}

	for id, entry := range g.nodes {
		switch entry.NodeType {
		case NodeTypeSource:
			if incomingEdges[id] > 0 {
				return fmt.Errorf("source node %q must not have incoming edges", id)
			}
		case NodeTypeSink:
			if outgoingEdges[id] > 0 {
				return fmt.Errorf("sink node %q must not have outgoing edges", id)
			}
		case NodeTypeOperator:
			if incomingEdges[id] == 0 {
				return fmt.Errorf("operator node %q has no incoming edges", id)
			}
			if outgoingEdges[id] == 0 {
				return fmt.Errorf("operator node %q has no outgoing edges", id)
			}
		}
	}

	// Check for cycles using DFS with coloring.
	if err := g.detectCycles(); err != nil {
		return err
	}

	return nil
}

// detectCycles uses DFS with three-color marking to detect cycles.
// White (0) = unvisited, Gray (1) = in current path, Black (2) = fully explored.
func (g *Graph) detectCycles() error {
	// Build adjacency list.
	adj := make(map[string][]string)
	for _, edge := range g.edges {
		adj[edge.FromNode] = append(adj[edge.FromNode], edge.ToNode)
	}

	color := make(map[string]int) // 0=white, 1=gray, 2=black
	for id := range g.nodes {
		color[id] = 0
	}

	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = 1 // gray
		for _, neighbor := range adj[node] {
			switch color[neighbor] {
			case 1: // gray - back edge found, cycle detected
				return fmt.Errorf("graph contains a cycle involving node %q", neighbor)
			case 0: // white - unvisited
				if err := dfs(neighbor); err != nil {
					return err
				}
			}
			// black (2) = already fully explored, skip
		}
		color[node] = 2 // black
		return nil
	}

	for id := range g.nodes {
		if color[id] == 0 {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}

	return nil
}

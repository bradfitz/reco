package recontrol

import (
	"fmt"
	"reflect"
	"sync"
)

// Graph is one customer/tailnet dataflow graph instance.
type Graph struct {
	mu      sync.Mutex
	nodes   map[*nodeDef]nodeValue
	defs    map[string]*nodeDef
	subs    map[*nodeDef]map[uint64]subscriber
	nextSub uint64
	version Version
}

// NewGraph creates an empty graph instance.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[*nodeDef]nodeValue),
		defs:  make(map[string]*nodeDef),
		subs:  make(map[*nodeDef]map[uint64]subscriber),
	}
}

// Register adds node definitions to the graph and validates key uniqueness and
// DAG structure.
func (g *Graph) Register(nodes ...any) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, n := range nodes {
		tn, err := nodeFromAny(n)
		if err != nil {
			return err
		}
		if existing := g.defs[tn.def.key]; existing != nil && existing != tn.def {
			return fmt.Errorf("recontrol: duplicate node key %q", tn.def.key)
		}
		g.defs[tn.def.key] = tn.def
		if _, ok := g.nodes[tn.def]; !ok {
			g.nodes[tn.def] = nodeValue{}
		}
	}
	for _, n := range nodes {
		tn, _ := nodeFromAny(n)
		if err := g.registerDeps(tn.def, map[*nodeDef]bool{}); err != nil {
			return err
		}
	}
	return g.checkDAG()
}

func (g *Graph) registerDeps(def *nodeDef, seen map[*nodeDef]bool) error {
	if seen[def] {
		return nil
	}
	seen[def] = true
	for _, dep := range def.deps {
		if existing := g.defs[dep.node.key]; existing != nil && existing != dep.node {
			return fmt.Errorf("recontrol: duplicate node key %q", dep.node.key)
		}
		g.defs[dep.node.key] = dep.node
		if _, ok := g.nodes[dep.node]; !ok {
			g.nodes[dep.node] = nodeValue{}
		}
		if err := g.registerDeps(dep.node, seen); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) checkDAG() error {
	const (
		unseen = 0
		active = 1
		done   = 2
	)
	seen := map[*nodeDef]int{}
	var visit func(*nodeDef) error
	visit = func(n *nodeDef) error {
		switch seen[n] {
		case active:
			return fmt.Errorf("recontrol: cycle involving %s", n.key)
		case done:
			return nil
		}
		seen[n] = active
		for _, dep := range n.deps {
			if err := visit(dep.node); err != nil {
				return err
			}
		}
		seen[n] = done
		return nil
	}
	for _, n := range g.defs {
		if seen[n] == unseen {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

// Update runs one serialized transaction and then recomputes function nodes to
// a fixed point before delivering subscription events.
func (g *Graph) Update(fn func(*Tx) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	before := g.copyValues()
	tx := &Tx{g: g, writes: make(map[*nodeDef]any)}
	if err := fn(tx); err != nil {
		return err
	}
	for def, v := range tx.writes {
		g.version++
		g.nodes[def] = nodeValue{value: v, version: g.version, valid: true}
	}
	if err := g.recompute(); err != nil {
		return err
	}
	g.deliverEvents(before)
	return nil
}

func (g *Graph) recompute() error {
	for {
		changed := false
		for _, def := range g.defs {
			if def.kind != nodeFunc {
				continue
			}
			depVals := make(map[*nodeDef]nodeValue, len(def.deps))
			ready := true
			for _, dep := range def.deps {
				v := g.nodes[dep.node]
				if !v.valid {
					ready = false
					break
				}
				depVals[dep.node] = v
			}
			if !ready {
				continue
			}
			next := def.compute(Context{graph: g}, depVals)
			prev := g.nodes[def]
			if !nodeValuesEqual(prev, next) {
				g.version++
				next.version = g.version
				next.valid = true
				g.nodes[def] = next
				changed = true
			}
		}
		if !changed {
			return nil
		}
	}
}

func nodeValuesEqual(a, b nodeValue) bool {
	if a.valid != b.valid || a.isPartial != b.isPartial {
		return false
	}
	if !reflect.DeepEqual(a.err, b.err) {
		return false
	}
	return reflect.DeepEqual(a.value, b.value)
}

func (g *Graph) copyValues() map[*nodeDef]nodeValue {
	out := make(map[*nodeDef]nodeValue, len(g.nodes))
	for k, v := range g.nodes {
		out[k] = v
	}
	return out
}

func nodeFromAny(n any) (typedNode, error) {
	h, ok := n.(nodeHandle)
	if !ok {
		return typedNode{}, fmt.Errorf("recontrol: expected Node[T], got %T", n)
	}
	return h.recontrolTypedNode(), nil
}

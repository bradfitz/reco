package reco

import (
	"container/heap"
	"fmt"
	"reflect"
	"slices"
	"sync"
)

// Graph is an independent reactive dataflow graph instance.
type Graph struct {
	mu      sync.Mutex
	nodes   map[*nodeDef]nodeValue
	defs    map[NodeClassName]*nodeDef
	subs    map[*nodeDef]map[uint64]subscriber
	nextSub uint64
	version Version
	order   []*nodeDef // dependencies before consumers
	funcs   map[*nodeDef]*funcState
	rank    map[*nodeDef]int
	users   map[*nodeDef][]*nodeDef
	initial []*nodeDef // newly registered functions, evaluated on the next update
}

type funcState struct {
	compute computeFunc
	seen    Version
	ran     bool
}

// NewGraph creates an empty graph instance.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[*nodeDef]nodeValue),
		defs:  make(map[NodeClassName]*nodeDef),
		subs:  make(map[*nodeDef]map[uint64]subscriber),
		funcs: make(map[*nodeDef]*funcState),
	}
}

// Register adds node definitions to the graph and validates class name uniqueness and
// DAG structure.
func (g *Graph) Register(nodes ...any) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, n := range nodes {
		tn, err := nodeFromAny(n)
		if err != nil {
			return err
		}
		if existing := g.defs[tn.def.className]; existing != nil && existing != tn.def {
			return fmt.Errorf("reco: duplicate node class name %q", tn.def.className)
		}
		g.defs[tn.def.className] = tn.def
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
		if existing := g.defs[dep.node.className]; existing != nil && existing != dep.node {
			return fmt.Errorf("reco: duplicate node class name %q", dep.node.className)
		}
		g.defs[dep.node.className] = dep.node
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
	var order []*nodeDef
	var visit func(*nodeDef) error
	visit = func(n *nodeDef) error {
		switch seen[n] {
		case active:
			return fmt.Errorf("reco: cycle involving %s", n.className)
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
		order = append(order, n)
		return nil
	}
	classNames := make([]NodeClassName, 0, len(g.defs))
	for className := range g.defs {
		classNames = append(classNames, className)
	}
	slices.Sort(classNames)
	for _, className := range classNames {
		n := g.defs[className]
		if seen[n] == unseen {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	g.order = order
	g.rank = make(map[*nodeDef]int, len(order))
	g.users = make(map[*nodeDef][]*nodeDef)
	g.initial = nil
	for i, def := range order {
		g.rank[def] = i
		if def.kind != nodeFunc {
			continue
		}
		if state := g.funcs[def]; state == nil || !state.ran {
			g.initial = append(g.initial, def)
		}
		seenDeps := make(map[*nodeDef]bool)
		for _, dep := range def.deps {
			if !seenDeps[dep.node] {
				g.users[dep.node] = append(g.users[dep.node], def)
				seenDeps[dep.node] = true
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

	tx := &Tx{g: g, writes: make(map[*nodeDef]any)}
	if err := fn(tx); err != nil {
		return err
	}
	// Keep only changed nodes' old values. Neither committing a point update
	// nor notifying its subscribers should scan/copy the whole node registry.
	before := make(map[*nodeDef]nodeValue)
	dirty := &dirtyQueue{rank: g.rank, scheduled: make(map[*nodeDef]bool)}
	for _, def := range g.initial {
		dirty.add(def)
	}
	g.initial = nil
	for def, v := range tx.writes {
		prev := g.nodes[def]
		if normalizer, ok := v.(interface{ recoNormalize(any) any }); ok {
			v = normalizer.recoNormalize(prev.value)
		}
		next := nodeValue{value: v, valid: true}
		if nodeValuesEqual(prev, next) {
			continue
		}
		before[def] = prev
		g.version++
		v = valueWithVersion(v, g.version)
		g.nodes[def] = nodeValue{value: v, version: g.version, valid: true}
		for _, user := range g.users[def] {
			dirty.add(user)
		}
	}
	g.recompute(before, dirty)
	g.deliverEvents(before)
	return nil
}

func (g *Graph) recompute(before map[*nodeDef]nodeValue, dirty *dirtyQueue) {
	// Evaluate only affected nodes, in dependency order and at most once per
	// transaction. Diamond joins see all changed branches together.
	for dirty.Len() != 0 {
		def := heap.Pop(dirty).(*nodeDef)
		state := g.funcs[def]
		if state == nil {
			state = &funcState{compute: def.compute}
			if def.newCompute != nil {
				state.compute = def.newCompute()
			}
			g.funcs[def] = state
		}
		ready, needsCompute := true, !state.ran
		for _, dep := range def.deps {
			v := g.nodes[dep.node]
			ready = ready && v.valid
			needsCompute = needsCompute || v.version > state.seen
		}
		if !ready || !needsCompute {
			continue
		}
		depVals := make(map[*nodeDef]nodeValue, len(def.deps))
		for _, dep := range def.deps {
			depVals[dep.node] = g.nodes[dep.node]
		}
		next := state.compute(Eval{graph: g, inputs: depVals}, depVals)
		state.seen, state.ran = g.version, true
		if !nodeValuesEqual(g.nodes[def], next) {
			before[def] = g.nodes[def]
			g.version++
			next.value = valueWithVersion(next.value, g.version)
			next.version = g.version
			next.valid = true
			g.nodes[def] = next
			for _, user := range g.users[def] {
				dirty.add(user)
			}
		}
	}
}

// dirtyQueue's ranks are fixed when the graph is registered, not on each
// mutation. scheduled deduplicates fan-in and diamond branches.
type dirtyQueue struct {
	nodes     []*nodeDef
	rank      map[*nodeDef]int
	scheduled map[*nodeDef]bool
}

func (q *dirtyQueue) add(def *nodeDef) {
	if !q.scheduled[def] {
		q.scheduled[def] = true
		heap.Push(q, def)
	}
}

func (q *dirtyQueue) Len() int           { return len(q.nodes) }
func (q *dirtyQueue) Less(i, j int) bool { return q.rank[q.nodes[i]] < q.rank[q.nodes[j]] }
func (q *dirtyQueue) Swap(i, j int)      { q.nodes[i], q.nodes[j] = q.nodes[j], q.nodes[i] }
func (q *dirtyQueue) Push(v any)         { q.nodes = append(q.nodes, v.(*nodeDef)) }
func (q *dirtyQueue) Pop() any {
	i := len(q.nodes) - 1
	def := q.nodes[i]
	q.nodes[i] = nil
	q.nodes = q.nodes[:i]
	return def
}

func valueWithVersion(v any, version Version) any {
	if versioned, ok := v.(VersionedValue); ok {
		next := versioned.RecoWithVersion(version)
		if reflect.TypeOf(next) != reflect.TypeOf(v) {
			panic(fmt.Sprintf("reco: RecoWithVersion changed value type from %T to %T", v, next))
		}
		return next
	}
	return v
}

func nodeValuesEqual(a, b nodeValue) bool {
	if a.valid != b.valid || a.isPartial != b.isPartial {
		return false
	}
	if !reflect.DeepEqual(a.err, b.err) {
		return false
	}
	return valuesEqual(a.value, b.value)
}

func valuesEqual(a, b any) bool {
	if equaler, ok := a.(ValueEqualer); ok {
		return equaler.RecoValueEqual(b)
	}
	return reflect.DeepEqual(a, b)
}

func nodeFromAny(n any) (typedNode, error) {
	h, ok := n.(nodeHandle)
	if !ok {
		return typedNode{}, fmt.Errorf("reco: expected Node[T], got %T", n)
	}
	return h.recoTypedNode(), nil
}

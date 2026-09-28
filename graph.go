package reco

import (
	"container/heap"
	"fmt"
	"reflect"
	"sync"
)

// Graph is an independent reactive dataflow graph instance.
type Graph struct {
	mu                      sync.Mutex
	nodes                   map[*nodeDef]nodeValue
	defs                    map[nodeName]*nodeDef
	subs                    map[*nodeDef]map[uint64]subscriber
	nextSub                 uint64
	version                 Version
	funcs                   map[*nodeDef]*funcState
	rank                    map[*nodeDef]int
	users                   map[*nodeDef]map[*nodeDef]bool // active consumers only in demand mode
	initial                 []*nodeDef                     // newly registered functions, evaluated on the next update
	demand                  bool
	refs                    map[*nodeDef]int // subscriptions and active consumers of functions
	functionCount, subCount int
	evaluations             uint64
}

type nodeName struct {
	scope     *Scope
	className NodeClassName
}

// GraphOptions configures evaluation lifetime.
type GraphOptions struct {
	// DemandDriven evaluates only functions reachable from subscriptions.
	// Read evaluates on demand without retaining a watch. When the last
	// observer goes away, derived values and operator caches are released.
	// Registered definitions and authoritative data values remain available.
	DemandDriven bool
}

type funcState struct {
	compute computeFunc
	seen    Version
	ran     bool
}

// NewGraph creates an empty, eager graph instance. See NewGraphWithOptions for
// demand-driven evaluation and automatic cache release.
func NewGraph() *Graph {
	return NewGraphWithOptions(GraphOptions{})
}

// NewGraphWithOptions creates a graph with the specified evaluation lifetime.
func NewGraphWithOptions(opts GraphOptions) *Graph {
	return &Graph{
		nodes:  make(map[*nodeDef]nodeValue),
		defs:   make(map[nodeName]*nodeDef),
		subs:   make(map[*nodeDef]map[uint64]subscriber),
		funcs:  make(map[*nodeDef]*funcState),
		rank:   make(map[*nodeDef]int),
		users:  make(map[*nodeDef]map[*nodeDef]bool),
		refs:   make(map[*nodeDef]int),
		demand: opts.DemandDriven,
	}
}

// Register atomically adds definitions and dependencies, validating class name
// uniqueness within each Scope and DAG structure. Work is proportional to newly
// registered definitions and their edges, not the existing graph's size.
func (g *Graph) Register(nodes ...any) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	staged := make(map[nodeName]*nodeDef)
	seen := make(map[*nodeDef]int)
	var order []*nodeDef
	var visit func(*nodeDef) error
	visit = func(n *nodeDef) error {
		if _, ok := g.nodes[n]; ok {
			return nil
		}
		switch seen[n] {
		case 1:
			return fmt.Errorf("reco: cycle involving %s", n.className)
		case 2:
			return nil
		}
		name := nodeName{n.scope, n.className}
		if old := g.defs[name]; old != nil && old != n {
			return fmt.Errorf("reco: duplicate node class name %q", n.className)
		}
		if old := staged[name]; old != nil && old != n {
			return fmt.Errorf("reco: duplicate node class name %q", n.className)
		}
		staged[name], seen[n] = n, 1
		for _, dep := range n.deps {
			if err := visit(dep.node); err != nil {
				return err
			}
		}
		seen[n] = 2
		order = append(order, n)
		return nil
	}
	for _, n := range nodes {
		tn, err := nodeFromAny(n)
		if err != nil {
			return err
		}
		if err := visit(tn.def); err != nil {
			return err
		}
	}
	for _, def := range order {
		g.defs[nodeName{def.scope, def.className}] = def
		g.nodes[def] = nodeValue{}
		for _, dep := range def.deps {
			g.rank[def] = max(g.rank[def], g.rank[dep.node]+1)
		}
		if def.kind != nodeFunc {
			continue
		}
		g.functionCount++
		if g.demand {
			continue
		}
		g.initial = append(g.initial, def)
		for _, dep := range def.deps {
			g.addUser(dep.node, def)
		}
	}
	return nil
}

func (g *Graph) addUser(dep, user *nodeDef) {
	if g.users[dep] == nil {
		g.users[dep] = make(map[*nodeDef]bool)
	}
	g.users[dep][user] = true
}

// Update runs one serialized transaction and then recomputes affected active
// functions to a fixed point before delivering subscription events. In eager
// graphs all registered functions are active; demand-driven graphs activate
// only subscribed dependency closures.
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
		for user := range g.users[def] {
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
		g.evaluations++
		state.seen, state.ran = g.version, true
		if !nodeValuesEqual(g.nodes[def], next) {
			before[def] = g.nodes[def]
			g.version++
			next.value = valueWithVersion(next.value, g.version)
			next.version = g.version
			next.valid = true
			g.nodes[def] = next
			for user := range g.users[def] {
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

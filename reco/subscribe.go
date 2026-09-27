package reco

import (
	"fmt"
	"slices"
)

type subscriber interface {
	deliver(prev nodeValue, cur nodeValue)
}

type typedSubscriber[T any] struct {
	fn func(Event[T])
}

// MapEvent is delivered after a transaction changes a map node. Changes lists
// only changed keys. Consecutive snapshots use their recorded delta; arbitrary
// snapshot replacements are reconciled against the previous value.
type MapEvent[K comparable, V any] struct {
	Previous MapSnapshot[K, V]
	Current  MapSnapshot[K, V]
	Changes  []MapChange[K, V]
	Version  Version
}

func (s typedSubscriber[T]) deliver(prev nodeValue, cur nodeValue) {
	ev := Event[T]{
		Previous: snapshotFromNodeValue[T](prev),
		Current:  snapshotFromNodeValue[T](cur),
		Version:  cur.version,
	}
	s.fn(ev)
}

func snapshotFromNodeValue[T any](v nodeValue) Snapshot[T] {
	if !v.valid {
		return Snapshot[T]{}
	}
	return newSnapshot(typedValue[T](v.value), v.version)
}

// A valid interface-valued node may contain a nil interface. A direct type
// assertion would panic, though nil is the correct zero value for its type.
func typedValue[T any](v any) T {
	if v == nil {
		var zero T
		return zero
	}
	return v.(T)
}

type subscription struct {
	g    *Graph
	def  *nodeDef
	id   uint64
	once bool
}

func (s *subscription) Unsubscribe() error {
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	if s.once {
		return nil
	}
	s.once = true
	delete(s.g.subs[s.def], s.id)
	return nil
}

// Subscribe registers a callback for node value changes.
func Subscribe[T any](g *Graph, node Node[T], opts SubscribeOptions, fn func(Event[T])) (SubscriptionHandle, error) {
	if fn == nil {
		return nil, fmt.Errorf("reco: nil subscriber")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if node.def == nil {
		return nil, fmt.Errorf("reco: zero node handle")
	}
	if _, ok := g.nodes[node.def]; !ok {
		return nil, fmt.Errorf("reco: node %s is not registered", node.def.className)
	}
	g.nextSub++
	id := g.nextSub
	if g.subs[node.def] == nil {
		g.subs[node.def] = make(map[uint64]subscriber)
	}
	g.subs[node.def][id] = typedSubscriber[T]{fn: fn}
	return &subscription{g: g, def: node.def, id: id}, nil
}

// SubscribeMap atomically returns the current snapshot and subscribes fn to
// later transaction-local map mutations. The atomic setup prevents updates
// from being lost between taking the initial snapshot and starting the watch.
func SubscribeMap[K comparable, V any](g *Graph, node Node[MapSnapshot[K, V]], opts SubscribeOptions, fn func(MapEvent[K, V])) (Snapshot[MapSnapshot[K, V]], SubscriptionHandle, error) {
	if fn == nil {
		return Snapshot[MapSnapshot[K, V]]{}, nil, fmt.Errorf("reco: nil subscriber")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if node.def == nil {
		return Snapshot[MapSnapshot[K, V]]{}, nil, fmt.Errorf("reco: zero node handle")
	}
	cur, ok := g.nodes[node.def]
	if !ok {
		return Snapshot[MapSnapshot[K, V]]{}, nil, fmt.Errorf("reco: node %s is not registered", node.def.className)
	}
	g.nextSub++
	id := g.nextSub
	if g.subs[node.def] == nil {
		g.subs[node.def] = make(map[uint64]subscriber)
	}
	g.subs[node.def][id] = typedSubscriber[MapSnapshot[K, V]]{fn: func(ev Event[MapSnapshot[K, V]]) {
		current := ev.Current.Value()
		fn(MapEvent[K, V]{
			Previous: ev.Previous.Value(),
			Current:  current,
			Changes:  slices.Collect(current.ChangesSince(ev.Previous.Value())),
			Version:  ev.Version,
		})
	}}
	return snapshotFromNodeValue[MapSnapshot[K, V]](cur), &subscription{g: g, def: node.def, id: id}, nil
}

func (g *Graph) deliverEvents(before map[*nodeDef]nodeValue) {
	for def, prev := range before {
		cur := g.nodes[def]
		if nodeValuesEqual(prev, cur) {
			continue
		}
		for _, sub := range g.subs[def] {
			sub.deliver(prev, cur)
		}
	}
}

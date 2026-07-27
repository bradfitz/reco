package recontrol

import "fmt"

type subscriber interface {
	deliver(prev nodeValue, cur nodeValue)
}

type typedSubscriber[T any] struct {
	fn func(Event[T])
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
	return newSnapshot(v.value.(T), v.version)
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
		return nil, fmt.Errorf("recontrol: nil subscriber")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if node.def == nil {
		return nil, fmt.Errorf("recontrol: zero node handle")
	}
	if _, ok := g.nodes[node.def]; !ok {
		return nil, fmt.Errorf("recontrol: node %s is not registered", node.def.key)
	}
	g.nextSub++
	id := g.nextSub
	if g.subs[node.def] == nil {
		g.subs[node.def] = make(map[uint64]subscriber)
	}
	g.subs[node.def][id] = typedSubscriber[T]{fn: fn}
	return &subscription{g: g, def: node.def, id: id}, nil
}

func (g *Graph) deliverEvents(before map[*nodeDef]nodeValue) {
	for def, subs := range g.subs {
		cur := g.nodes[def]
		prev := before[def]
		if nodeValuesEqual(prev, cur) {
			continue
		}
		for _, sub := range subs {
			sub.deliver(prev, cur)
		}
	}
}

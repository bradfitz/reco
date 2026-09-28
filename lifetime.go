package reco

// GraphStats reports graph lifetime and work counters. Registered definitions
// and data leaves outlive subscriptions; CachedFunctions counts retained
// operator states, which are released with last demand in demand-driven mode.
type GraphStats struct {
	Nodes, Functions, ActiveFunctions, CachedFunctions, Subscriptions int
	Evaluations                                                       uint64
}

// Stats returns a constant-time snapshot of graph counters.
func (g *Graph) Stats() GraphStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	active := g.functionCount
	if g.demand {
		active = len(g.refs)
	}
	return GraphStats{Nodes: len(g.nodes), Functions: g.functionCount,
		ActiveFunctions: active, CachedFunctions: len(g.funcs),
		Subscriptions: g.subCount, Evaluations: g.evaluations}
}

// observeLocked acquires one demand reference and settles newly active work.
// Subscription callers install the callback afterwards: this is initial state,
// not a change event. Read releases its temporary demand before returning.
func (g *Graph) observeLocked(def *nodeDef) {
	if !g.demand {
		return
	}
	dirty := &dirtyQueue{rank: g.rank, scheduled: make(map[*nodeDef]bool)}
	g.retainLocked(def, dirty)
	g.recompute(make(map[*nodeDef]nodeValue), dirty)
}

func (g *Graph) retainLocked(def *nodeDef, dirty *dirtyQueue) {
	if def.kind != nodeFunc {
		return
	}
	g.refs[def]++
	if g.refs[def] != 1 {
		return
	}
	for _, dep := range def.deps {
		if !g.users[dep.node][def] {
			g.addUser(dep.node, def)
			g.retainLocked(dep.node, dirty)
		}
	}
	dirty.add(def)
}

func (g *Graph) releaseLocked(def *nodeDef) {
	if !g.demand || def.kind != nodeFunc {
		return
	}
	g.refs[def]--
	if g.refs[def] != 0 {
		return
	}
	delete(g.refs, def)
	delete(g.funcs, def)       // release incremental caches, including old input roots
	g.nodes[def] = nodeValue{} // release the derived output
	for _, dep := range def.deps {
		if g.users[dep.node][def] {
			delete(g.users[dep.node], def)
			if len(g.users[dep.node]) == 0 {
				delete(g.users, dep.node)
			}
			g.releaseLocked(dep.node)
		}
	}
}

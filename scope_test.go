package reco

import (
	"sync"
	"testing"
)

func TestScopesAndCrossScopeDemand(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	leaf := Data[int]("leaf")
	factories := 0
	doubled := Operator("doubled", []Dependency{leaf}, func() Compute[int] {
		factories++
		return func(e Eval) Result[int] { return OK(Input(e, leaf).Value() * 2) }
	})
	var a, b Scope
	x, y := In(&a, leaf), In(&b, leaf)
	xx, yy := In(&a, doubled), In(&b, doubled)
	if x != In(&a, leaf) || x == y || xx.ClassName() != doubled.ClassName() {
		t.Fatal("invalid instance identity")
	}
	// Preserve an explicitly scoped foreign dependency while instantiating the
	// local branch. Only this combined output will be subscribed.
	joined := In(&b, Func("joined", Deps(struct{ Remote, Local Node[int] }{xx, doubled}), func(_ Eval, in struct{ Remote, Local int }) Result[int] { return OK(in.Remote + in.Local) }))
	if err := g.Register(joined); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error { Set(tx, x, 2); Set(tx, y, 3); return nil }); err != nil {
		t.Fatal(err)
	}
	if factories != 0 {
		t.Fatal("idle instances initialized caches")
	}
	var events []int
	sub, err := Subscribe(g, joined, SubscribeOptions{}, func(e Event[int]) { events = append(events, e.Current.Value()) })
	if err != nil {
		t.Fatal(err)
	}
	if factories != 2 || g.Stats().ActiveFunctions != 3 {
		t.Fatal("cross-scope dependency not active")
	}
	if got, _ := Read(g, joined); got.Value() != 10 {
		t.Fatal("scoped Func/Input bindings wrong")
	}
	if got, _ := Read(g, yy); got.Value() != 6 {
		t.Fatal("instances share values")
	}
	g.Update(func(tx *Tx) error { Set(tx, x, 4); Set(tx, y, 5); return nil })
	if len(events) != 1 || events[0] != 18 {
		t.Fatalf("cross-scope transaction not atomic: %v", events)
	}
	sub.Unsubscribe()
	if got := g.Stats(); got.ActiveFunctions != 0 || got.CachedFunctions != 0 {
		t.Fatal("foreign dependency kept alive")
	}
	g.Update(func(tx *Tx) error { Set(tx, x, 6); return nil })
	if factories != 2 {
		t.Fatal("foreign source recomputed without a watcher")
	}
	if got, _ := Read(g, joined); got.Value() != 22 || factories != 4 {
		t.Fatal("cross-scope read failed to reactivate")
	}
}

func TestScopeConcurrentResolutionAndNameValidation(t *testing.T) {
	var scope Scope
	leaf := Data[int]("leaf")
	n := Func("out", Deps(struct{ V Node[int] }{leaf}), func(_ Eval, in struct{ V int }) Result[int] { return OK(in.V) })
	var wg sync.WaitGroup
	results := make(chan Node[int], 20)
	for range 20 {
		wg.Go(func() { results <- In(&scope, n) })
	}
	wg.Wait()
	close(results)
	for got := range results {
		if got != In(&scope, n) {
			t.Fatal("resolution created duplicate instances")
		}
	}
	g := NewGraph()
	if err := g.Register(In(&scope, n)); err != nil {
		t.Fatal(err)
	}
	before := g.Stats()
	extra := In(&scope, Data[int]("extra"))
	if err := g.Register(extra, In(&scope, Data[int]("leaf"))); err == nil {
		t.Fatal("duplicate name accepted")
	}
	if g.Stats() != before {
		t.Fatal("failed registration partially modified graph")
	}
	if err := g.Register(In(new(Scope), leaf)); err != nil {
		t.Fatal("another scope cannot reuse class name", err)
	}
	if err := g.Register(leaf); err != nil {
		t.Fatal("unscoped namespace conflicts", err)
	}
	if err := g.Register(In(&scope, n)); err != nil {
		t.Fatal("repeat registration", err)
	}
}

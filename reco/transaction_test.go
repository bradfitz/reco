package reco

import (
	"fmt"
	"slices"
	"testing"
)

func mustRead[T any](t *testing.T, g *Graph, n Node[T]) T {
	t.Helper()
	s, err := Read(g, n)
	if err != nil || !s.Valid() {
		t.Fatalf("Read(%s): valid=%v, err=%v", n.ClassName(), s.Valid(), err)
	}
	return s.Value()
}

func update(t *testing.T, g *Graph, fn func(*Tx)) {
	t.Helper()
	if err := g.Update(func(tx *Tx) error { fn(tx); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestSetDeltaAtomicAndRollback(t *testing.T) {
	n := SetData[string]("set")
	g := NewGraph()
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { SetUpsert(tx, n, "old") })
	var events []Event[SetSnapshot[string]]
	_, err := Subscribe(g, n, SubscribeOptions{}, func(ev Event[SetSnapshot[string]]) { events = append(events, ev) })
	if err != nil {
		t.Fatal(err)
	}
	err = g.Update(func(tx *Tx) error {
		ApplySetDelta(tx, n, SetDelta[string]{Clear: true, Add: []string{"discard"}})
		return fmt.Errorf("abort")
	})
	if err == nil || len(events) != 0 || !mustRead(t, g, n).Contains("old") {
		t.Fatal("aborted transaction leaked")
	}
	update(t, g, func(tx *Tx) {
		ApplySetDelta(tx, n, SetDelta[string]{Clear: true, Remove: []string{"x"}, Add: []string{"x", "y", "x"}})
	})
	if len(events) != 1 {
		t.Fatalf("events=%d, want one", len(events))
	}
	var keys []string
	events[0].Current.Value().Range(func(k string) bool { keys = append(keys, k); return true })
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"x", "y"}) {
		t.Fatalf("got %v", keys)
	}
}

func TestFuncSkipsUnchangedDependencies(t *testing.T) {
	x, other := Data[int]("x"), Data[int]("other")
	calls := 0
	parity := Func("parity", Deps(struct{ X Node[int] }{x}), func(_ Eval, in struct{ X int }) Result[int] { return OK(in.X % 2) })
	out := Func("out", Deps(struct{ P Node[int] }{parity}), func(_ Eval, in struct{ P int }) Result[int] { calls++; return OK(in.P) })
	g := NewGraph()
	if err := g.Register(out, other); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, x, 0); Set(tx, other, 0) })
	update(t, g, func(tx *Tx) { Set(tx, x, 2); Set(tx, other, 1) })
	if calls != 1 {
		t.Fatalf("downstream recomputed with unchanged input: calls=%d", calls)
	}
	update(t, g, func(tx *Tx) { Set(tx, x, 3) })
	if calls != 2 || mustRead(t, g, out) != 1 {
		t.Fatal("changed dependency did not propagate")
	}
}

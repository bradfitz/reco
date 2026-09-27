package nodes_test

import (
	"math/rand/v2"
	"testing"

	"recontrol/reco"
	"recontrol/reco/nodes"
)

func mustRead[T any](t *testing.T, g *reco.Graph, n reco.Node[T]) T {
	t.Helper()
	s, err := reco.Read(g, n)
	if err != nil || !s.Valid() {
		t.Fatalf("reco.Read(%s): valid=%v, err=%v", n.ClassName(), s.Valid(), err)
	}
	return s.Value()
}

func update(t *testing.T, g *reco.Graph, fn func(*reco.Tx)) {
	t.Helper()
	if err := g.Update(func(tx *reco.Tx) error { fn(tx); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestUnionAndMapSet(t *testing.T) {
	a, b, c := reco.SetData[string]("a"), reco.SetData[string]("b"), reco.SetData[string]("c")
	u := nodes.Union("union", a, b, c)
	calls := 0
	m := nodes.MapSet("map", u, func(k string) int { calls++; return len(k) })
	g := reco.NewGraph()
	if err := g.Register(m); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) {
		for _, n := range []reco.Node[reco.SetSnapshot[string]]{a, b, c} {
			reco.Set(tx, n, reco.SetSnapshot[string]{})
		}
		reco.ApplySetDelta(tx, a, reco.SetDelta[string]{Add: []string{"one", "shared"}})
		reco.ApplySetDelta(tx, b, reco.SetDelta[string]{Add: []string{"shared"}})
	})
	initial := mustRead(t, g, m)
	if calls != 2 || initial.Len() != 2 {
		t.Fatalf("calls=%d, len=%d", calls, initial.Len())
	}
	var events []reco.MapEvent[string, int]
	_, sub, err := reco.SubscribeMap(g, m, reco.SubscribeOptions{}, func(e reco.MapEvent[string, int]) { events = append(events, e) })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	update(t, g, func(tx *reco.Tx) { reco.SetDelete(tx, a, "shared") })
	if len(events) != 0 || calls != 2 {
		t.Fatal("removing one contributor changed map")
	}
	update(t, g, func(tx *reco.Tx) { reco.SetDelete(tx, b, "shared"); reco.SetUpsert(tx, c, "shared") })
	if len(events) != 0 || calls != 2 {
		t.Fatal("moving a member between inputs changed map")
	}
	update(t, g, func(tx *reco.Tx) { reco.SetDelete(tx, c, "shared") })
	if len(events) != 1 || len(events[0].Changes) != 1 || events[0].Changes[0].AfterValid || calls != 2 {
		t.Fatalf("last-contributor removal: events=%+v calls=%d", events, calls)
	}
	update(t, g, func(tx *reco.Tx) {
		reco.ApplySetDelta(tx, a, reco.SetDelta[string]{Clear: true, Remove: []string{"new"}, Add: []string{"new", "new"}})
	})
	got := mustRead(t, g, m)
	if v, ok := got.Get("new"); !ok || v != 3 || got.Len() != 1 || calls != 3 {
		t.Fatalf("replace: len=%d calls=%d", got.Len(), calls)
	}
	if len(events) != 2 || len(events[1].Changes) != 2 {
		t.Fatalf("replace was not one atomic two-key event: %+v", events)
	}
	if initial.Len() != 2 {
		t.Fatal("old snapshot changed")
	}
	update(t, g, func(tx *reco.Tx) { reco.ApplySetDelta(tx, a, reco.SetDelta[string]{Clear: true}) })
	if mustRead(t, g, m).Len() != 0 {
		t.Fatal("clear did not propagate")
	}
}

func TestOperatorsIndependentPerGraphAndReplacement(t *testing.T) {
	a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
	u := nodes.Union("u", a, b, a) // Repeated inputs still have union semantics.
	m := nodes.MapSet("m", u, func(k int) int { return k * 2 })
	graphs := []*reco.Graph{reco.NewGraph(), reco.NewGraph()}
	for i, g := range graphs {
		if err := g.Register(m); err != nil {
			t.Fatal(err)
		}
		update(t, g, func(tx *reco.Tx) {
			reco.Set(tx, a, reco.SetSnapshot[int]{})
			reco.Set(tx, b, reco.SetSnapshot[int]{})
			reco.SetUpsert(tx, a, i+1)
		})
	}
	for i, g := range graphs {
		v := mustRead(t, g, m)
		if got, ok := v.Get(i + 1); !ok || got != 2*(i+1) || v.Len() != 1 {
			t.Fatal("operator state leaked across graphs")
		}
	}
	// A snapshot from another graph is a replacement, not a delta from ours.
	replacement := mustRead(t, graphs[1], a)
	update(t, graphs[0], func(tx *reco.Tx) { reco.Set(tx, a, replacement); reco.SetUpsert(tx, a, 3) })
	v := mustRead(t, graphs[0], m)
	if _, ok := v.Get(1); ok || v.Len() != 2 {
		t.Fatal("snapshot replacement applied stale changes")
	}
	if got, ok := v.Get(2); !ok || got != 4 {
		t.Fatal("replacement lost key")
	}
}

func TestDiamondComputesOnceWithWholeTransaction(t *testing.T) {
	source := reco.SetData[int]("source")
	leftEmpty, rightEmpty := reco.SetData[int]("le"), reco.SetData[int]("re")
	left := nodes.Union("left", source, leftEmpty)
	right := nodes.Union("right", source, rightEmpty)
	joined := nodes.Union("joined", left, right)
	calls := 0
	mapped := nodes.MapSet("mapped", joined, func(k int) int { calls++; return k })
	scalar := reco.Data[int]("unrelated")
	g := reco.NewGraph()
	if err := g.Register(mapped, scalar); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) {
		reco.Set(tx, source, reco.SetSnapshot[int]{})
		reco.Set(tx, leftEmpty, reco.SetSnapshot[int]{})
		reco.Set(tx, rightEmpty, reco.SetSnapshot[int]{})
		reco.Set(tx, scalar, 0)
	})
	for i := range 100 {
		update(t, g, func(tx *reco.Tx) {
			reco.SetUpsert(tx, source, i)
			reco.SetUpsert(tx, leftEmpty, i+100)
			reco.SetUpsert(tx, rightEmpty, i+200)
		})
	}
	if calls != 300 || mustRead(t, g, mapped).Len() != 300 {
		t.Fatalf("diamond calls=%d", calls)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, scalar, 1); reco.SetUpsert(tx, source, 0) })
	if calls != 300 {
		t.Fatal("unrelated update recomputed map entries")
	}
}

func TestUnionRandomTransactions(t *testing.T) {
	a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
	u := nodes.Union("u", a, b)
	m := nodes.MapSet("m", u, func(k int) int { return k * k })
	g := reco.NewGraph()
	if err := g.Register(m); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, a, reco.SetSnapshot[int]{}); reco.Set(tx, b, reco.SetSnapshot[int]{}) })
	want := []map[int]bool{{}, {}}
	r := rand.New(rand.NewPCG(1, 2))
	for iteration := range 500 {
		update(t, g, func(tx *reco.Tx) {
			for range 5 {
				i, k := r.IntN(2), r.IntN(30)
				n := []reco.Node[reco.SetSnapshot[int]]{a, b}[i]
				switch r.IntN(10) {
				case 0:
					reco.ApplySetDelta(tx, n, reco.SetDelta[int]{Clear: true, Add: []int{k}})
					want[i] = map[int]bool{k: true}
				case 1, 2, 3, 4:
					reco.SetDelete(tx, n, k)
					delete(want[i], k)
				default:
					reco.SetUpsert(tx, n, k)
					want[i][k] = true
				}
			}
		})
		out := mustRead(t, g, m)
		expectedLen := 0
		for k := range 30 {
			expected := want[0][k] || want[1][k]
			v, ok := out.Get(k)
			if ok != expected || (ok && v != k*k) {
				t.Fatalf("transaction %d key %d: value=%d present=%v want present=%v", iteration, k, v, ok, expected)
			}
			if expected {
				expectedLen++
			}
		}
		if out.Len() != expectedLen {
			t.Fatal("wrong map length")
		}
	}
}

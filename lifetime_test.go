package reco

import (
	"fmt"
	"sync"
	"testing"
)

func TestDemandLifetime(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	leaf := Data[int]("leaf")
	runs, factories := 0, 0
	shared := Operator("shared", []Dependency{leaf, leaf}, func() Compute[int] {
		factories++
		return func(e Eval) Result[int] { runs++; return OK(Input(e, leaf).Value() * 2) }
	})
	branch := func(name NodeClassName) Node[int] {
		return Func(name, Deps(struct{ N Node[int] }{shared}), func(_ Eval, in struct{ N int }) Result[int] { return OK(in.N + 1) })
	}
	a, b := branch("a"), branch("b")
	join := Func("join", Deps(struct{ A, B Node[int] }{a, b}), func(_ Eval, in struct{ A, B int }) Result[int] { return OK(in.A + in.B) })
	if err := g.Register(join); err != nil {
		t.Fatal(err)
	}
	set := func(v int) {
		t.Helper()
		if err := g.Update(func(tx *Tx) error { Set(tx, leaf, v); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	set(1)
	if runs != 0 || factories != 0 {
		t.Fatal("unwatched functions ran")
	}
	var events []int
	sub, err := Subscribe(g, join, SubscribeOptions{}, func(e Event[int]) { events = append(events, e.Current.Value()) })
	if err != nil {
		t.Fatal(err)
	}
	if runs != 1 || len(events) != 0 {
		t.Fatal("activation must settle once without an initial event")
	}
	other, err := Subscribe(g, a, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	set(2)
	if runs != 2 || len(events) != 1 || events[0] != 10 {
		t.Fatalf("diamond: runs=%d events=%v", runs, events)
	}
	sub.Unsubscribe()
	sub.Unsubscribe()
	if got := g.Stats(); got.ActiveFunctions != 2 || got.CachedFunctions != 2 || got.Subscriptions != 1 {
		t.Fatalf("shared demand lost/leaked: %+v", got)
	}
	set(3)
	if runs != 3 || len(events) != 1 {
		t.Fatal("shared input stopped or unsubscribed root ran")
	}
	other.Unsubscribe()
	if got := g.Stats(); got.ActiveFunctions != 0 || got.CachedFunctions != 0 || got.Subscriptions != 0 {
		t.Fatalf("leaked demand: %+v", got)
	}
	for _, node := range []Node[int]{shared, a, b, join} {
		if g.nodes[node.def].valid || g.nodes[node.def].value != nil {
			t.Fatal("retained derived value")
		}
	}
	if len(g.users) != 0 || len(g.subs) != 0 {
		t.Fatal("retained dependency watches")
	}
	if s := sub.(*subscription); s.g != nil || s.def != nil {
		t.Fatal("cancelled handle retained graph")
	}
	set(4)
	if runs != 3 {
		t.Fatal("unobserved recomputation")
	}
	for range 2 {
		v, err := Read(g, join)
		if err != nil || !v.Valid() || v.Value() != 18 {
			t.Fatalf("demand read: %v %v", v, err)
		}
		if got := g.Stats(); got.CachedFunctions != 0 || got.ActiveFunctions != 0 {
			t.Fatal("one-shot read retained demand")
		}
	}
	if runs != 5 || factories != 3 {
		t.Fatalf("cache lifetime: runs=%d factories=%d", runs, factories)
	}
}

func TestDemandMapAndStructReconnect(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	leaf := MapData[int, int]("leaf")
	work := 0
	mapped := Operator("mapped", []Dependency{leaf}, func() Compute[MapSnapshot[int, int]] {
		var previous, result MapSnapshot[int, int]
		return func(e Eval) Result[MapSnapshot[int, int]] {
			current := Input(e, leaf).Value()
			var d MapDelta[int, int]
			for c := range current.ChangesSince(previous) {
				work++
				if c.AfterValid {
					d.Put = append(d.Put, MapEntry[int, int]{Key: c.Key, Value: c.After * 2})
				} else {
					d.Remove = append(d.Remove, c.Key)
				}
			}
			result, previous = result.WithDelta(d), current
			return OK(result)
		}
	})
	type record struct{ Items MapSnapshot[int, int] }
	output := Func("record", Deps(struct{ Items Node[MapSnapshot[int, int]] }{mapped}), func(_ Eval, in record) Result[StructSnapshot[record]] { return OK(NewStruct(in)) })
	if err := g.Register(output); err != nil {
		t.Fatal(err)
	}
	var events []MapEvent[int, int]
	initial, mapSub, err := SubscribeMap(g, mapped, SubscribeOptions{}, func(e MapEvent[int, int]) { events = append(events, e) })
	if err != nil || initial.Valid() {
		t.Fatal("uninitialized input became valid")
	}
	update := func(k, v int) {
		t.Helper()
		if err := g.Update(func(tx *Tx) error { MapPut(tx, leaf, k, v); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 100 {
		update(i, i)
	}
	if work != 100 || len(events) != 100 {
		t.Fatal("map updates not incremental")
	}
	var recordEvents []StructEvent[record]
	first, structSub, err := SubscribeStruct(g, output, SubscribeOptions{}, func(e StructEvent[record]) { recordEvents = append(recordEvents, e) })
	if err != nil || first.Value().Value().Items.Len() != 100 || work != 100 {
		t.Fatal("shared map rebuilt during struct subscription")
	}
	mapSub.Unsubscribe()
	update(42, 999)
	if work != 101 || len(recordEvents) != 1 || len(events) != 100 {
		t.Fatal("subscription cancellation lost shared demand or deltas")
	}
	structSub.Unsubscribe()
	for i := range 100 {
		update(i, i+1)
	}
	if work != 101 || g.Stats().CachedFunctions != 0 {
		t.Fatal("dormant caches retained")
	}
	next, sub, err := SubscribeStruct(g, output, SubscribeOptions{}, func(StructEvent[record]) {})
	if err != nil || work != 201 {
		t.Fatal("reconnect did not rebuild once from current leaves")
	}
	if got, _ := next.Value().Value().Items.Get(42); got != 86 {
		t.Fatal("stale reconnect value")
	}
	if got, _ := first.Value().Value().Items.Get(42); got != 84 {
		t.Fatal("old snapshot mutated")
	}
	sub.Unsubscribe()
}

func TestDemandConstantAndConcurrentCancellation(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	constant := Operator("constant", nil, func() Compute[int] { return func(Eval) Result[int] { return OK(42) } })
	if err := g.Register(constant); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 20 {
				sub, err := Subscribe(g, constant, SubscribeOptions{}, func(Event[int]) {})
				if err != nil {
					t.Error(err)
					return
				}
				v, err := Read(g, constant)
				if err != nil || v.Value() != 42 {
					t.Error("bad constant")
				}
				sub.Unsubscribe()
				sub.Unsubscribe()
			}
		})
	}
	wg.Wait()
	// Concurrent callers may cancel the very same handle.
	sub, err := Subscribe(g, constant, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		wg.Go(func() { sub.Unsubscribe() })
	}
	wg.Wait()
	if got := g.Stats(); got.ActiveFunctions != 0 || got.CachedFunctions != 0 || got.Subscriptions != 0 {
		t.Fatalf("leaked: %+v", got)
	}
}

func BenchmarkDemandIdleFanout(b *testing.B) {
	for _, count := range []int{10, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
			leaf := Data[int]("leaf")
			for i := range count {
				n := Func(NodeClassName(fmt.Sprint(i)), Deps(struct{ N Node[int] }{leaf}), func(_ Eval, in struct{ N int }) Result[int] { return OK(in.N) })
				if err := g.Register(n); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				g.Update(func(tx *Tx) error { Set(tx, leaf, i); return nil })
			}
		})
	}
}

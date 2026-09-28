package reco

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"weak"
)

func TestDurableLifetime(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	data := MapData[string, int]("data")
	plain := Data[string]("plain")
	if err := g.Register(data, plain); err != nil {
		t.Fatal(err)
	}
	backing := (MapSnapshot[string, int]{}).WithDelta(MapDelta[string, int]{Put: []MapEntry[string, int]{{"a", 1}, {"b", 2}}})
	loads := 0
	if err := BindDurable(g, data, func() (MapSnapshot[string, int], error) { loads++; return backing, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error { Set(tx, plain, "keep"); return nil }); err != nil {
		t.Fatal(err)
	}
	var events []MapEvent[string, int]
	initial, sub, err := SubscribeMap(g, data, SubscribeOptions{}, func(ev MapEvent[string, int]) { events = append(events, ev) })
	if err != nil {
		t.Fatal(err)
	}
	_, sub2, err := SubscribeMap(g, data, SubscribeOptions{}, func(MapEvent[string, int]) {})
	if err != nil {
		t.Fatal(err)
	}
	if loads != 1 || initial.Value().Len() != 2 {
		t.Fatalf("loads=%d initial=%v", loads, initial)
	}
	called := 0
	update := func(d MapDelta[string, int]) error {
		return g.Update(func(tx *Tx) error {
			backing = backing.WithDelta(d) // durable commit under graph serialization
			UpdateDurable(tx, data, func(old MapSnapshot[string, int]) MapSnapshot[string, int] { called++; return old.WithDelta(d) })
			return nil
		})
	}
	if err := update(MapDelta[string, int]{Put: []MapEntry[string, int]{{"c", 3}}}); err != nil {
		t.Fatal(err)
	}
	if called != 1 || len(events) != 1 || len(events[0].Changes) != 1 {
		t.Fatalf("called=%d events=%v", called, events)
	}
	sub.Unsubscribe()
	if g.Stats().CachedDurableNodes != 1 {
		t.Fatal("evicted shared input")
	}
	sub2.Unsubscribe()
	if g.Stats().CachedDurableNodes != 0 {
		t.Fatal("retained idle value")
	}
	if v, _ := ReadCached(g, data); v.Valid() {
		t.Fatal("cold data is still cached")
	}
	if err := update(MapDelta[string, int]{Remove: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	if called != 1 || loads != 1 {
		t.Fatal("cold publication loaded/updated cache")
	}
	v, err := Read(g, data)
	_, hasC := v.Value().Get("c")
	if err != nil || loads != 2 || v.Value().Len() != 2 || !hasC {
		t.Fatalf("reload: %v %v loads=%d", v, err, loads)
	}
	if g.Stats().CachedDurableNodes != 0 {
		t.Fatal("Read retained demand")
	}
	_, hasB := initial.Value().Get("b")
	if initial.Value().Len() != 2 || !hasB {
		t.Fatal("eviction invalidated old snapshot")
	}
	p, _ := Read(g, plain)
	if p.Value() != "keep" {
		t.Fatal("evicted authoritative data")
	}
}

func TestDurableActivationFailure(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	a, b := Data[int]("a"), Data[int]("b")
	join := Func("join", Deps(struct{ A, B Node[int] }{a, b}), func(_ Eval, in struct{ A, B int }) Result[int] { return OK(in.A + in.B) })
	if err := g.Register(join); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("offline")
	fail := true
	BindDurable(g, a, func() (int, error) { return 1, nil })
	BindDurable(g, b, func() (int, error) {
		if fail {
			return 0, boom
		}
		return 2, nil
	})
	for range 3 {
		if _, err := Read(g, join); !errors.Is(err, boom) {
			t.Fatalf("Read: %v", err)
		}
		if _, err := Subscribe(g, join, SubscribeOptions{}, func(Event[int]) {}); !errors.Is(err, boom) {
			t.Fatalf("Subscribe: %v", err)
		}
		if s := g.Stats(); s.ActiveFunctions != 0 || s.CachedDurableNodes != 0 || s.Subscriptions != 0 || s.CachedFunctions != 0 {
			t.Fatalf("leaked activation: %+v", s)
		}
		if len(g.users) != 0 || len(g.refs) != 0 {
			t.Fatal("leaked edges or references")
		}
	}
	fail = false
	v, err := Read(g, join)
	if err != nil || v.Value() != 3 {
		t.Fatalf("retry = %v, %v", v, err)
	}
}

func TestDurableRejectsVolatileWrites(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	n := MapData[string, int]("map")
	plain := Data[int]("plain")
	g.Register(n, plain)
	BindDurable(g, n, func() (MapSnapshot[string, int], error) { return MapSnapshot[string, int]{}, nil })
	for _, mutate := range []func(*Tx){
		func(tx *Tx) { Set(tx, n, MapSnapshot[string, int]{}) },
		func(tx *Tx) { MapPut(tx, n, "a", 1) },
		func(tx *Tx) { MapDelete(tx, n, "a") },
	} {
		if err := g.Update(func(tx *Tx) error { Set(tx, plain, 42); mutate(tx); return nil }); err == nil {
			t.Fatal("accepted unsaved write")
		}
		if v, _ := Read(g, plain); v.Valid() {
			t.Fatal("failed transaction partly committed")
		}
	}
}

func ExampleBindDurable() {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	n := Data[string]("greeting")
	g.Register(n)
	stored := "hello" // Stand-in for a database record.
	BindDurable(g, n, func() (string, error) { return stored, nil })
	g.Update(func(tx *Tx) error {
		stored = "good morning" // Commit storage first; return errors before publication.
		UpdateDurable(tx, n, func(string) string { return stored })
		return nil
	})
	snapshot, _ := Read(g, n)
	fmt.Println(snapshot.Value(), g.Stats().CachedDurableNodes)
	// Output: good morning 0
}

func TestDurableSharedScopeAndConcurrentDemand(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	template := Data[int]("value")
	a, b := In(new(Scope), template), In(new(Scope), template)
	join := Func("join", Deps(struct{ A, B Node[int] }{a, b}), func(_ Eval, in struct{ A, B int }) Result[int] { return OK(in.A + in.B) })
	if err := g.Register(join); err != nil {
		t.Fatal(err)
	}
	BindDurable(g, a, func() (int, error) { return 10, nil })
	BindDurable(g, b, func() (int, error) { return 20, nil })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				sub, err := Subscribe(g, join, SubscribeOptions{}, func(Event[int]) {})
				if err != nil {
					t.Error(err)
					return
				}
				v, err := Read(g, join)
				if err != nil || v.Value() != 30 {
					t.Errorf("read = %v %v", v, err)
				}
				sub.Unsubscribe()
			}
		})
	}
	wg.Wait()
	if s := g.Stats(); s.CachedDurableNodes != 0 || s.CachedFunctions != 0 || s.Subscriptions != 0 {
		t.Fatalf("leak: %+v", s)
	}
}

type durableCompared struct {
	value       int
	comparisons *int
}

func (v durableCompared) RecoValueEqual(other any) bool {
	*v.comparisons++
	o, ok := other.(durableCompared)
	return ok && v.value == o.value
}

func TestDurableRepeatedPublicationStaysIncremental(t *testing.T) {
	for _, size := range []int{10, 10000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
			n := MapData[int, durableCompared]("map")
			g.Register(n)
			comparisons := 0
			d := MapDelta[int, durableCompared]{}
			for i := range size {
				d.Put = append(d.Put, MapEntry[int, durableCompared]{i, durableCompared{i, &comparisons}})
			}
			stored := (MapSnapshot[int, durableCompared]{}).WithDelta(d)
			BindDurable(g, n, func() (MapSnapshot[int, durableCompared], error) { return stored, nil })
			events := 0
			_, sub, err := SubscribeMap(g, n, SubscribeOptions{}, func(ev MapEvent[int, durableCompared]) {
				events++
				if len(ev.Changes) != 2 {
					t.Errorf("changes=%d", len(ev.Changes))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			comparisons = 0
			err = g.Update(func(tx *Tx) error {
				for i := range 2 {
					d := MapDelta[int, durableCompared]{Put: []MapEntry[int, durableCompared]{{i, durableCompared{-1, &comparisons}}}}
					stored = stored.WithDelta(d)
					UpdateDurable(tx, n, func(old MapSnapshot[int, durableCompared]) MapSnapshot[int, durableCompared] { return old.WithDelta(d) })
				}
				return nil
			})
			if err != nil || events != 1 || comparisons > 50 {
				t.Fatalf("err=%v events=%d comparisons=%d", err, events, comparisons)
			}
		})
	}
}

func TestDurableSetAndStructPublication(t *testing.T) {
	type record struct {
		Label   string
		Members SetSnapshot[int]
	}
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	set := SetData[int]("set")
	rec := StructData[record]("record")
	g.Register(set, rec)
	storedSet := (SetSnapshot[int]{}).WithDelta(SetDelta[int]{Add: []int{1, 2}})
	storedRecord := NewStruct(record{Label: "old", Members: storedSet})
	BindDurable(g, set, func() (SetSnapshot[int], error) { return storedSet, nil })
	BindDurable(g, rec, func() (StructSnapshot[record], error) { return storedRecord, nil })
	setEvents, recordEvents := 0, 0
	sub, _ := Subscribe(g, set, SubscribeOptions{}, func(e Event[SetSnapshot[int]]) {
		setEvents++
		if !e.Current.Value().Contains(3) {
			t.Error("lost set edit")
		}
	})
	_, recSub, err := SubscribeStruct(g, rec, SubscribeOptions{}, func(e StructEvent[record]) {
		recordEvents++
		if e.Changes.Len() != 2 {
			t.Errorf("fields=%d", e.Changes.Len())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	label := Field[record, string]("Label")
	members := Field[record, SetSnapshot[int]]("Members")
	err = g.Update(func(tx *Tx) error {
		for _, d := range []SetDelta[int]{{Remove: []int{1}}, {Add: []int{1, 3}}} {
			storedSet = storedSet.WithDelta(d)
			UpdateDurable(tx, set, func(old SetSnapshot[int]) SetSnapshot[int] { return old.WithDelta(d) })
		}
		for _, d := range []StructDelta[record]{{label.Set("new")}, {members.Set(storedSet)}} {
			storedRecord = storedRecord.WithDelta(d)
			UpdateDurable(tx, rec, func(old StructSnapshot[record]) StructSnapshot[record] { return old.WithDelta(d) })
		}
		return nil
	})
	if err != nil || setEvents != 1 || recordEvents != 1 {
		t.Fatalf("%v %d %d", err, setEvents, recordEvents)
	}
	sub.Unsubscribe()
	recSub.Unsubscribe()
	if g.Stats().CachedDurableNodes != 0 {
		t.Fatal("retained collections")
	}
	s, err := Read(g, rec)
	if err != nil || s.Value().Value().Label != "new" || !s.Value().Value().Members.Contains(3) {
		t.Fatal("bad reload", err)
	}
}

func TestDurableValueIsCollectible(t *testing.T) {
	type payload struct{ Bytes [4096]byte }
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	n := Data[*payload]("large")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	if err := BindDurable(g, n, func() (*payload, error) { return new(payload), nil }); err != nil {
		t.Fatal(err)
	}
	// Neither the binding nor temporary Read demand should retain this
	// allocation once the caller drops its snapshot. Keep the graph alive.
	ref := func() weak.Pointer[payload] {
		snap, err := Read(g, n)
		if err != nil {
			t.Fatal(err)
		}
		return weak.Make(snap.Value())
	}()
	for range 5 {
		runtime.GC()
		if ref.Value() == nil {
			runtime.KeepAlive(g)
			return
		}
	}
	runtime.KeepAlive(g)
	t.Fatal("durable payload remained reachable")
}

func TestBindDurableValidation(t *testing.T) {
	n := Data[int]("data")
	load := func() (int, error) { return 1, nil }
	eager := NewGraph()
	eager.Register(n)
	if err := BindDurable(eager, n, load); err == nil {
		t.Fatal("bound eager graph")
	}
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	if err := BindDurable(g, n, load); err == nil {
		t.Fatal("bound unregistered node")
	}
	g.Register(n)
	if err := BindDurable(g, n, nil); err == nil {
		t.Fatal("bound nil loader")
	}
	sub, err := Subscribe(g, n, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	if err := BindDurable(g, n, load); err == nil {
		t.Fatal("discarded observed value")
	}
	sub.Unsubscribe()
	if err := BindDurable(g, n, load); err != nil {
		t.Fatal(err)
	}
	if err := BindDurable(g, n, load); err == nil {
		t.Fatal("silently replaced binding")
	}
}

package reco

import (
	"sync"
	"testing"
)

func TestSubscribeSnapshotConcurrentSetup(t *testing.T) {
	g := NewGraph()
	n := Data[int]("counter")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error { Set(tx, n, 0); return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; i <= 500; i++ {
			if err := g.Update(func(tx *Tx) error { Set(tx, n, i); return nil }); err != nil {
				t.Error(err)
			}
		}
	})
	var events []Event[int] // graph serializes delivery; read after writer joins
	snap, sub, err := SubscribeSnapshot(g, n, SubscribeOptions{}, func(ev Event[int]) { events = append(events, ev) })
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	value := snap.Value()
	for _, ev := range events {
		if ev.Previous.Value() != value || ev.Current.Value() != value+1 {
			t.Fatalf("gap after %d: %+v", value, ev)
		}
		value = ev.Current.Value()
	}
	if value != 500 {
		t.Fatalf("snapshot + events ended at %d", value)
	}
}

func TestSubscribeSnapshotInvalidAndDemand(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	n := Data[int]("input")
	f := Func("double", Deps(struct{ N Node[int] }{n}), func(_ Eval, in struct{ N int }) Result[int] { return OK(in.N * 2) })
	if err := g.Register(f); err != nil {
		t.Fatal(err)
	}
	var event Event[int]
	snap, sub, err := SubscribeSnapshot(g, f, SubscribeOptions{}, func(ev Event[int]) { event = ev })
	if err != nil {
		t.Fatal(err)
	}
	if snap.Valid() {
		t.Fatal("uninitialized value was invented")
	}
	if err := g.Update(func(tx *Tx) error { Set(tx, n, 5); return nil }); err != nil {
		t.Fatal(err)
	}
	if event.Previous.Valid() || event.Current.Value() != 10 {
		t.Fatalf("initial event: %+v", event)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if st := g.Stats(); st.ActiveFunctions != 0 || st.Subscriptions != 0 {
		t.Fatalf("demand leaked: %+v", st)
	}
}

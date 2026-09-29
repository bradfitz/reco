package reco

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/benbjohnson/immutable"
)

func bagDelta[K comparable](tb testing.TB, s MultisetSnapshot[K], d MultisetDelta[K]) MultisetSnapshot[K] {
	tb.Helper()
	next, err := s.WithDelta(d)
	if err != nil {
		tb.Fatal(err)
	}
	return next
}

func TestMultisetSnapshot(t *testing.T) {
	var zero MultisetSnapshot[string]
	a := bagDelta(t, zero, MultisetDelta[string]{Put: []MultisetEntry[string]{{"a", 3}, {"b", 2}}})
	b := bagDelta(t, a, MultisetDelta[string]{Remove: []string{"b"}, Put: []MultisetEntry[string]{{"b", 7}, {"b", 4}}, Adjust: map[string]int64{"a": -1, "b": -4, "c": 2}})
	if b.Len() != 2 || b.Count("a") != 2 || b.Count("b") != 0 || b.Count("c") != 2 || a.Count("a") != 3 || a.Count("b") != 2 {
		t.Fatal("wrong multiplicities or mutable snapshot")
	}
	want := map[string]MultisetChange[string]{"a": {"a", 3, 2}, "b": {"b", 2, 0}, "c": {"c", 0, 2}}
	for c := range b.ChangesSince(a) {
		if want[c.Key] != c {
			t.Fatalf("unexpected change: %+v", c)
		}
		delete(want, c.Key)
	}
	if len(want) != 0 {
		t.Fatal("missing changes")
	}
	for _, d := range []MultisetDelta[string]{
		{}, {Adjust: map[string]int64{"a": 0}},
		{Remove: []string{"a"}, Put: []MultisetEntry[string]{{"a", 2}}},
		{Clear: true, Put: []MultisetEntry[string]{{"a", 2}, {"c", 2}}},
	} {
		if n := bagDelta(t, b, d); !n.RecoValueEqual(b) || len(slices.Collect(n.ChangesSince(b))) != 0 {
			t.Fatal("net-zero edit changed root")
		}
	}
	for _, d := range []MultisetDelta[string]{
		{Put: []MultisetEntry[string]{{"ok", 1}, {"bad", -1}}},
		{Put: []MultisetEntry[string]{{"a", -1}, {"a", 2}}}, // Even overwritten assignments must be valid.
		{Adjust: map[string]int64{"ok": 1, "absent": -1}},
		{Adjust: map[string]int64{"a": -3}},
		{Adjust: map[string]int64{"a": math.MinInt64}},
		{Adjust: map[string]int64{"a": math.MaxInt64}},
		{Clear: true, Adjust: map[string]int64{"a": -1}},
	} {
		if n, err := b.WithDelta(d); !errors.Is(err, ErrMultisetCount) || !n.RecoValueEqual(b) {
			t.Fatalf("invalid edit was not atomic: %v", err)
		}
	}
	max := bagDelta(t, zero, MultisetDelta[string]{Put: []MultisetEntry[string]{{"x", math.MaxInt64}}})
	if bagDelta(t, max, MultisetDelta[string]{Adjust: map[string]int64{"x": -math.MaxInt64}}).Len() != 0 {
		t.Fatal("last occurrence remained")
	}
	for _, stop := range []func(){
		func() {
			for range b.All() {
				break
			}
		},
		func() { b.Range(func(string, int64) bool { return false }) },
		func() {
			for range b.ChangesSince(a) {
				break
			}
		},
	} {
		stop()
	}
	if got := maps.Collect(b.All()); !maps.Equal(got, map[string]int64{"a": 2, "c": 2}) {
		t.Fatal(got)
	}
	// Counts is a read-only view, not a route to mutate this multiset.
	b.Counts().WithDelta(MapDelta[string, int64]{Put: []MapEntry[string, int64]{{"a", -100}}})
	if b.Count("a") != 2 {
		t.Fatal("map view modified source")
	}
}

func TestMultisetTransactions(t *testing.T) {
	g := NewGraph()
	n, marker := MultisetData[string]("bag"), Data[int]("marker")
	if err := g.Register(n, marker); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) {
		Set(tx, n, bagDelta(t, MultisetSnapshot[string]{}, MultisetDelta[string]{Adjust: map[string]int64{"a": 2}}))
	})
	events := 0
	var changes []MultisetChange[string]
	_, err := Subscribe(g, n, SubscribeOptions{}, func(e Event[MultisetSnapshot[string]]) {
		events++
		changes = slices.Collect(e.Current.Value().ChangesSince(e.Previous.Value()))
		if e.Current.Value().Version() != e.Version {
			t.Error("missing observed version")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(tx *Tx, d MultisetDelta[string]) {
		if err := ApplyMultisetDelta(tx, n, d); err != nil {
			t.Fatal(err)
		}
	}
	update(t, g, func(tx *Tx) {
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"a": -1}})
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"a": 1}})
	})
	if events != 0 {
		t.Fatal("net-zero transaction notified")
	}
	update(t, g, func(tx *Tx) {
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"a": 1}})
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"b": 4}})
	})
	if events != 1 || len(changes) != 2 {
		t.Fatalf("events=%d changes=%v", events, changes)
	}
	err = g.Update(func(tx *Tx) error {
		Set(tx, marker, 99)
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"b": 1}})
		return ApplyMultisetDelta(tx, n, MultisetDelta[string]{Adjust: map[string]int64{"a": -99}})
	})
	if !errors.Is(err, ErrMultisetCount) || events != 1 || mustRead(t, g, n).Count("b") != 4 {
		t.Fatal("failed transaction committed")
	}
	if snap, _ := Read(g, marker); snap.Valid() {
		t.Fatal("other leaf was not rolled back")
	}
	update(t, g, func(tx *Tx) {
		Set(tx, n, bagDelta(t, MultisetSnapshot[string]{}, MultisetDelta[string]{Put: []MultisetEntry[string]{{"z", 5}}}))
		apply(tx, MultisetDelta[string]{Adjust: map[string]int64{"z": 1}})
	})
	if events != 2 || len(changes) != 3 || mustRead(t, g, n).Count("z") != 6 {
		t.Fatal("replacement lost delta")
	}
}

func TestMultisetTransactionBatchWork(t *testing.T) {
	g := NewGraph()
	n := MultisetData[int]("counts")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	var old MultisetSnapshot[int]
	update(t, g, func(tx *Tx) { Set(tx, n, old) })
	update(t, g, func(tx *Tx) {
		var last *MapChange[int, int64]
		copies := 0
		for k := range 1024 {
			if err := ApplyMultisetDelta(tx, n, MultisetDelta[int]{Adjust: map[int]int64{k: 1}}); err != nil {
				t.Fatal(err)
			}
			cur := tx.writes[n.def].(MultisetSnapshot[int])
			if ptr := &cur.counts.changes[0]; ptr != last {
				copies++
				last = ptr
			}
		}
		if copies > 20 {
			t.Fatalf("copied accumulated delta %d times; want amortized growth", copies)
		}
	})
	next := mustRead(t, g, n)
	if got := len(slices.Collect(next.ChangesSince(old))); got != 1024 {
		t.Fatalf("got %d changes, want 1024", got)
	}
	// A Set of a derived snapshot must be copied before extending its metadata.
	// Two independent transactions may start from this same immutable value.
	provided := bagDelta(t, next, MultisetDelta[int]{Adjust: map[int]int64{-1: 2}})
	providedChanges := slices.Clone(provided.counts.changes)
	update(t, g, func(tx *Tx) {
		Set(tx, n, provided)
		if err := ApplyMultisetDelta(tx, n, MultisetDelta[int]{Adjust: map[int]int64{-2: 3}}); err != nil {
			t.Fatal(err)
		}
		staged := tx.writes[n.def].(MultisetSnapshot[int])
		if &staged.counts.changes[0] == &provided.counts.changes[0] {
			t.Fatal("transaction reused caller-owned delta buffer")
		}
	})
	if !reflect.DeepEqual(provided.counts.changes, providedChanges) {
		t.Fatal("caller delta changed")
	}
	if got := len(slices.Collect(mustRead(t, g, n).ChangesSince(next))); got != 2 {
		t.Fatalf("got %d changes after Set+Adjust, want 2", got)
	}
}

func TestMultisetDurableAccumulation(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	n := MultisetData[int]("counts")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	stored := bagDelta(t, MultisetSnapshot[int]{}, MultisetDelta[int]{Adjust: map[int]int64{1: 2}})
	if err := BindDurable(g, n, func() (MultisetSnapshot[int], error) { return stored, nil }); err != nil {
		t.Fatal(err)
	}
	events := 0
	sub, err := Subscribe(g, n, SubscribeOptions{}, func(e Event[MultisetSnapshot[int]]) {
		events++
		if len(slices.Collect(e.Current.Value().ChangesSince(e.Previous.Value()))) != 1 {
			t.Error("lost accumulated durable delta")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) {
		for _, d := range []MultisetDelta[int]{{Adjust: map[int]int64{1: 1, 2: 1}}, {Adjust: map[int]int64{1: -1, 2: 1}}} {
			stored = bagDelta(t, stored, d)
			UpdateDurable(tx, n, func(old MultisetSnapshot[int]) MultisetSnapshot[int] { return bagDelta(t, old, d) })
		}
	})
	if events != 1 || mustRead(t, g, n).Count(2) != 2 {
		t.Fatal("bad durable update")
	}
	if err := g.Update(func(tx *Tx) error { return ApplyMultisetDelta(tx, n, MultisetDelta[int]{Adjust: map[int]int64{1: 1}}) }); err == nil {
		t.Fatal("ordinary mutation of durable node accepted")
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if cached, _ := ReadCached(g, n); cached.Valid() {
		t.Fatal("durable counts remained resident")
	}
	if got := mustRead(t, g, n); got.Count(1) != 2 || got.Count(2) != 2 {
		t.Fatal("reload failed")
	}
}

func TestMultisetStructComposition(t *testing.T) {
	type rec struct{ Counts MultisetSnapshot[int] }
	f := Field[rec, MultisetSnapshot[int]]("Counts")
	a := NewStruct(rec{bagDelta(t, MultisetSnapshot[int]{}, MultisetDelta[int]{Adjust: map[int]int64{1: 2, 2: 3}})})
	b := a.WithDelta(StructDelta[rec]{f.Update(func(s MultisetSnapshot[int]) MultisetSnapshot[int] {
		return bagDelta(t, s, MultisetDelta[int]{Adjust: map[int]int64{1: 1, 2: -3}})
	})})
	c := b.WithDelta(StructDelta[rec]{f.Update(func(s MultisetSnapshot[int]) MultisetSnapshot[int] {
		return bagDelta(t, s, MultisetDelta[int]{Adjust: map[int]int64{1: -1, 3: 9}})
	})})
	combined, err := b.ChangesSince(a).Then(c.ChangesSince(b))
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]MultisetChange[int]{2: {2, 3, 0}, 3: {3, 0, 9}}
	for change := range MultisetFieldChanges(f, combined) {
		if want[change.Key] != change {
			t.Fatal(change)
		}
		delete(want, change.Key)
	}
	if combined.ChangeCount() != 2 || len(want) != 0 {
		t.Fatal("wrong coalesced changes")
	}
	normal := combined.normalized().Value().Counts
	if got := slices.Collect(normal.ChangesSince(a.Value().Counts)); len(got) != 2 {
		t.Fatal(got)
	}
	g := NewGraph()
	n := StructData[rec]("record")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, n, a) })
	update(t, g, func(tx *Tx) {
		ApplyStructDelta(tx, n, StructDelta[rec]{f.Set(b.Value().Counts)})
		ApplyStructDelta(tx, n, StructDelta[rec]{f.Set(c.Value().Counts)})
	})
	got := mustRead(t, g, n)
	if got.ChangesSince(a).ChangeCount() != 2 {
		t.Fatal("graph normalization lost typed delta")
	}
}

func TestMultisetRandomChangesAndReplacement(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 92))
	var cur MultisetSnapshot[int]
	ref := map[int]int64{}
	for i := range 500 {
		before, old := cur, maps.Clone(ref)
		if i%17 == 0 {
			cur, ref = MultisetSnapshot[int]{}, map[int]int64{}
		}
		k, n := rng.IntN(30), rng.Int64N(6)
		cur = bagDelta(t, cur, MultisetDelta[int]{Put: []MultisetEntry[int]{{k, n}}})
		if n == 0 {
			delete(ref, k)
		} else {
			ref[k] = n
		}
		for c := range cur.ChangesSince(before) {
			if c.Before != old[c.Key] || c.After != ref[c.Key] {
				t.Fatal("incorrect delta")
			}
			if c.After == 0 {
				delete(old, c.Key)
			} else {
				old[c.Key] = c.After
			}
		}
		if !maps.Equal(old, ref) || !maps.Equal(maps.Collect(cur.All()), ref) {
			t.Fatal("lost change")
		}
	}
}

func TestMultisetPointWork(t *testing.T) {
	for _, size := range []int{1024, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			h := &countingHasher{}
			h.reset()
			builder := immutable.NewMapBuilder[int, int64](h)
			for k := range size {
				builder.Set(k, 2)
			}
			before := MultisetSnapshot[int]{counts: MapSnapshot[int, int64]{items: builder.Map()}}
			h.reset()
			after := bagDelta(t, before, MultisetDelta[int]{Adjust: map[int]int64{5: -1}})
			if got := slices.Collect(after.ChangesSince(before)); !reflect.DeepEqual(got, []MultisetChange[int]{{5, 2, 1}}) {
				t.Fatal(got)
			}
			h.check(t, []int{5}, 40)
			assertSharedHAMT(t, before.counts.items, after.counts.items, 1)
		})
	}
}

func BenchmarkMultisetPointDelta(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var d MultisetDelta[int]
			for k := range size {
				d.Put = append(d.Put, MultisetEntry[int]{k, 2})
			}
			s := bagDelta(b, MultisetSnapshot[int]{}, d)
			d = MultisetDelta[int]{Adjust: map[int]int64{0: 1}}
			b.ReportAllocs()
			for b.Loop() {
				s = bagDelta(b, s, d)
				d.Adjust[0] = -d.Adjust[0]
			}
		})
	}
}

package reco

import (
	"fmt"
	"slices"
	"testing"

	"github.com/benbjohnson/immutable"
)

// StructWorkRecord is test-only, allowing reco_test to supply nodes.Struct.
type StructWorkRecord struct {
	Label   int
	Peers   MapSnapshot[int, int]
	Members SetSnapshot[int]
}

type StructNodeTestFunc func(NodeClassName, any) Node[StructSnapshot[StructWorkRecord]]

func TestStructDeltaWorkAndSharing(t *testing.T) {
	label := Field[StructWorkRecord, int]("Label")
	peers := Field[StructWorkRecord, MapSnapshot[int, int]]("Peers")
	members := Field[StructWorkRecord, SetSnapshot[int]]("Members")
	for _, size := range []int{1024, 100000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			hm, hs := &countingHasher{}, &countingHasher{}
			hm.reset()
			hs.reset()
			before := NewStruct(StructWorkRecord{Peers: makeMap(size, hm), Members: makeSet(size, hs)})
			hm.reset()
			hs.reset()
			after := before.WithDelta(StructDelta[StructWorkRecord]{label.Set(1)})
			if after.Value().Peers.items != before.Value().Peers.items || after.Value().Members.items != before.Value().Members.items {
				t.Fatal("scalar update replaced collection roots")
			}
			if after.ChangesSince(before).Len() != 1 || len(slices.Collect(MapFieldChanges(peers, after.ChangesSince(before)))) != 0 {
				t.Fatal("scalar edit dirtied collections")
			}
			hm.check(t, nil, 0)
			hs.check(t, nil, 0)
			before = after
			after = before.WithDelta(StructDelta[StructWorkRecord]{
				peers.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
					return m.WithDelta(MapDelta[int, int]{Put: []MapEntry[int, int]{{Key: size, Value: 1}}})
				}),
				peers.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
					return m.WithDelta(MapDelta[int, int]{Put: []MapEntry[int, int]{{Key: size + 1, Value: 2}}})
				}),
				members.Update(func(s SetSnapshot[int]) SetSnapshot[int] { return s.WithDelta(SetDelta[int]{Add: []int{size}}) }),
				members.Update(func(s SetSnapshot[int]) SetSnapshot[int] { return s.WithDelta(SetDelta[int]{Add: []int{size + 1}}) }),
			})
			c := after.ChangesSince(before)
			if len(slices.Collect(MapFieldChanges(peers, c))) != 2 || len(slices.Collect(SetFieldChanges(members, c))) != 2 {
				t.Fatal("wrong delta size")
			}
			// Both the outer change batch and extracted field snapshots retain lineage.
			if len(slices.Collect(after.Value().Peers.ChangesSince(before.Value().Peers))) != 2 || len(slices.Collect(after.Value().Members.ChangesSince(before.Value().Members))) != 2 {
				t.Fatal("nested lineage lost")
			}
			hm.check(t, []int{size, size + 1}, 256)
			hs.check(t, []int{size, size + 1}, 256)
			assertSharedHAMT(t, before.Value().Peers.items, after.Value().Peers.items, 2)
			assertSharedHAMT(t, before.Value().Members.items, after.Value().Members.items, 2)
			// Cancelling repeated field edits reuses the actual original roots.
			cancelled := before.WithDelta(StructDelta[StructWorkRecord]{
				peers.Set(after.Value().Peers),
				peers.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
					return m.WithDelta(MapDelta[int, int]{Remove: []int{size, size + 1}})
				}),
			})
			if cancelled.root != before.root {
				t.Fatal("net-zero record did not reuse root")
			}
		})
	}
}

func TestStructCompositionSharesPendingStorage(t *testing.T) {
	peers := Field[StructWorkRecord, MapSnapshot[int, int]]("Peers")
	base := NewStruct(StructWorkRecord{})
	var edits []MapEntry[int, int]
	for k := range 4096 {
		edits = append(edits, MapEntry[int, int]{Key: k, Value: k})
	}
	first := base.WithDelta(StructDelta[StructWorkRecord]{peers.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
		return m.WithDelta(MapDelta[int, int]{Put: edits})
	})})
	pending := first.ChangesSince(base)
	previousStorage := pending.fields[1].(mapStructChange[int, int]).entries
	second := first.WithDelta(StructDelta[StructWorkRecord]{peers.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
		return m.WithDelta(MapDelta[int, int]{Put: []MapEntry[int, int]{{Key: 17, Value: -1}}})
	})})
	combined, err := pending.Then(second.ChangesSince(first))
	if err != nil {
		t.Fatal(err)
	}
	assertSharedHAMT(t, previousStorage, combined.fields[1].(mapStructChange[int, int]).entries, 1)
	if c, _ := previousStorage.Get(17); c.After != 17 {
		t.Fatal("mutated pending batch")
	}
}

func TestStructUsesFieldEqualityHooks(t *testing.T) {
	m := makeMap(1000, newComparableHasher[int]())
	s := NewStruct(StructWorkRecord{Peers: m})
	m.version = 99 // Metadata changes alone must not dirty the record.
	m.changes = []MapChange[int, int]{{Key: 99}}
	equivalent := NewStruct(StructWorkRecord{Peers: m})
	if !s.RecoValueEqual(equivalent) || equivalent.ChangesSince(s).Len() != 0 {
		t.Fatal("compared collection metadata instead of roots")
	}
}

func CheckStructNodeWork(t *testing.T, build StructNodeTestFunc) {
	for _, size := range []int{1024, 100000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			label := Data[int]("label")
			peers := MapData[int, int]("peers")
			members := SetData[int]("members")
			n := build("mrm", struct {
				Label   Node[int]
				Peers   Node[MapSnapshot[int, int]]
				Members Node[SetSnapshot[int]]
			}{label, peers, members})
			calls := 0
			countCompute(n, &calls)
			g := NewGraph()
			if err := g.Register(n); err != nil {
				t.Fatal(err)
			}
			hm, hs := &countingHasher{}, &countingHasher{}
			hm.reset()
			hs.reset()
			update(t, g, func(tx *Tx) {
				Set(tx, label, 0)
				Set(tx, peers, makeMap(size, hm))
				Set(tx, members, makeSet(size, hs))
			})
			var pending StructChanges[StructWorkRecord]
			events := 0
			initial, sub, err := SubscribeStruct(g, n, SubscribeOptions{}, func(ev StructEvent[StructWorkRecord]) {
				events++
				var err error
				pending, err = pending.Then(ev.Changes)
				if err != nil {
					t.Fatal(err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			hm.reset()
			hs.reset()
			calls = 0
			// Simulate an initial writer holding its snapshot while updates accumulate.
			for k := range 16 {
				update(t, g, func(tx *Tx) { MapPut(tx, peers, k, -k-1) })
			}
			update(t, g, func(tx *Tx) { Set(tx, label, 1) })
			field := Field[StructWorkRecord, MapSnapshot[int, int]]("Peers")
			if calls != 17 || events != 17 || len(slices.Collect(MapFieldChanges(field, pending))) != 16 {
				t.Fatalf("calls=%d events=%d", calls, events)
			}
			keys := make([]int, 16)
			for k := range keys {
				keys[k] = k
			}
			hm.check(t, keys, 2048)
			hs.check(t, nil, 0)
			if v, _ := initial.Value().Value().Peers.Get(0); v != 0 {
				t.Fatal("initial snapshot mutated")
			}
			assertSharedHAMT(t, initial.Value().Value().Peers.items, pending.After().Value().Peers.items, 16)
			// A no-op transaction does not recompute or publish.
			update(t, g, func(tx *Tx) { MapPut(tx, peers, 0, -1); Set(tx, label, 1) })
			if calls != 17 || events != 17 {
				t.Fatal("no-op propagated")
			}
		})
	}
}

func TestStructTransactionRetainsNestedLineage(t *testing.T) {
	type pair struct {
		First  MapSnapshot[int, int]
		Second int
	}
	field := Field[pair, MapSnapshot[int, int]]("First")
	h := &countingHasher{}
	h.reset()
	n := StructData[pair]("record")
	g := NewGraph()
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, n, NewStruct(pair{First: makeMap(10000, h)})) })
	before := mustRead(t, g, n)
	h.reset()
	update(t, g, func(tx *Tx) {
		for k := range 16 {
			ApplyStructDelta(tx, n, StructDelta[pair]{field.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] {
				return m.WithDelta(MapDelta[int, int]{Put: []MapEntry[int, int]{{Key: k, Value: -1}}})
			})})
		}
	})
	after := mustRead(t, g, n)
	if len(slices.Collect(after.Value().First.ChangesSince(before.Value().First))) != 16 {
		t.Fatal("lost aggregate field delta")
	}
	allowed := make([]int, 16)
	for k := range allowed {
		allowed[k] = k
	}
	h.check(t, allowed, 2048)
}

func BenchmarkStructDelta(b *testing.B) {
	field := Field[StructWorkRecord, MapSnapshot[int, int]]("Peers")
	for _, size := range []int{1000, 100000} {
		for _, delta := range []int{1, 16} {
			b.Run(fmt.Sprintf("n=%d/delta=%d", size, delta), func(b *testing.B) {
				builder := immutable.NewMapBuilder[int, int](newComparableHasher[int]())
				for k := range size {
					builder.Set(k, k)
				}
				current := NewStruct(StructWorkRecord{Peers: MapSnapshot[int, int]{items: builder.Map()}})
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var puts []MapEntry[int, int]
					for k := range delta {
						puts = append(puts, MapEntry[int, int]{Key: k, Value: -i - 1})
					}
					next := current.WithDelta(StructDelta[StructWorkRecord]{field.Update(func(m MapSnapshot[int, int]) MapSnapshot[int, int] { return m.WithDelta(MapDelta[int, int]{Put: puts}) })})
					count := 0
					for range MapFieldChanges(field, next.ChangesSince(current)) {
						count++
					}
					if count != delta {
						b.Fatal(count)
					}
					current = next
				}
			})
		}
	}
}

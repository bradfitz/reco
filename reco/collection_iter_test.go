package reco

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"testing"
)

func TestSnapshotIterators(t *testing.T) {
	for _, size := range []int{-1, 0, 1, 257} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var set SetSnapshot[int]
			var mp MapSnapshot[int, int]
			if size >= 0 {
				set = makeSet(size, newComparableHasher[int]())
				mp = makeMap(size, newComparableHasher[int]())
			}
			var keys iter.Seq[int] = set.All()
			var entries iter.Seq2[int, int] = mp.All()
			for range 2 { // The same iterator is reusable, not single-use.
				seen := make(map[int]bool)
				for k := range keys {
					if seen[k] || !set.Contains(k) {
						t.Fatalf("unexpected or duplicate key %d", k)
					}
					seen[k] = true
				}
				if len(seen) != set.Len() {
					t.Fatalf("visited %d/%d set keys", len(seen), set.Len())
				}
				pairs := make(map[int]int)
				for k, v := range entries {
					if _, seen := pairs[k]; seen || k != v {
						t.Fatalf("unexpected or duplicate entry %d:%d", k, v)
					}
					pairs[k] = v
				}
				if len(pairs) != mp.Len() {
					t.Fatalf("visited %d/%d map entries", len(pairs), mp.Len())
				}
			}
			if got := slices.Collect(keys); len(got) != set.Len() {
				t.Fatal("iter.Seq interoperability failed")
			}
			if got := maps.Collect(entries); len(got) != mp.Len() {
				t.Fatal("iter.Seq2 interoperability failed")
			}
			// The callback API remains supported and yields the same contents.
			want := make(map[int]int)
			mp.Range(func(k, v int) bool { want[k] = v; return true })
			if !maps.Equal(maps.Collect(entries), want) {
				t.Fatal("All differs from Range")
			}
		})
	}
}

func TestSnapshotIteratorsStopAndRestart(t *testing.T) {
	set := makeSet(1024, newComparableHasher[int]())
	mp := makeMap(1024, newComparableHasher[int]())
	keys, entries := set.All(), mp.All()
	for _, limit := range []int{1, 7} {
		count := 0
		for range keys {
			count++
			if count == limit {
				break
			}
		}
		if count != limit {
			t.Fatalf("set iterator visited %d keys, want %d", count, limit)
		}
		count = 0
		for range entries {
			count++
			if count == limit {
				break
			}
		}
		if count != limit {
			t.Fatalf("map iterator visited %d entries, want %d", count, limit)
		}
	}
	// Returning false must never invoke yield again, even outside a range loop.
	setYields, mapYields := 0, 0
	keys(func(int) bool { setYields++; return false })
	entries(func(int, int) bool { mapYields++; return false })
	if setYields != 1 || mapYields != 1 {
		t.Fatalf("iterator continued after false: set=%d map=%d", setYields, mapYields)
	}
	if len(slices.Collect(keys)) != set.Len() || len(maps.Collect(entries)) != mp.Len() {
		t.Fatal("early exit consumed the iterator")
	}
	// Nested iteration uses independent cursors.
	count := 0
	for range keys {
		for range keys {
			count++
			break
		}
		break
	}
	if count != 1 {
		t.Fatal("nested set iteration failed")
	}
	for range entries {
		for range entries {
			count++
			break
		}
		break
	}
	if count != 2 {
		t.Fatal("nested map iteration failed")
	}
}

func TestSnapshotIteratorsRetainSnapshot(t *testing.T) {
	s, m := SetData[int]("set"), MapData[int, int]("map")
	g := NewGraph()
	if err := g.Register(s, m); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) {
		Set(tx, s, makeSet(3, newComparableHasher[int]()))
		Set(tx, m, makeMap(3, newComparableHasher[int]()))
	})
	set, mp := mustRead(t, g, s), mustRead(t, g, m)
	keys, entries := set.All(), mp.All()
	update(t, g, func(tx *Tx) {
		SetDelete(tx, s, 0)
		SetUpsert(tx, s, 3)
		MapDelete(tx, m, 0)
		MapPut(tx, m, 1, 100)
		MapPut(tx, m, 3, 3)
	})
	// Even assigning new snapshots to the source variables cannot retarget a
	// previously obtained iterator: the receiver was captured by value.
	set, mp = mustRead(t, g, s), mustRead(t, g, m)
	if got := slices.Sorted(keys); !slices.Equal(got, []int{0, 1, 2}) {
		t.Fatalf("set iterator changed snapshot: %v", got)
	}
	if got := maps.Collect(entries); !maps.Equal(got, map[int]int{0: 0, 1: 1, 2: 2}) {
		t.Fatalf("map iterator changed snapshot: %v", got)
	}
	if got := slices.Sorted(set.All()); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("new set snapshot: %v", got)
	}
	if got := maps.Collect(mp.All()); !maps.Equal(got, map[int]int{1: 100, 2: 2, 3: 3}) {
		t.Fatalf("new map snapshot: %v", got)
	}
}

// A prefix iteration must not materialize N elements before yielding. Measuring
// allocated bytes as well as allocations exposes even a single O(N) slice copy.
func BenchmarkSnapshotIteratorPrefix(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			set := makeSet(size, newComparableHasher[int]())
			mp := makeMap(size, newComparableHasher[int]())
			b.ReportAllocs()
			for b.Loop() {
				for range set.All() {
					break
				}
				for range mp.All() {
					break
				}
			}
		})
	}
}

package reco

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/benbjohnson/immutable"
)

// Counting hashes catches a full reconciliation of an unchanged collection:
// comparing all its memberships would hash all N keys, not just the delta.
// These assertions measure actual storage operations, not elapsed wall time.
type countingHasher struct {
	keys  map[int]int
	reads int
}

func (h *countingHasher) Hash(k int) uint32 {
	h.keys[k]++
	return uint32(k) * 2654435761 // bijection for the small integer keys in these tests
}

func (h *countingHasher) Equal(a, b int) bool { h.reads++; return a == b }
func (h *countingHasher) reset()              { h.keys = make(map[int]int); h.reads = 0 }

func (h *countingHasher) check(t *testing.T, allowed []int, budget int) {
	t.Helper()
	total := 0
	for k, n := range h.keys {
		if !slices.Contains(allowed, k) {
			t.Fatalf("hashed untouched key %d (%d times); delta keys: %v", k, n, allowed)
		}
		total += n
	}
	if total > budget || h.reads > budget {
		t.Fatalf("storage work hashes=%d equality=%d exceeds delta budget %d", total, h.reads, budget)
	}
}

func countCompute[T any](node Node[T], calls *int) {
	wrap := func(fn computeFunc) computeFunc {
		return func(eval Eval, vals map[*nodeDef]nodeValue) nodeValue { *calls++; return fn(eval, vals) }
	}
	if factory := node.def.newCompute; factory != nil {
		node.def.newCompute = func() computeFunc { return wrap(factory()) }
	} else {
		node.def.compute = wrap(node.def.compute)
	}
}

func makeSet(n int, hasher immutable.Hasher[int]) SetSnapshot[int] {
	b := immutable.NewMapBuilder[int, struct{}](hasher)
	for k := range n {
		b.Set(k, struct{}{})
	}
	return SetSnapshot[int]{items: b.Map()}
}

func makeMap(n int, hasher immutable.Hasher[int]) MapSnapshot[int, int] {
	b := immutable.NewMapBuilder[int, int](hasher)
	for k := range n {
		b.Set(k, k)
	}
	return MapSnapshot[int, int]{items: b.Map()}
}

// OperatorTestFuncs lets external tests supply the standard nodes to these
// white-box checks without importing reco/nodes into package reco. It exists
// only in test builds; no instrumentation is exposed by the production API.
type OperatorTestFuncs struct {
	Union  func(NodeClassName, ...Node[SetSnapshot[int]]) Node[SetSnapshot[int]]
	MapSet func(NodeClassName, Node[SetSnapshot[int]], func(int) int) Node[MapSnapshot[int, int]]
}

func CheckCollectionDeltaWork(t *testing.T, operators OperatorTestFuncs) {
	for _, size := range []int{1024, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a, b := SetData[int]("a"), SetData[int]("b")
			dataMap := MapData[int, int]("data-map")
			u := operators.Union("union", a, b)
			mappedKeys := []int{}
			m := operators.MapSet("map", u, func(k int) int { mappedKeys = append(mappedKeys, k); return k * 2 })
			unionCalls, mapCalls := 0, 0
			countCompute(u, &unionCalls)
			countCompute(m, &mapCalls)
			g := NewGraph()
			if err := g.Register(m, dataMap); err != nil {
				t.Fatal(err)
			}
			ha, hb, hm := &countingHasher{}, &countingHasher{}, &countingHasher{}
			for _, h := range []*countingHasher{ha, hb, hm} {
				h.reset()
			}
			update(t, g, func(tx *Tx) {
				Set(tx, a, makeSet(size, ha))
				Set(tx, b, makeSet(size, hb))
				Set(tx, dataMap, makeMap(size, hm))
			})
			if len(mappedKeys) != size {
				t.Fatalf("initial F calls=%d, want %d", len(mappedKeys), size)
			}
			mapEvents, setEvents := 0, 0
			var lastMap []MapChange[int, int]
			_, _, err := SubscribeMap(g, m, SubscribeOptions{}, func(e MapEvent[int, int]) { mapEvents++; lastMap = e.Changes })
			if err != nil {
				t.Fatal(err)
			}
			_, err = Subscribe(g, u, SubscribeOptions{}, func(Event[SetSnapshot[int]]) { setEvents++ })
			if err != nil {
				t.Fatal(err)
			}

			tests := []struct {
				name                          string
				mutate                        func(*Tx)
				keys, mapped                  []int
				unionCalls, mapCalls, changes int
			}{
				{"add-one", func(tx *Tx) { SetUpsert(tx, a, size) }, []int{size}, []int{size}, 1, 1, 1},
				{"add-batch", func(tx *Tx) {
					for k := size + 1; k <= size+4; k++ {
						SetUpsert(tx, a, k)
					}
				}, []int{size + 1, size + 2, size + 3, size + 4}, []int{size + 1, size + 2, size + 3, size + 4}, 1, 1, 4},
				{"duplicate-add-and-absent-remove", func(tx *Tx) { SetUpsert(tx, a, size); SetDelete(tx, a, -1) }, []int{size, -1}, nil, 0, 0, 0},
				{"remove-one-contributor", func(tx *Tx) { SetDelete(tx, a, 0) }, []int{0}, nil, 1, 0, 0},
				{"remove-last-contributor", func(tx *Tx) { SetDelete(tx, b, 0) }, []int{0}, nil, 1, 1, 1},
				{"move-between-inputs", func(tx *Tx) { SetDelete(tx, a, size); SetUpsert(tx, b, size) }, []int{size}, nil, 1, 0, 0},
				{"remove-add-same-member", func(tx *Tx) { ApplySetDelta(tx, a, SetDelta[int]{Remove: []int{1}, Add: []int{1}}) }, []int{1}, nil, 0, 0, 0},
				{"map-put", func(tx *Tx) { MapPut(tx, dataMap, 1, -1) }, []int{1}, nil, 0, 0, 0},
				{"identical-map-put", func(tx *Tx) { MapPut(tx, dataMap, 1, -1) }, []int{1}, nil, 0, 0, 0},
				{"map-delete", func(tx *Tx) { MapDelete(tx, dataMap, 2) }, []int{2}, nil, 0, 0, 0},
				{"absent-map-delete", func(tx *Tx) { MapDelete(tx, dataMap, -1) }, []int{-1}, nil, 0, 0, 0},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					for _, h := range []*countingHasher{ha, hb, hm} {
						h.reset()
					}
					mappedKeys = nil
					unionCalls, mapCalls, mapEvents, setEvents, lastMap = 0, 0, 0, 0, nil
					update(t, g, tt.mutate)
					slices.Sort(mappedKeys)
					if !slices.Equal(mappedKeys, tt.mapped) {
						t.Fatalf("F ran on %v; want only %v", mappedKeys, tt.mapped)
					}
					if unionCalls != tt.unionCalls || mapCalls != tt.mapCalls {
						t.Fatalf("computations union=%d map=%d; want %d,%d", unionCalls, mapCalls, tt.unionCalls, tt.mapCalls)
					}
					wantEvents := 0
					if tt.changes > 0 {
						wantEvents = 1
					}
					if mapEvents != wantEvents || setEvents != wantEvents || len(lastMap) != tt.changes {
						t.Fatalf("events map=%d set=%d; map changes=%d; want %d events and %d changes", mapEvents, setEvents, len(lastMap), wantEvents, tt.changes)
					}
					for _, h := range []*countingHasher{ha, hb, hm} {
						h.check(t, tt.keys, 64*len(tt.keys))
					}
				})
			}
		})
	}
}

// Inspect only immutable's HAMT node pointers, never entry values. This is a
// deliberate white-box regression guard tied to the pinned immutable version:
// checking value equality alone cannot detect rebuilding the entire backing
// store. No unsafe access or production instrumentation is needed.
func hamtNodes(t *testing.T, m any) map[uintptr]bool {
	t.Helper()
	root := reflect.ValueOf(m).Elem().FieldByName("root")
	if !root.IsValid() {
		t.Fatal("immutable layout changed: no HAMT root")
	}
	out := make(map[uintptr]bool)
	var visit func(reflect.Value)
	visit = func(v reflect.Value) {
		if v.IsNil() {
			return
		}
		v = v.Elem() // mapNode interface -> concrete pointer
		out[v.Pointer()] = true
		if children := v.Elem().FieldByName("nodes"); children.IsValid() {
			for i := range children.Len() {
				visit(children.Index(i))
			}
		}
	}
	visit(root)
	return out
}

func assertSharedHAMT[K comparable, V any](t *testing.T, before, after *immutable.Map[K, V], delta int) {
	t.Helper()
	a, b := hamtNodes(t, before), hamtNodes(t, after)
	shared := 0
	for p := range b {
		if a[p] {
			shared++
		}
	}
	// A 32-bit HAMT has at most seven 5-bit levels. Leave generous room for
	// branch splits/merges, but no dependence on the number of stored entries.
	if fresh := len(b) - shared; fresh > 16*delta+16 {
		t.Fatalf("allocated %d new HAMT nodes for delta=%d (backing store=%d nodes)", fresh, delta, len(a))
	}
	if shared < len(a)/2 {
		t.Fatalf("only %d/%d old HAMT nodes reused; backing store was rebuilt", shared, len(a))
	}
	runtime.KeepAlive(before)
	runtime.KeepAlive(after)
}

func CheckCollectionStructuralSharing(t *testing.T, operators OperatorTestFuncs) {
	const size = 8192
	a, b := SetData[int]("a"), SetData[int]("b")
	mp := MapData[int, int]("data-map")
	u := operators.Union("union", a, b)
	m := operators.MapSet("map", u, func(k int) int { return k * 2 })
	g := NewGraph()
	if err := g.Register(m, mp); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) {
		Set(tx, a, makeSet(size, newComparableHasher[int]()))
		Set(tx, b, SetSnapshot[int]{})
		Set(tx, mp, makeMap(size, newComparableHasher[int]()))
	})
	for _, delta := range []int{1, 16} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("delta=%d/remove=%v", delta, remove), func(t *testing.T) {
				oldA, oldU := mustRead(t, g, a), mustRead(t, g, u)
				oldM, oldMP := mustRead(t, g, m), mustRead(t, g, mp)
				update(t, g, func(tx *Tx) {
					for k := size; k < size+delta; k++ {
						if remove {
							SetDelete(tx, a, k)
							MapDelete(tx, mp, k)
						} else {
							SetUpsert(tx, a, k)
							MapPut(tx, mp, k, k)
						}
					}
				})
				newA, newU := mustRead(t, g, a), mustRead(t, g, u)
				newM, newMP := mustRead(t, g, m), mustRead(t, g, mp)
				assertSharedHAMT(t, oldA.items, newA.items, delta)
				assertSharedHAMT(t, oldU.items, newU.items, delta)
				assertSharedHAMT(t, oldM.items, newM.items, delta)
				assertSharedHAMT(t, oldMP.items, newMP.items, delta)
				if oldA.Contains(size) != remove || oldU.Contains(size) != remove {
					t.Fatal("old set snapshots mutated")
				}
				if _, ok := oldM.Get(size); ok != remove {
					t.Fatal("old derived map snapshot mutated")
				}
				if _, ok := oldMP.Get(size); ok != remove {
					t.Fatal("old data map snapshot mutated")
				}
				if newA.Contains(size) == remove || newU.Contains(size) == remove {
					t.Fatal("set delta missing")
				}
			})
		}
	}
	old := mustRead(t, g, mp)
	update(t, g, func(tx *Tx) { MapPut(tx, mp, 42, -1) })
	next := mustRead(t, g, mp)
	assertSharedHAMT(t, old.items, next.items, 1)
	if v, _ := old.Get(42); v != 42 {
		t.Fatal("old map entry overwritten")
	}
	update(t, g, func(tx *Tx) { MapPut(tx, mp, 42, -1); MapDelete(tx, mp, -1) })
	if mustRead(t, g, mp).items != next.items {
		t.Fatal("no-op map mutation replaced its backing store")
	}
	set := mustRead(t, g, a)
	update(t, g, func(tx *Tx) { SetUpsert(tx, a, 42); SetDelete(tx, a, -1) })
	if mustRead(t, g, a).items != set.items {
		t.Fatal("no-op set mutation replaced its backing store")
	}
}

func TestFuncMinimumComputations(t *testing.T) {
	for _, inline := range []bool{false, true} {
		t.Run(fmt.Sprintf("inline=%v", inline), func(t *testing.T) {
			a, b, unrelated := Data[int]("a"), Data[int]("b"), Data[int]("unrelated")
			calls, downstream := 0, 0
			var sum Node[int]
			if inline {
				sum = Func("sum", Deps(struct{ A, B Node[int] }{a, b}), func(_ Eval, in struct{ A, B int }) Result[int] { calls++; return OK(in.A + in.B) })
			} else {
				sum = Func("sum", struct{ A, B Node[int] }{a, b}, func(_ Eval, in struct{ A, B Dep[int] }) Result[int] {
					calls++
					return OK(in.A.Value() + in.B.Value())
				})
			}
			out := Func("out", Deps(struct{ Sum Node[int] }{sum}), func(_ Eval, in struct{ Sum int }) Result[int] { downstream++; return OK(in.Sum * 2) })
			g := NewGraph()
			if err := g.Register(out, unrelated); err != nil {
				t.Fatal(err)
			}
			update(t, g, func(tx *Tx) { Set(tx, a, 1); Set(tx, b, 2); Set(tx, unrelated, 0) })
			tests := []struct {
				name              string
				mutate            func(*Tx)
				calls, downstream int
			}{
				{"no-op", func(tx *Tx) { Set(tx, a, 1); Set(tx, b, 2) }, 0, 0},
				{"unrelated", func(tx *Tx) { Set(tx, unrelated, 1) }, 0, 0},
				{"both-inputs-one-run", func(tx *Tx) { Set(tx, a, 10); Set(tx, b, 20) }, 1, 1},
				{"equal-output-stops-propagation", func(tx *Tx) { Set(tx, a, 11); Set(tx, b, 19) }, 1, 0},
				{"overwrite-within-tx", func(tx *Tx) { Set(tx, a, 12); Set(tx, a, 13); Set(tx, a, 11) }, 0, 0},
				{"empty-transaction", func(*Tx) {}, 0, 0},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					calls, downstream = 0, 0
					update(t, g, tt.mutate)
					if calls != tt.calls || downstream != tt.downstream {
						t.Fatalf("calls=%d downstream=%d, want %d,%d", calls, downstream, tt.calls, tt.downstream)
					}
				})
			}
		})
	}
}

type untouchedValue struct{}

func (untouchedValue) RecoValueEqual(any) bool { panic("compared an unrelated node") }

func TestDeltaDoesNotVisitUnrelatedSubscriptions(t *testing.T) {
	g := NewGraph()
	source := Data[int]("source")
	if err := g.Register(source); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, source, 0) })
	for i := range 1000 {
		n := Data[untouchedValue](NodeClassName(fmt.Sprintf("unrelated-%d", i)))
		if err := g.Register(n); err != nil {
			t.Fatal(err)
		}
		update(t, g, func(tx *Tx) { Set(tx, n, untouchedValue{}) })
		if _, err := Subscribe(g, n, SubscribeOptions{}, func(Event[untouchedValue]) { t.Fatal("unrelated notification") }); err != nil {
			t.Fatal(err)
		}
	}
	update(t, g, func(tx *Tx) { Set(tx, source, 1) })
}

func CheckMapSetDirectDeltaAndNetZeroBatches(t *testing.T, operators OperatorTestFuncs) {
	s := SetData[int]("set")
	calls := []int{}
	m := operators.MapSet("mapped", s, func(k int) int { calls = append(calls, k); return k * 2 })
	data := MapData[int, int]("data")
	g := NewGraph()
	if err := g.Register(m, data); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) {
		Set(tx, s, makeSet(4096, newComparableHasher[int]()))
		Set(tx, data, makeMap(4096, newComparableHasher[int]()))
	})
	calls = nil
	update(t, g, func(tx *Tx) { ApplySetDelta(tx, s, SetDelta[int]{Remove: []int{0, 1}, Add: []int{4096, 4097, 4096}}) })
	slices.Sort(calls)
	if !slices.Equal(calls, []int{4096, 4097}) {
		t.Fatalf("F called for %v, want only new members", calls)
	}
	oldSet, oldMap := mustRead(t, g, s), mustRead(t, g, data)
	calls = nil
	update(t, g, func(tx *Tx) {
		SetDelete(tx, s, 2)
		SetUpsert(tx, s, 2)
		SetUpsert(tx, s, -1)
		SetDelete(tx, s, -1)
		MapPut(tx, data, 2, -2)
		MapPut(tx, data, 2, 2)
		MapPut(tx, data, -1, -1)
		MapDelete(tx, data, -1)
	})
	if len(calls) != 0 || mustRead(t, g, s).items != oldSet.items || mustRead(t, g, data).items != oldMap.items {
		t.Fatal("net-zero transaction recomputed values or replaced persistent roots")
	}
}

func TestSparseSchedulerLateRegistrationAndReadiness(t *testing.T) {
	g := NewGraph()
	a, b := Data[int]("a"), Data[int]("b")
	calls := 0
	sum := Func("sum", Deps(struct{ A, B Node[int] }{a, b}), func(_ Eval, in struct{ A, B int }) Result[int] { calls++; return OK(in.A + in.B) })
	if err := g.Register(sum); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, a, 1) })
	if calls != 0 {
		t.Fatal("computed before all inputs ready")
	}
	update(t, g, func(tx *Tx) { Set(tx, b, 2) })
	if calls != 1 || mustRead(t, g, sum) != 3 {
		t.Fatal("pending dependency did not wake function")
	}
	constantCalls := 0
	constant := Func("constant", Deps(struct{}{}), func(Eval, struct{}) Result[int] { constantCalls++; return OK(42) })
	if err := g.Register(constant, sum); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(*Tx) {})
	update(t, g, func(*Tx) {})
	if constantCalls != 1 || calls != 1 {
		t.Fatalf("late registration calls: constant=%d sum=%d", constantCalls, calls)
	}
}

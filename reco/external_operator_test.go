package reco_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	"recontrol/reco"
	"recontrol/reco/nodes"
)

// These implementations deliberately live outside package reco. They use no
// private fields, reflection, unsafe, or calls to the built-in Union/MapSet.
func externalMapSet[K comparable, V any](className reco.NodeClassName, input reco.Node[reco.SetSnapshot[K]], f func(K) V) reco.Node[reco.MapSnapshot[K, V]] {
	return reco.Operator(className, []reco.Dependency{input}, func() reco.Compute[reco.MapSnapshot[K, V]] {
		var previous reco.SetSnapshot[K]
		var result reco.MapSnapshot[K, V]
		return func(eval reco.Eval) reco.Result[reco.MapSnapshot[K, V]] {
			current := reco.Input(eval, input).Value()
			var delta reco.MapDelta[K, V]
			for c := range current.ChangesSince(previous) {
				if c.Present {
					delta.Put = append(delta.Put, reco.MapEntry[K, V]{Key: c.Key, Value: f(c.Key)})
				} else {
					delta.Remove = append(delta.Remove, c.Key)
				}
			}
			previous = current
			result = result.WithDelta(delta)
			return reco.OK(result)
		}
	})
}

func externalUnion[K comparable](className reco.NodeClassName, inputs ...reco.Node[reco.SetSnapshot[K]]) reco.Node[reco.SetSnapshot[K]] {
	inputs = slices.Clone(inputs)
	deps := make([]reco.Dependency, len(inputs))
	for i, n := range inputs {
		deps[i] = n
	}
	return reco.Operator(className, deps, func() reco.Compute[reco.SetSnapshot[K]] {
		last := make([]reco.SetSnapshot[K], len(inputs))
		var counts reco.MapSnapshot[K, int]
		var result reco.SetSnapshot[K]
		return func(eval reco.Eval) reco.Result[reco.SetSnapshot[K]] {
			net := make(map[K]int)
			for i, n := range inputs {
				current := reco.Input(eval, n).Value()
				for c := range current.ChangesSince(last[i]) {
					if c.Present {
						net[c.Key]++
					} else {
						net[c.Key]--
					}
				}
				last[i] = current
			}
			var members reco.SetDelta[K]
			var contributors reco.MapDelta[K, int]
			for k, delta := range net {
				if delta == 0 {
					continue
				}
				before, _ := counts.Get(k)
				after := before + delta
				if after == 0 {
					contributors.Remove = append(contributors.Remove, k)
					members.Remove = append(members.Remove, k)
				} else {
					contributors.Put = append(contributors.Put, reco.MapEntry[K, int]{Key: k, Value: after})
					if before == 0 {
						members.Add = append(members.Add, k)
					}
				}
			}
			counts = counts.WithDelta(contributors)
			result = result.WithDelta(members)
			return reco.OK(result)
		}
	})
}

func readValue[T any](t testing.TB, g *reco.Graph, n reco.Node[T]) T {
	t.Helper()
	s, err := reco.Read(g, n)
	if err != nil || !s.Valid() {
		t.Fatalf("read %s: valid=%v err=%v", n.ClassName(), s.Valid(), err)
	}
	return s.Value()
}

func change(t testing.TB, g *reco.Graph, fn func(*reco.Tx)) {
	t.Helper()
	if err := g.Update(func(tx *reco.Tx) error { fn(tx); return nil }); err != nil {
		t.Fatal(err)
	}
}

var operatorImplementations = []struct {
	name   string
	union  func(reco.NodeClassName, ...reco.Node[reco.SetSnapshot[int]]) reco.Node[reco.SetSnapshot[int]]
	mapSet func(reco.NodeClassName, reco.Node[reco.SetSnapshot[int]], func(int) int) reco.Node[reco.MapSnapshot[int, int]]
}{
	{"builtin", nodes.Union[int], nodes.MapSet[int, int]},
	{"external", externalUnion[int], externalMapSet[int, int]},
}

// The same conformance tests run against both implementations, including
// collection-size-independent F calls, atomic overlap changes, and replacements.
func TestOperatorConformance(t *testing.T) {
	for _, impl := range operatorImplementations {
		for _, size := range []int{1000, 10000} {
			t.Run(fmt.Sprintf("%s/%d", impl.name, size), func(t *testing.T) {
				a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
				u := impl.union("u", a, b)
				var calls []int
				m := impl.mapSet("m", u, func(k int) int { calls = append(calls, k); return k * 2 })
				g := reco.NewGraph()
				if err := g.Register(m); err != nil {
					t.Fatal(err)
				}
				keys := make([]int, size)
				for i := range keys {
					keys[i] = i
				}
				seed := (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: keys})
				change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, seed); reco.Set(tx, b, seed) })
				if len(calls) != size {
					t.Fatalf("initial F calls=%d want=%d", len(calls), size)
				}
				var events []reco.MapEvent[int, int]
				_, sub, err := reco.SubscribeMap(g, m, reco.SubscribeOptions{}, func(ev reco.MapEvent[int, int]) { events = append(events, ev) })
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				oldSet, oldMap := readValue(t, g, u), readValue(t, g, m)
				calls = nil
				change(t, g, func(tx *reco.Tx) { reco.ApplySetDelta(tx, a, reco.SetDelta[int]{Add: []int{size, size + 1, size}}) })
				slices.Sort(calls)
				if !slices.Equal(calls, []int{size, size + 1}) || len(events) != 1 || len(events[0].Changes) != 2 {
					t.Fatalf("point addition: calls=%v events=%+v", calls, events)
				}
				assertExternalSharing(t, oldSet, readValue(t, g, u), 2)
				assertExternalSharing(t, oldMap, readValue(t, g, m), 2)
				if oldSet.Len() != size || oldMap.Len() != size {
					t.Fatal("published snapshot mutated")
				}
				calls, events = nil, nil
				change(t, g, func(tx *reco.Tx) { reco.SetDelete(tx, a, 0); reco.SetDelete(tx, a, size); reco.SetUpsert(tx, b, size) })
				if len(calls) != 0 || len(events) != 0 {
					t.Fatal("overlap or membership move propagated")
				}
				change(t, g, func(tx *reco.Tx) { reco.SetDelete(tx, b, 0) })
				if len(calls) != 0 || len(events) != 1 || len(events[0].Changes) != 1 || events[0].Changes[0].AfterValid {
					t.Fatal("removal recomputed F or lost deletion")
				}
				calls, events = nil, nil
				// Replacing one input must reconcile all missing contributions.
				change(t, g, func(tx *reco.Tx) {
					reco.Set(tx, a, (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: []int{-1}}))
				})
				if !slices.Equal(calls, []int{-1}) {
					t.Fatalf("replacement F calls=%v", calls)
				}
				out := readValue(t, g, m)
				if _, ok := out.Get(size + 1); ok {
					t.Fatal("replacement left deleted key")
				}
				if _, ok := out.Get(size); !ok {
					t.Fatal("replacement removed other input's member")
				}
				if v, ok := out.Get(-1); !ok || v != -2 {
					t.Fatal("replacement missing new key")
				}
			})
		}
	}
}

// Test-only inspection of the pinned immutable implementation. Operator code
// above uses only exported APIs. Inspect actual branch reuse, not just values.
func assertExternalSharing(t testing.TB, before, after any, delta int) {
	t.Helper()
	inspect := func(snapshot any) map[uintptr]bool {
		root := reflect.ValueOf(snapshot).FieldByName("items").Elem().FieldByName("root")
		out := make(map[uintptr]bool)
		var visit func(reflect.Value)
		visit = func(v reflect.Value) {
			if v.IsNil() {
				return
			}
			ptr := v.Elem()
			out[ptr.Pointer()] = true
			if children := ptr.Elem().FieldByName("nodes"); children.IsValid() {
				for i := range children.Len() {
					visit(children.Index(i))
				}
			}
		}
		visit(root)
		return out
	}
	a, b := inspect(before), inspect(after)
	shared := 0
	for p := range b {
		if a[p] {
			shared++
		}
	}
	if len(b)-shared > 16*delta+16 || shared < len(a)/2 {
		t.Fatalf("delta=%d reused only %d/%d branches; %d new", delta, shared, len(a), len(b)-shared)
	}
	runtime.KeepAlive(before)
	runtime.KeepAlive(after)
}

func TestExternalOperatorsIndependentGraphs(t *testing.T) {
	a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
	u := externalUnion("u", a, b, a)
	m := externalMapSet("m", u, func(k int) int { return k * 2 })
	var wg sync.WaitGroup
	for i := range 4 {
		g := reco.NewGraph()
		if err := g.Register(m); err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, reco.SetSnapshot[int]{}); reco.Set(tx, b, reco.SetSnapshot[int]{}) })
			for j := range 50 {
				change(t, g, func(tx *reco.Tx) { reco.SetUpsert(tx, a, i*100+j) })
			}
			out := readValue(t, g, m)
			if out.Len() != 50 {
				t.Errorf("graph %d length=%d", i, out.Len())
			}
			for k, v := range out.All() {
				if k/100 != i || v != k*2 {
					t.Errorf("graph %d has foreign entry %d:%d", i, k, v)
				}
			}
		})
	}
	wg.Wait()
}

func TestExternalOperatorRandomBatches(t *testing.T) {
	for _, impl := range operatorImplementations {
		t.Run(impl.name, func(t *testing.T) {
			a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
			var called []int
			m := impl.mapSet("m", impl.union("u", a, b), func(k int) int { called = append(called, k); return k * k })
			g := reco.NewGraph()
			if err := g.Register(m); err != nil {
				t.Fatal(err)
			}
			change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, reco.SetSnapshot[int]{}); reco.Set(tx, b, reco.SetSnapshot[int]{}) })
			model := []map[int]bool{{}, {}}
			previous := make(map[int]int)
			rng := rand.New(rand.NewPCG(17, 19))
			for range 200 {
				called = nil
				change(t, g, func(tx *reco.Tx) {
					for range 5 {
						i, k := rng.IntN(2), rng.IntN(64)
						n := []reco.Node[reco.SetSnapshot[int]]{a, b}[i]
						switch rng.IntN(8) {
						case 0:
							reco.ApplySetDelta(tx, n, reco.SetDelta[int]{Clear: true, Add: []int{k}})
							model[i] = map[int]bool{k: true}
						case 1, 2, 3:
							reco.SetDelete(tx, n, k)
							delete(model[i], k)
						default:
							reco.SetUpsert(tx, n, k)
							model[i][k] = true
						}
					}
				})
				want := make(map[int]int)
				for _, members := range model {
					for k := range members {
						want[k] = k * k
					}
				}
				got := maps.Collect(readValue(t, g, m).All())
				if !maps.Equal(got, want) {
					t.Fatalf("got %v want %v", got, want)
				}
				var added []int
				for k := range want {
					if _, ok := previous[k]; !ok {
						added = append(added, k)
					}
				}
				slices.Sort(added)
				slices.Sort(called)
				if !slices.Equal(added, called) {
					t.Fatalf("F called for %v, newly added keys=%v", called, added)
				}
				previous = want
			}
		})
	}
}

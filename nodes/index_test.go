package nodes_test

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func TestIndexCountByAndMap(t *testing.T) {
	type value struct{ Group, Other int }
	in := reco.MapData[int, value]("input")
	mapCalls, indexCalls, countCalls := 0, 0, 0
	mapped := nodes.Map("mapped", in, func(_ int, v value) int { mapCalls++; return v.Group })
	index := nodes.Index("index", in, func(_ int, v value) int { indexCalls++; return v.Group })
	counts := nodes.CountBy("counts", in, func(_ int, v value) int { countCalls++; return v.Group })
	distinct := nodes.Distinct("distinct", counts)
	g := reco.NewGraph()
	if err := g.Register(mapped, index, distinct); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, in, reco.MapSnapshot[int, value]{}) })
	events := 0
	_, err := reco.Subscribe(g, distinct, reco.SubscribeOptions{}, func(reco.Event[reco.SetSnapshot[int]]) { events++ })
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name                         string
		edit                         func(*reco.Tx)
		mapCalls, groupCalls, events int
		want                         map[int]int64
	}{
		{"add-duplicates", func(tx *reco.Tx) { reco.MapPut(tx, in, 1, value{0, 1}); reco.MapPut(tx, in, 2, value{0, 2}) }, 2, 2, 1, map[int]int64{0: 2}},
		{"same-group", func(tx *reco.Tx) { reco.MapPut(tx, in, 1, value{0, 3}) }, 1, 2, 0, map[int]int64{0: 2}},
		{"one-remaining", func(tx *reco.Tx) { reco.MapDelete(tx, in, 1) }, 0, 1, 0, map[int]int64{0: 1}},
		{"transfer", func(tx *reco.Tx) { reco.MapDelete(tx, in, 2); reco.MapPut(tx, in, 3, value{0, 4}) }, 1, 2, 0, map[int]int64{0: 1}},
		{"move-group", func(tx *reco.Tx) { reco.MapPut(tx, in, 3, value{5, 4}) }, 1, 2, 1, map[int]int64{5: 1}},
		{"last-removal", func(tx *reco.Tx) { reco.MapDelete(tx, in, 3) }, 0, 1, 1, map[int]int64{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			oldMap, oldIndex, oldCounts := mustRead(t, g, mapped), mustRead(t, g, index), mustRead(t, g, counts)
			mapCalls, indexCalls, countCalls, events = 0, 0, 0, 0
			update(t, g, tt.edit)
			if tt.name == "same-group" {
				if !mustRead(t, g, mapped).RecoValueEqual(oldMap) || !mustRead(t, g, index).RecoValueEqual(oldIndex) {
					t.Fatal("irrelevant value edit changed projection/index roots")
				}
			}
			if tt.name == "same-group" || tt.name == "transfer" {
				if !mustRead(t, g, counts).RecoValueEqual(oldCounts) {
					t.Fatal("unchanged counts changed root")
				}
			}
			if mapCalls != tt.mapCalls || indexCalls != tt.groupCalls || countCalls != tt.groupCalls || events != tt.events {
				t.Fatalf("calls map/index/count/events=%d/%d/%d/%d", mapCalls, indexCalls, countCalls, events)
			}
			if got := maps.Collect(mustRead(t, g, counts).All()); !maps.Equal(got, tt.want) {
				t.Fatalf("counts=%v want %v", got, tt.want)
			}
			ix := mustRead(t, g, index)
			if ix.Len() != len(tt.want) {
				t.Fatal("empty index bucket retained")
			}
			for group, members := range ix.All() {
				if int64(members.Len()) != tt.want[group] {
					t.Fatal("index/count mismatch")
				}
				for k := range members.All() {
					if v, ok := mustRead(t, g, mapped).Get(k); !ok || v != group {
						t.Fatal("map/index mismatch")
					}
				}
			}
		})
	}
}

func TestGroupCountsAndInvertSets(t *testing.T) {
	members := reco.MapData[int, reco.SetSnapshot[string]]("members")
	groups := reco.MapData[int, string]("groups")
	counted := nodes.GroupCounts("counted", members, groups)
	inverse := nodes.InvertSets("inverse", members)
	// Reuse the definitions in separate runtimes to test graph-local caches.
	for _, demand := range []bool{false, true} {
		t.Run(fmt.Sprintf("demand=%v", demand), func(t *testing.T) {
			g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: demand})
			if err := g.Register(counted, inverse); err != nil {
				t.Fatal(err)
			}
			update(t, g, func(tx *reco.Tx) {
				reco.Set(tx, members, reco.MapSnapshot[int, reco.SetSnapshot[string]]{})
				reco.Set(tx, groups, reco.MapSnapshot[int, string]{})
			})
			sub, err := reco.Subscribe(g, counted, reco.SubscribeOptions{}, func(reco.Event[reco.MapSnapshot[string, reco.MultisetSnapshot[string]]]) {})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			invSub, err := reco.Subscribe(g, inverse, reco.SubscribeOptions{}, func(reco.Event[reco.MapSnapshot[string, reco.SetSnapshot[int]]]) {})
			if err != nil {
				t.Fatal(err)
			}
			defer invSub.Unsubscribe()
			set := func(v ...string) reco.SetSnapshot[string] {
				return (reco.SetSnapshot[string]{}).WithDelta(reco.SetDelta[string]{Add: v})
			}
			current := map[int]reco.SetSnapshot[string]{}
			grouping := map[int]string{}
			check := func() {
				t.Helper()
				want := map[string]map[string]int64{}
				wantInv := map[string]map[int]bool{}
				for k, values := range current {
					for v := range values.All() {
						if wantInv[v] == nil {
							wantInv[v] = map[int]bool{}
						}
						wantInv[v][k] = true
						if group, ok := grouping[k]; ok {
							if want[group] == nil {
								want[group] = map[string]int64{}
							}
							want[group][v]++
						}
					}
				}
				got, gotInv := mustRead(t, g, counted), mustRead(t, g, inverse)
				if got.Len() != len(want) || gotInv.Len() != len(wantInv) {
					t.Fatal("wrong number of nonempty groups")
				}
				for group, bag := range got.All() {
					if !maps.Equal(maps.Collect(bag.All()), want[group]) {
						t.Fatalf("%q: %v, want %v", group, maps.Collect(bag.All()), want[group])
					}
				}
				for v, keys := range gotInv.All() {
					if keys.Len() != len(wantInv[v]) {
						t.Fatal("wrong inverted bucket size")
					}
					for k := range keys.All() {
						if !wantInv[v][k] {
							t.Fatal("incorrect reverse edge")
						}
					}
				}
			}
			publish := func() {
				update(t, g, func(tx *reco.Tx) {
					var md reco.MapDelta[int, reco.SetSnapshot[string]]
					var gd reco.MapDelta[int, string]
					for k, v := range current {
						md.Put = append(md.Put, reco.MapEntry[int, reco.SetSnapshot[string]]{Key: k, Value: v})
					}
					for k, v := range grouping {
						gd.Put = append(gd.Put, reco.MapEntry[int, string]{Key: k, Value: v})
					}
					// Arbitrary replacements deliberately lack the previous roots.
					reco.Set(tx, members, (reco.MapSnapshot[int, reco.SetSnapshot[string]]{}).WithDelta(md))
					reco.Set(tx, groups, (reco.MapSnapshot[int, string]{}).WithDelta(gd))
				})
				check()
			}
			current[1], current[2], current[3] = set("x", "y"), set("x"), set("ignored")
			grouping[1], grouping[2] = "", "" // Empty string is a real group; 3 is ungrouped.
			publish()
			current[1] = set("y")
			publish() // x remains contributed by 2.
			grouping[2] = "other"
			current[2] = set("y", "z")
			publish() // Move+edit atomically.
			rng := rand.New(rand.NewPCG(14, 73))
			for i := range 200 {
				k := rng.IntN(12)
				if rng.IntN(3) == 0 {
					delete(current, k)
				} else {
					current[k] = set(fmt.Sprint(rng.IntN(4)), fmt.Sprint(rng.IntN(4)))
				}
				if rng.IntN(3) == 0 {
					delete(grouping, k)
				} else {
					grouping[k] = fmt.Sprint(rng.IntN(3))
				}
				if i%13 == 0 {
					publish()
					continue
				}
				update(t, g, func(tx *reco.Tx) {
					if v, ok := current[k]; ok {
						reco.MapPut(tx, members, k, v)
					} else {
						reco.MapDelete(tx, members, k)
					}
					if v, ok := grouping[k]; ok {
						reco.MapPut(tx, groups, k, v)
					} else {
						reco.MapDelete(tx, groups, k)
					}
				})
				check()
			}
			if err := sub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
			if err := invSub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
			check() // Demand-driven caches rebuild correctly after last unsubscribe.
		})
	}
}

func TestSumMultisetsAtomicAndOverflow(t *testing.T) {
	a, b := reco.MultisetData[string]("a"), reco.MultisetData[string]("b")
	sum := nodes.SumMultisets("sum", a, b)
	repeated := nodes.SumMultisets("repeated", a, a)
	distinct := nodes.Distinct("distinct", sum)
	status := reco.Operator("status", []reco.Dependency{sum}, func() reco.Compute[error] {
		return func(e reco.Eval) reco.Result[error] { return reco.OK(reco.Input(e, sum).Err()) }
	})
	distinctStatus := reco.Operator("distinct-status", []reco.Dependency{distinct}, func() reco.Compute[error] {
		return func(e reco.Eval) reco.Result[error] { return reco.OK(reco.Input(e, distinct).Err()) }
	})
	g := reco.NewGraph()
	if err := g.Register(status, repeated, distinctStatus); err != nil {
		t.Fatal(err)
	}
	apply := func(tx *reco.Tx, n reco.Node[reco.MultisetSnapshot[string]], count int64) {
		if err := reco.ApplyMultisetDelta(tx, n, reco.MultisetDelta[string]{Put: []reco.MultisetEntry[string]{{Key: "x", Count: count}}}); err != nil {
			t.Fatal(err)
		}
	}
	update(t, g, func(tx *reco.Tx) { apply(tx, a, 2); apply(tx, b, 3) })
	if mustRead(t, g, sum).Count("x") != 5 || mustRead(t, g, repeated).Count("x") != 4 {
		t.Fatal("not additive")
	}
	events := 0
	_, err := reco.Subscribe(g, sum, reco.SubscribeOptions{}, func(reco.Event[reco.MultisetSnapshot[string]]) { events++ })
	if err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { apply(tx, a, 4); apply(tx, b, 1) })
	if events != 0 {
		t.Fatal("transfer emitted a change")
	}
	update(t, g, func(tx *reco.Tx) { apply(tx, a, math.MaxInt64); apply(tx, b, math.MaxInt64) })
	if !errors.Is(mustRead(t, g, status), reco.ErrMultisetCount) {
		t.Fatal("overflow was not reported")
	}
	if !errors.Is(mustRead(t, g, distinctStatus), reco.ErrMultisetCount) {
		t.Fatal("input error swallowed by Distinct")
	}
	update(t, g, func(tx *reco.Tx) { apply(tx, a, 1); apply(tx, b, 2) })
	if mustRead(t, g, status) != nil || mustRead(t, g, sum).Count("x") != 3 {
		t.Fatal("did not recover after overflow")
	}
	if !slices.Equal(slices.Collect(mustRead(t, g, distinct).All()), []string{"x"}) {
		t.Fatal("distinct failed to recover")
	}
}

func TestGroupedTransferSuppressesChanges(t *testing.T) {
	in := reco.MapData[int, reco.SetSnapshot[int]]("sets")
	groups := reco.MapData[int, int]("groups")
	out := nodes.GroupCounts("counted", in, groups)
	g := reco.NewGraph()
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	s := (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: []int{42}})
	update(t, g, func(tx *reco.Tx) {
		reco.MapPut(tx, in, 1, s)
		reco.MapPut(tx, groups, 1, 0)
		reco.MapPut(tx, groups, 2, 0)
	})
	before := mustRead(t, g, out)
	update(t, g, func(tx *reco.Tx) { reco.MapDelete(tx, in, 1); reco.MapPut(tx, in, 2, s) })
	after := mustRead(t, g, out)
	if !before.RecoValueEqual(after) || !reflect.DeepEqual(slices.Collect(after.ChangesSince(before)), []reco.MapChange[int, reco.MultisetSnapshot[int]](nil)) {
		t.Fatal("transfer changed counted bucket")
	}
}

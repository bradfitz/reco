package nodes_test

import (
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"recontrol/reco"
	"recontrol/reco/nodes"
)

var setOperations = []struct {
	name  string
	build func(reco.NodeClassName, ...reco.Node[reco.SetSnapshot[int]]) reco.Node[reco.SetSnapshot[int]]
	want  func([]bool) bool
}{
	{"Union", nodes.Union[int], func(in []bool) bool { return slices.Contains(in, true) }},
	{"Intersection", nodes.Intersection[int], func(in []bool) bool { return !slices.Contains(in, false) }},
	{"Xor", nodes.Xor[int], func(in []bool) bool {
		odd := false
		for _, v := range in {
			odd = odd != v
		}
		return odd
	}},
	{"Difference", func(className reco.NodeClassName, in ...reco.Node[reco.SetSnapshot[int]]) reco.Node[reco.SetSnapshot[int]] {
		return nodes.Difference(className, in[0], in[1:]...)
	}, func(in []bool) bool { return in[0] && !slices.Contains(in[1:], true) }},
}

func assertMembers[K comparable](t *testing.T, got reco.SetSnapshot[K], want map[K]bool) {
	t.Helper()
	if got.Len() != len(want) {
		t.Fatalf("members=%v want=%v", slices.Collect(got.All()), want)
	}
	for k := range want {
		if !got.Contains(k) {
			t.Fatalf("missing member %v", k)
		}
	}
}

func assertSetEvents[K comparable](t *testing.T, before, after reco.SetSnapshot[K], events []reco.Event[reco.SetSnapshot[K]]) {
	t.Helper()
	want := make(map[K]bool)
	for k := range before.All() {
		if !after.Contains(k) {
			want[k] = false
		}
	}
	for k := range after.All() {
		if !before.Contains(k) {
			want[k] = true
		}
	}
	if len(want) == 0 {
		if len(events) != 0 || !before.RecoValueEqual(after) {
			t.Fatal("unchanged membership notified or replaced its root")
		}
		return
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want one atomic event", len(events))
	}
	ev := events[0]
	if !ev.Previous.Value().RecoValueEqual(before) || !ev.Current.Value().RecoValueEqual(after) || ev.Version != after.Version() {
		t.Fatal("event snapshots or version are incorrect")
	}
	got := make(map[K]bool)
	// Verify direct delta metadata, not just the reconciled final value.
	for _, c := range after.Changes() {
		if _, duplicate := got[c.Key]; duplicate {
			t.Fatalf("duplicate change for %v", c.Key)
		}
		got[c.Key] = c.Present
	}
	if !maps.Equal(got, want) {
		t.Fatalf("delta=%v want=%v", got, want)
	}
}

func TestSetOperationsRandomBatches(t *testing.T) {
	for _, op := range setOperations {
		for _, repeated := range []bool{false, true} {
			name := op.name
			if repeated {
				name += "/repeated-input"
			}
			t.Run(name, func(t *testing.T) {
				a, b, c := reco.SetData[int]("a"), reco.SetData[int]("b"), reco.SetData[int]("c")
				leaves := []reco.Node[reco.SetSnapshot[int]]{a, b, c}
				indices := []int{0, 1, 2}
				if repeated {
					indices = []int{0, 1, 0}
				}
				inputs := []reco.Node[reco.SetSnapshot[int]]{leaves[indices[0]], leaves[indices[1]], leaves[indices[2]]}
				out := op.build("out", inputs...)
				inputs[0] = c // Constructors must copy the caller's slice.
				g := reco.NewGraph()
				if err := g.Register(out, c); err != nil {
					t.Fatal(err)
				}
				model := []map[int]bool{{}, {}, {}}
				update(t, g, func(tx *reco.Tx) {
					for _, n := range leaves {
						reco.Set(tx, n, reco.SetSnapshot[int]{})
					}
				})
				var events []reco.Event[reco.SetSnapshot[int]]
				sub, err := reco.Subscribe(g, out, reco.SubscribeOptions{}, func(ev reco.Event[reco.SetSnapshot[int]]) { events = append(events, ev) })
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				rng := rand.New(rand.NewPCG(47, 19))
				for step := range 300 {
					before := mustRead(t, g, out)
					oldMembers := slices.Collect(before.All())
					events = nil
					update(t, g, func(tx *reco.Tx) {
						for range 6 {
							i, k := rng.IntN(3), rng.IntN(30)
							switch rng.IntN(12) {
							case 0:
								reco.Set(tx, leaves[i], (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: []int{k}}))
								model[i] = map[int]bool{k: true}
							case 1:
								reco.ApplySetDelta(tx, leaves[i], reco.SetDelta[int]{Clear: true})
								model[i] = map[int]bool{}
							case 2, 3, 4:
								reco.SetDelete(tx, leaves[i], k)
								delete(model[i], k)
							default:
								reco.ApplySetDelta(tx, leaves[i], reco.SetDelta[int]{Remove: []int{k}, Add: []int{k, k}})
								model[i][k] = true
							}
						}
					})
					want := make(map[int]bool)
					for k := range 30 {
						if op.want([]bool{model[indices[0]][k], model[indices[1]][k], model[indices[2]][k]}) {
							want[k] = true
						}
					}
					after := mustRead(t, g, out)
					assertMembers(t, after, want)
					assertSetEvents(t, before, after, events)
					if !slices.Equal(oldMembers, slices.Collect(before.All())) {
						t.Fatalf("step %d mutated an old snapshot", step)
					}
				}
			})
		}
	}
}

func TestSetOperationsSameInputAndGraphIsolation(t *testing.T) {
	for _, op := range setOperations {
		t.Run(op.name, func(t *testing.T) {
			a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
			out := op.build("out", a, b)
			same := op.build("same", a, a)
			var wg sync.WaitGroup
			for graphID := range 4 {
				wg.Go(func() {
					g := reco.NewGraph()
					if err := g.Register(out, same); err != nil {
						t.Error(err)
						return
					}
					update(t, g, func(tx *reco.Tx) {
						reco.Set(tx, a, reco.SetSnapshot[int]{})
						reco.Set(tx, b, reco.SetSnapshot[int]{})
					})
					want := make(map[int]bool)
					for j := range 30 {
						k := graphID*100 + j
						update(t, g, func(tx *reco.Tx) {
							reco.SetUpsert(tx, a, k)
							if j%2 == 0 {
								reco.SetUpsert(tx, b, k)
							}
						})
						if op.want([]bool{true, j%2 == 0}) {
							want[k] = true
						}
					}
					assertMembers(t, mustRead(t, g, out), want)
					if op.name == "Union" || op.name == "Intersection" {
						all := make(map[int]bool)
						for k := range mustRead(t, g, a).All() {
							all[k] = true
						}
						assertMembers(t, mustRead(t, g, same), all)
					} else if mustRead(t, g, same).Len() != 0 {
						t.Error("xor/difference of a set with itself is nonempty")
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestSetOperationArity(t *testing.T) {
	a := reco.SetData[int]("a")
	for _, op := range setOperations {
		t.Run(op.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("accepted fewer than two inputs")
				}
			}()
			op.build("bad", a)
		})
	}
}

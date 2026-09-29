package nodes_test

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func collectionFanIn[T any](tb testing.TB, width int, scoped bool, initial T, build func(reco.NodeClassName, ...reco.Node[T]) reco.Node[T]) (*reco.Graph, []reco.Node[T], reco.Node[T]) {
	tb.Helper()
	inputs := make([]reco.Node[T], width)
	for i := range inputs {
		inputs[i] = reco.Data[T](reco.NodeClassName(fmt.Sprint("input-", i)))
	}
	out := build("out", inputs...)
	if scoped {
		scope := new(reco.Scope)
		out = reco.In(scope, out)
		for i := range inputs {
			inputs[i] = reco.In(scope, inputs[i])
		}
	}
	g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true})
	if err := g.Register(out); err != nil {
		tb.Fatal(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		for _, n := range inputs {
			reco.Set(tx, n, initial)
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	sub, err := reco.Subscribe(g, out, reco.SubscribeOptions{}, func(reco.Event[T]) {})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { sub.Unsubscribe() })
	return g, inputs, out
}

func checkInputWork(t *testing.T, before, after reco.GraphStats, checks, reads uint64) {
	t.Helper()
	if got := after.InputChecks - before.InputChecks; got != checks {
		t.Fatalf("readiness checks = %d, want %d", got, checks)
	}
	if got := after.InputReads - before.InputReads; got != reads {
		t.Fatalf("input reads = %d, want %d", got, reads)
	}
}

func TestSetOperationsFanIn(t *testing.T) {
	for _, op := range setOperations {
		for _, width := range []int{8, 8192} {
			for _, scoped := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/inputs=%d/scoped=%t", op.name, width, scoped), func(t *testing.T) {
					initial := (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: []int{0}})
					g, inputs, out := collectionFanIn(t, width, scoped, initial, op.build)
					present := make([]bool, width)
					for i := range present {
						present[i] = true
					}
					for _, indices := range [][]int{{0}, {width - 1}, {width / 2}, {0, width - 1}, {width / 2}} {
						before := g.Stats()
						update(t, g, func(tx *reco.Tx) {
							for _, i := range indices {
								if present[i] {
									reco.SetDelete(tx, inputs[i], 0)
								} else {
									reco.SetUpsert(tx, inputs[i], 0)
								}
								present[i] = !present[i]
							}
						})
						checkInputWork(t, before, g.Stats(), uint64(len(indices)), uint64(len(indices)))
						got := mustRead(t, g, out)
						if got.Contains(0) != op.want(present) || got.Len() > 1 {
							t.Fatalf("wrong result after changing %v", indices)
						}
					}
				})
			}
		}
	}
}

func TestSumMultisetsFanIn(t *testing.T) {
	for _, width := range []int{8, 8192} {
		for _, scoped := range []bool{false, true} {
			t.Run(fmt.Sprintf("inputs=%d/scoped=%t", width, scoped), func(t *testing.T) {
				// Repetition must multiply contributions without multiplying reads.
				build := func(name reco.NodeClassName, inputs ...reco.Node[reco.MultisetSnapshot[int]]) reco.Node[reco.MultisetSnapshot[int]] {
					var repeated []reco.Node[reco.MultisetSnapshot[int]]
					for _, n := range inputs {
						repeated = append(repeated, n, n, n)
					}
					return nodes.SumMultisets(name, repeated...)
				}
				g, inputs, out := collectionFanIn(t, width, scoped, reco.MultisetSnapshot[int]{}, build)
				want := int64(0)
				for _, indices := range [][]int{{0}, {width - 1}, {width / 2}, {0, width - 1}} {
					before := g.Stats()
					update(t, g, func(tx *reco.Tx) {
						for _, i := range indices {
							if err := reco.ApplyMultisetDelta(tx, inputs[i], reco.MultisetDelta[int]{Adjust: map[int]int64{0: 1}}); err != nil {
								t.Fatal(err)
							}
							want += 3
						}
					})
					checkInputWork(t, before, g.Stats(), uint64(len(indices)), uint64(len(indices)))
					if got := mustRead(t, g, out).Count(0); got != want {
						t.Fatalf("count = %d, want %d", got, want)
					}
				}
			})
		}
	}
}

func TestCollectionScopedAliasesAndReactivation(t *testing.T) {
	for _, op := range setOperations {
		t.Run(op.name, func(t *testing.T) {
			scope := new(reco.Scope)
			a := reco.SetData[int]("a")
			sa := reco.In(scope, a)
			// Two declared handles resolve to the same instance; the first also
			// appears twice. Difference uses it as both first and subtractor.
			out := reco.In(scope, op.build("out", a, sa, a))
			g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true})
			if err := g.Register(out); err != nil {
				t.Fatal(err)
			}
			update(t, g, func(tx *reco.Tx) { reco.Set(tx, sa, reco.SetSnapshot[int]{}) })
			for k := range 3 {
				sub, err := reco.Subscribe(g, out, reco.SubscribeOptions{}, func(reco.Event[reco.SetSnapshot[int]]) {})
				if err != nil {
					t.Fatal(err)
				}
				before := g.Stats()
				update(t, g, func(tx *reco.Tx) { reco.SetUpsert(tx, sa, k) })
				checkInputWork(t, before, g.Stats(), 1, 2)
				want := make(map[int]bool)
				if op.want([]bool{true, true, true}) {
					for j := range k + 1 {
						want[j] = true
					}
				}
				assertMembers(t, mustRead(t, g, out), want)
				sub.Unsubscribe()
				if g.Stats().CachedFunctions != 0 {
					t.Fatal("cache survived last unsubscribe")
				}
				assertMembers(t, mustRead(t, g, out), want) // Cold rebuild.
			}
		})
	}
}

func multisetCounts(t *testing.T, counts ...int64) reco.MultisetSnapshot[int] {
	t.Helper()
	var delta reco.MultisetDelta[int]
	for k, count := range counts {
		delta.Put = append(delta.Put, reco.MultisetEntry[int]{Key: k, Count: count})
	}
	out, err := (reco.MultisetSnapshot[int]{}).WithDelta(delta)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func resultNode[T any](input reco.Node[T]) reco.Node[reco.Result[T]] {
	return reco.Operator("result", []reco.Dependency{input}, func() reco.Compute[reco.Result[T]] {
		return func(e reco.Eval) reco.Result[reco.Result[T]] {
			d := reco.Input(e, input)
			return reco.OK(reco.Result[T]{Value: d.Value(), Err: d.Err(), IsPartial: d.IsPartial()})
		}
	})
}

func TestSumMultisetsPendingRecovery(t *testing.T) {
	for _, initiallyBad := range []bool{false, true} {
		t.Run(fmt.Sprint(initiallyBad), func(t *testing.T) {
			// Values are Results so we can drive input-error transitions without
			// changing their collection values, including two simultaneous errors.
			leaves := make([]reco.Node[reco.Result[reco.MultisetSnapshot[int]]], 4)
			inputs := make([]reco.Node[reco.MultisetSnapshot[int]], len(leaves))
			for i := range leaves {
				n := reco.Data[reco.Result[reco.MultisetSnapshot[int]]](reco.NodeClassName(fmt.Sprint("leaf-", i)))
				leaves[i] = n
				inputs[i] = reco.Operator(reco.NodeClassName(fmt.Sprint("input-", i)), []reco.Dependency{n}, func() reco.Compute[reco.MultisetSnapshot[int]] {
					return func(e reco.Eval) reco.Result[reco.MultisetSnapshot[int]] { return reco.Input(e, n).Value() }
				})
			}
			sum := nodes.SumMultisets("sum", inputs...)
			result := resultNode(sum)
			g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true})
			if err := g.Register(result); err != nil {
				t.Fatal(err)
			}
			values := []int64{1, 2, 3, 4}
			inputErr := errors.New("unavailable input")
			set := func(tx *reco.Tx, i int, count int64, err error) {
				values[i] = count
				reco.Set(tx, leaves[i], reco.Result[reco.MultisetSnapshot[int]]{Value: multisetCounts(t, count), Err: err})
			}
			update(t, g, func(tx *reco.Tx) {
				for i, v := range values {
					var err error
					if initiallyBad && i == 0 {
						err = inputErr
					}
					set(tx, i, v, err)
				}
			})
			sub, err := reco.Subscribe(g, result, reco.SubscribeOptions{}, func(reco.Event[reco.Result[reco.MultisetSnapshot[int]]]) {})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			check := func(wantErr error) {
				t.Helper()
				got := mustRead(t, g, result)
				if !errors.Is(got.Err, wantErr) {
					t.Fatalf("result error = %v, want %v", got.Err, wantErr)
				}
				if wantErr == nil {
					want := values[0] + values[1] + values[2] + values[3]
					if got.Value.Count(0) != want {
						t.Fatalf("sum = %d, want %d", got.Value.Count(0), want)
					}
				}
			}
			update(t, g, func(tx *reco.Tx) { set(tx, 0, 1, inputErr); set(tx, 1, 2, inputErr); set(tx, 2, 7, nil) })
			check(inputErr)
			update(t, g, func(tx *reco.Tx) { set(tx, 3, 8, nil) })
			check(inputErr) // Neither failing input changed this time.
			update(t, g, func(tx *reco.Tx) { set(tx, 0, 1, nil) })
			check(inputErr) // One unchanged error still blocks success.
			update(t, g, func(tx *reco.Tx) { set(tx, 1, 2, nil) })
			check(nil) // Must include the changes from earlier failed evaluations.
			update(t, g, func(tx *reco.Tx) { set(tx, 0, math.MaxInt64, nil) })
			check(reco.ErrMultisetCount)
			update(t, g, func(tx *reco.Tx) { set(tx, 2, 9, nil) })
			check(reco.ErrMultisetCount)
			update(t, g, func(tx *reco.Tx) { set(tx, 0, 1, nil) })
			check(nil) // Input 2 is no longer in ChangedInputs, but is still pending.
			before := g.Stats()
			update(t, g, func(tx *reco.Tx) { set(tx, 3, 10, nil) })
			checkInputWork(t, before, g.Stats(), 3, 3) // Input, sum, result; no stale pending inputs.
			check(nil)
			sub.Unsubscribe()
			check(nil) // Demand eviction forgets all caches, including pending work.
		})
	}
}

func TestSumMultisetsRepeatedOverflow(t *testing.T) {
	a, b := reco.MultisetData[int]("a"), reco.MultisetData[int]("b")
	args := []reco.Node[reco.MultisetSnapshot[int]]{a, a, a, b}
	sum := nodes.SumMultisets("sum", args...)
	result := resultNode(sum)
	args[0] = b // Declaration must not retain the caller's mutable slice.
	g := reco.NewGraph()
	if err := g.Register(result); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, a, multisetCounts(t, 1)); reco.Set(tx, b, multisetCounts(t, 2)) })
	if got := mustRead(t, g, sum).Count(0); got != 5 {
		t.Fatalf("sum = %d, want 5", got)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, a, multisetCounts(t, math.MaxInt64)) })
	got := mustRead(t, g, result)
	if !errors.Is(got.Err, reco.ErrMultisetCount) {
		t.Fatalf("multiplication overflow: %v", got.Err)
	}
	// Recover by changing only a; b's unchanged contribution must survive.
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, a, multisetCounts(t, 2)) })
	if got := mustRead(t, g, sum).Count(0); got != 8 {
		t.Fatalf("recovered sum = %d, want 8", got)
	}
	// A large atomic transfer must not depend on ChangedInputs iteration order.
	q := int64(math.MaxInt64 / 3)
	update(t, g, func(tx *reco.Tx) {
		reco.Set(tx, a, multisetCounts(t, q))
		reco.Set(tx, b, multisetCounts(t, math.MaxInt64-3*q))
	})
	for i := range 20 {
		update(t, g, func(tx *reco.Tx) {
			if i%2 == 0 {
				reco.Set(tx, a, multisetCounts(t, 0))
				reco.Set(tx, b, multisetCounts(t, math.MaxInt64))
			} else {
				reco.Set(tx, a, multisetCounts(t, q))
				reco.Set(tx, b, multisetCounts(t, math.MaxInt64-3*q))
			}
		})
		if got := mustRead(t, g, sum).Count(0); got != math.MaxInt64 {
			t.Fatalf("transfer sum = %d", got)
		}
	}
}

func TestSumMultisetsScopedAliasesAndReactivation(t *testing.T) {
	scope := new(reco.Scope)
	a := reco.MultisetData[int]("a")
	sa := reco.In(scope, a)
	out := reco.In(scope, nodes.SumMultisets("out", a, sa, a))
	g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true})
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, sa, multisetCounts(t, 1)) })
	for count := int64(2); count < 5; count++ {
		sub, err := reco.Subscribe(g, out, reco.SubscribeOptions{}, func(reco.Event[reco.MultisetSnapshot[int]]) {})
		if err != nil {
			t.Fatal(err)
		}
		before := g.Stats()
		update(t, g, func(tx *reco.Tx) { reco.Set(tx, sa, multisetCounts(t, count)) })
		checkInputWork(t, before, g.Stats(), 1, 2)
		if got := mustRead(t, g, out).Count(0); got != 3*count {
			t.Fatalf("sum = %d, want %d", got, 3*count)
		}
		sub.Unsubscribe()
		if g.Stats().CachedFunctions != 0 {
			t.Fatal("cache survived last unsubscribe")
		}
		if got := mustRead(t, g, out).Count(0); got != 3*count {
			t.Fatal("wrong cold sum")
		}
	}
}

func TestSumMultisetsRandomBatches(t *testing.T) {
	a, b, c := reco.MultisetData[int]("a"), reco.MultisetData[int]("b"), reco.MultisetData[int]("c")
	leaves := []reco.Node[reco.MultisetSnapshot[int]]{a, b, c}
	sum := nodes.SumMultisets("sum", a, a, b, c, c, c)
	result := resultNode(sum)
	g := reco.NewGraph()
	if err := g.Register(result); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) {
		for _, n := range leaves {
			reco.Set(tx, n, reco.MultisetSnapshot[int]{})
		}
	})
	model := [3][12]int64{}
	var events []reco.Event[reco.MultisetSnapshot[int]]
	sub, err := reco.Subscribe(g, sum, reco.SubscribeOptions{}, func(e reco.Event[reco.MultisetSnapshot[int]]) { events = append(events, e) })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	rng := rand.New(rand.NewPCG(19, 83))
	for step := range 300 {
		before := mustRead(t, g, sum)
		oldCounts := [12]int64{}
		for k := range oldCounts {
			oldCounts[k] = before.Count(k)
		}
		events = nil
		update(t, g, func(tx *reco.Tx) {
			for range 4 {
				i, k, count := rng.IntN(3), rng.IntN(12), rng.Int64N(10)
				d := reco.MultisetDelta[int]{Put: []reco.MultisetEntry[int]{{Key: k, Count: count}}}
				switch rng.IntN(10) {
				case 0:
					model[i] = [12]int64{}
					v, err := (reco.MultisetSnapshot[int]{}).WithDelta(d)
					if err != nil {
						t.Fatal(err)
					}
					reco.Set(tx, leaves[i], v) // Replacement with unrelated lineage.
				case 1:
					model[i] = [12]int64{}
					d.Clear = true
				}
				if err := reco.ApplyMultisetDelta(tx, leaves[i], d); err != nil {
					t.Fatal(err)
				}
				model[i][k] = count
			}
		})
		after := mustRead(t, g, result)
		if after.Err != nil {
			t.Fatal(after.Err)
		}
		wantLen, changed := 0, 0
		for k := range oldCounts {
			want := 2*model[0][k] + model[1][k] + 3*model[2][k]
			if got := after.Value.Count(k); got != want {
				t.Fatalf("step %d, key %d: %d, want %d", step, k, got, want)
			}
			if before.Count(k) != oldCounts[k] {
				t.Fatal("old snapshot mutated")
			}
			if want != 0 {
				wantLen++
			}
			if want != oldCounts[k] {
				changed++
			}
		}
		if after.Value.Len() != wantLen {
			t.Fatal("unexpected keys in sum")
		}
		if changed == 0 {
			if len(events) != 0 || !before.RecoValueEqual(after.Value) {
				t.Fatal("unchanged sum notified or changed root")
			}
		} else if len(events) != 1 || len(after.Value.Changes()) != changed {
			t.Fatalf("step %d: events=%d, delta=%d; want one atomic %d-key event", step, len(events), len(after.Value.Changes()), changed)
		}
	}
}

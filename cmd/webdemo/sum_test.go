package main

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"recontrol/reco"
)

func TestSumMapDeltaWork(t *testing.T) {
	for _, size := range []int{1000, 100000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			source := reco.MapData[int, int]("source")
			var called []int
			out := sumMap("sum", source, func(v int) int { called = append(called, v); return v })
			g := reco.NewGraph()
			if err := g.Register(out); err != nil {
				t.Fatal(err)
			}
			change := func(fn func(*reco.Tx)) {
				t.Helper()
				if err := g.Update(func(tx *reco.Tx) error { fn(tx); return nil }); err != nil {
					t.Fatal(err)
				}
			}
			var seed reco.MapDelta[int, int]
			for k := range size {
				seed.Put = append(seed.Put, reco.MapEntry[int, int]{Key: k, Value: 1})
			}
			change(func(tx *reco.Tx) { reco.Set(tx, source, (reco.MapSnapshot[int, int]{}).WithDelta(seed)) })
			if len(called) != size {
				t.Fatalf("initial work=%d, want %d", len(called), size)
			}
			events := 0
			sub, err := reco.Subscribe(g, out, reco.SubscribeOptions{}, func(reco.Event[int]) { events++ })
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			for _, test := range []struct {
				name        string
				mutate      func(*reco.Tx)
				calls       []int
				sum, events int
			}{
				{"add", func(tx *reco.Tx) { reco.MapPut(tx, source, size, 2) }, []int{2}, size + 2, 1},
				{"update-value", func(tx *reco.Tx) { reco.MapPut(tx, source, 0, 3) }, []int{1, 3}, size + 4, 1},
				{"remove", func(tx *reco.Tx) { reco.MapDelete(tx, source, size) }, []int{2}, size + 2, 1},
				{"same-total", func(tx *reco.Tx) { reco.MapPut(tx, source, 0, 2); reco.MapPut(tx, source, 1, 2) }, []int{1, 2, 2, 3}, size + 2, 0},
				{"net-zero", func(tx *reco.Tx) { reco.MapPut(tx, source, 1, 9); reco.MapPut(tx, source, 1, 2) }, nil, size + 2, 0},
			} {
				t.Run(test.name, func(t *testing.T) {
					called, events = nil, 0
					change(test.mutate)
					slices.Sort(called)
					result, err := reco.Read(g, out)
					if err != nil || !result.Valid() || result.Value() != test.sum || events != test.events || !slices.Equal(called, test.calls) {
						t.Fatalf("sum=%d, calls=%v events=%d err=%v; want %d,%v,%d", result.Value(), called, events, err, test.sum, test.calls, test.events)
					}
				})
			}
			// Arbitrary replacements are reconciled; then normal deltas resume.
			change(func(tx *reco.Tx) {
				reco.Set(tx, source, (reco.MapSnapshot[int, int]{}).WithDelta(reco.MapDelta[int, int]{Put: []reco.MapEntry[int, int]{{Key: -1, Value: 7}}}))
			})
			if result, _ := reco.Read(g, out); result.Value() != 7 {
				t.Fatal("replacement was not reconciled")
			}
			change(func(tx *reco.Tx) { reco.Set(tx, source, reco.MapSnapshot[int, int]{}) })
			if result, _ := reco.Read(g, out); result.Value() != 0 {
				t.Fatal("clear left a nonzero sum")
			}
		})
	}
}

func TestSumMapGraphIsolation(t *testing.T) {
	source := reco.MapData[int, int]("source")
	out := sumMap("sum", source, func(v int) int { return v })
	var wg sync.WaitGroup
	for graphID := range 4 {
		wg.Go(func() {
			g := reco.NewGraph()
			if err := g.Register(out); err != nil {
				t.Error(err)
				return
			}
			for k := range 20 {
				if err := g.Update(func(tx *reco.Tx) error { reco.MapPut(tx, source, k, graphID+1); return nil }); err != nil {
					t.Error(err)
					return
				}
			}
			result, err := reco.Read(g, out)
			if err != nil || result.Value() != 20*(graphID+1) {
				t.Errorf("graph %d sum=%d err=%v", graphID, result.Value(), err)
			}
		})
	}
	wg.Wait()
}

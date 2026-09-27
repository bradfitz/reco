package reco

import (
	"fmt"
	"testing"
)

// Vary collection size and delta size independently. Set/Map/Union/MapSet
// should scale with touched keys and HAMT path length, not collection length.
func BenchCollectionDelta(b *testing.B, operators OperatorTestFuncs) {
	for _, kind := range []string{"SetData", "MapData", "Union", "MapSet"} {
		for _, size := range []int{1000, 100000} {
			for _, delta := range []int{1, 16} {
				b.Run(fmt.Sprintf("%s/N=%d/delta=%d", kind, size, delta), func(b *testing.B) {
					g := NewGraph()
					a, other := SetData[int]("a"), SetData[int]("b")
					mp := MapData[int, int]("map")
					var node any = a
					calls := 0
					switch kind {
					case "MapData":
						node = mp
					case "Union":
						node = operators.Union("union", a, other)
					case "MapSet":
						node = operators.MapSet("mapped", operators.Union("union", a, other), func(k int) int { calls++; return k * 2 })
					}
					if err := g.Register(node); err != nil {
						b.Fatal(err)
					}
					if err := g.Update(func(tx *Tx) error {
						if kind == "MapData" {
							Set(tx, mp, makeMap(size, newComparableHasher[int]()))
						} else {
							Set(tx, a, makeSet(size, newComparableHasher[int]()))
							if kind != "SetData" {
								Set(tx, other, SetSnapshot[int]{})
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					calls = 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := g.Update(func(tx *Tx) error {
							for k := range delta {
								if kind == "MapData" {
									MapPut(tx, mp, k, -i-1)
								} else if i%2 == 0 {
									SetUpsert(tx, a, size+k)
								} else {
									SetDelete(tx, a, size+k)
								}
							}
							return nil
						}); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if kind == "MapSet" {
						if want := ((b.N + 1) / 2) * delta; calls != want {
							b.Fatalf("F calls=%d, want %d", calls, want)
						}
						b.ReportMetric(float64(calls)/float64(b.N), "F-calls/op")
					}
				})
			}
		}
	}
}

// Unrelated nodes and subscribers must not add per-transaction scans or copies.
func BenchmarkSparseGraphDelta(b *testing.B) {
	for _, size := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			g := NewGraph()
			source := Data[int]("source")
			calls := 0
			out := Func("out", Deps(struct{ Source Node[int] }{source}), func(_ Eval, in struct{ Source int }) Result[int] { calls++; return OK(in.Source * 2) })
			nodes := []any{out}
			for i := range size {
				nodes = append(nodes, Data[int](NodeClassName(fmt.Sprintf("unrelated-%d", i))))
			}
			if err := g.Register(nodes...); err != nil {
				b.Fatal(err)
			}
			for _, n := range nodes[1:] {
				if _, err := Subscribe(g, n.(Node[int]), SubscribeOptions{}, func(Event[int]) { b.Fatal("unrelated notification") }); err != nil {
					b.Fatal(err)
				}
			}
			if err := g.Update(func(tx *Tx) error { Set(tx, source, 0); return nil }); err != nil {
				b.Fatal(err)
			}
			calls = 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := g.Update(func(tx *Tx) error { Set(tx, source, i+1); return nil }); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if calls != b.N {
				b.Fatalf("computations=%d, want %d", calls, b.N)
			}
		})
	}
}

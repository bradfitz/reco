package nodes_test

import (
	"fmt"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func BenchmarkIndexPointDelta(b *testing.B) {
	for _, kind := range []string{"Map", "Index", "CountBy"} {
		for _, size := range []int{1000, 100000} {
			b.Run(fmt.Sprintf("%s/N=%d", kind, size), func(b *testing.B) {
				in := reco.MapData[int, int]("in")
				var out reco.Dependency
				switch kind {
				case "Map":
					out = nodes.Map("out", in, func(_ int, v int) int { return 2 * v })
				case "Index":
					out = nodes.Index("out", in, func(_ int, v int) int { return v })
				case "CountBy":
					out = nodes.CountBy("out", in, func(_ int, v int) int { return v })
				}
				g := reco.NewGraph()
				if err := g.Register(out); err != nil {
					b.Fatal(err)
				}
				var d reco.MapDelta[int, int]
				for i := range size {
					d.Put = append(d.Put, reco.MapEntry[int, int]{Key: i, Value: i % 4})
				}
				if err := g.Update(func(tx *reco.Tx) error { reco.Set(tx, in, (reco.MapSnapshot[int, int]{}).WithDelta(d)); return nil }); err != nil {
					b.Fatal(err)
				}
				v := 0
				b.ReportAllocs()
				for b.Loop() {
					v = 1 - v
					if err := g.Update(func(tx *reco.Tx) error { reco.MapPut(tx, in, 0, v); return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkGroupedPointDelta(b *testing.B) {
	for _, kind := range []string{"InvertSets", "GroupCounts"} {
		for _, size := range []int{1000, 100000} {
			b.Run(fmt.Sprintf("%s/N=%d", kind, size), func(b *testing.B) {
				in := reco.MapData[int, reco.SetSnapshot[int]]("in")
				groups := reco.MapData[int, int]("groups")
				var out reco.Dependency = nodes.InvertSets("out", in)
				if kind == "GroupCounts" {
					out = nodes.GroupCounts("out", in, groups)
				}
				g := reco.NewGraph()
				if err := g.Register(out, groups); err != nil {
					b.Fatal(err)
				}
				var md reco.MapDelta[int, reco.SetSnapshot[int]]
				var gd reco.MapDelta[int, int]
				for i := range size {
					md.Put = append(md.Put, reco.MapEntry[int, reco.SetSnapshot[int]]{Key: i, Value: (reco.SetSnapshot[int]{}).WithDelta(reco.SetDelta[int]{Add: []int{i % 4}})})
					gd.Put = append(gd.Put, reco.MapEntry[int, int]{Key: i, Value: i % 2})
				}
				if err := g.Update(func(tx *reco.Tx) error {
					reco.Set(tx, in, (reco.MapSnapshot[int, reco.SetSnapshot[int]]{}).WithDelta(md))
					reco.Set(tx, groups, (reco.MapSnapshot[int, int]{}).WithDelta(gd))
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				set := md.Put[0].Value
				present := false
				b.ReportAllocs()
				for b.Loop() {
					if present {
						set = set.WithDelta(reco.SetDelta[int]{Remove: []int{9}})
					} else {
						set = set.WithDelta(reco.SetDelta[int]{Add: []int{9}})
					}
					present = !present
					if err := g.Update(func(tx *reco.Tx) error { reco.MapPut(tx, in, 0, set); return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkCountedProjection(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			a, c := reco.MultisetData[int]("a"), reco.MultisetData[int]("b")
			out := nodes.Distinct("distinct", nodes.SumMultisets("sum", a, c))
			g := reco.NewGraph()
			if err := g.Register(out); err != nil {
				b.Fatal(err)
			}
			var d reco.MultisetDelta[int]
			for i := range size {
				d.Put = append(d.Put, reco.MultisetEntry[int]{Key: i, Count: 1})
			}
			if err := g.Update(func(tx *reco.Tx) error {
				if err := reco.ApplyMultisetDelta(tx, a, d); err != nil {
					return err
				}
				reco.Set(tx, c, reco.MultisetSnapshot[int]{})
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			d = reco.MultisetDelta[int]{Adjust: map[int]int64{0: -1}}
			b.ReportAllocs()
			for b.Loop() {
				if err := g.Update(func(tx *reco.Tx) error { return reco.ApplyMultisetDelta(tx, a, d) }); err != nil {
					b.Fatal(err)
				}
				d.Adjust[0] = -d.Adjust[0]
			}
		})
	}
}

package nodes_test

import (
	"fmt"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

// Hold collection and delta sizes fixed while increasing only the input count.
// Initialization is outside the timer; a warmed point edit reads one input.
func BenchmarkCollectionFanIn(b *testing.B) {
	for _, op := range setOperations {
		for _, width := range []int{8, 128, 8192} {
			b.Run(fmt.Sprintf("%s/inputs=%d", op.name, width), func(b *testing.B) {
				var initial reco.SetSnapshot[int]
				present := op.name == "Intersection"
				if present {
					initial = initial.WithDelta(reco.SetDelta[int]{Add: []int{0}})
				}
				g, inputs, _ := collectionFanIn(b, width, false, initial, op.build)
				before := g.Stats()
				b.ReportAllocs()
				for b.Loop() {
					if err := g.Update(func(tx *reco.Tx) error {
						if present {
							reco.SetDelete(tx, inputs[0], 0)
						} else {
							reco.SetUpsert(tx, inputs[0], 0)
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					present = !present
				}
				reportInputWork(b, before, g.Stats())
			})
		}
	}
	for _, width := range []int{8, 128, 8192} {
		b.Run(fmt.Sprintf("SumMultisets/inputs=%d", width), func(b *testing.B) {
			g, inputs, _ := collectionFanIn(b, width, false, reco.MultisetSnapshot[int]{}, nodes.SumMultisets[int])
			delta := reco.MultisetDelta[int]{Adjust: map[int]int64{0: 1}}
			before := g.Stats()
			b.ReportAllocs()
			for b.Loop() {
				if err := g.Update(func(tx *reco.Tx) error { return reco.ApplyMultisetDelta(tx, inputs[0], delta) }); err != nil {
					b.Fatal(err)
				}
				delta.Adjust[0] = -delta.Adjust[0]
			}
			reportInputWork(b, before, g.Stats())
		})
	}
}

func reportInputWork(b *testing.B, before, after reco.GraphStats) {
	b.ReportMetric(float64(after.InputChecks-before.InputChecks)/float64(b.N), "input-checks/op")
	b.ReportMetric(float64(after.InputReads-before.InputReads)/float64(b.N), "input-reads/op")
}

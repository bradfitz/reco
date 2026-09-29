package reco

import (
	"fmt"
	"testing"
)

// Read one known input while declaring many: isolate runtime/readiness/scope
// overhead from an application's own loops over all of its dependencies.
func BenchmarkWideInput(b *testing.B) {
	for _, scoped := range []bool{false, true} {
		for _, n := range []int{1, 32, 1024, 8192} {
			b.Run(fmt.Sprintf("scoped=%t/inputs=%d", scoped, n), func(b *testing.B) {
				g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
				inputs := make([]Node[int], n)
				deps := make([]Dependency, n)
				for i := range inputs {
					inputs[i] = Data[int](NodeClassName(fmt.Sprint("input-", i)))
					deps[i] = inputs[i]
				}
				first := inputs[0]
				out := Operator("out", deps, func() Compute[int] {
					return func(e Eval) Result[int] { return OK(Input(e, first).Value()) }
				})
				if scoped {
					s := new(Scope)
					out = In(s, out)
					for i := range inputs {
						inputs[i] = In(s, inputs[i])
					}
				}
				if err := g.Register(out); err != nil {
					b.Fatal(err)
				}
				if err := g.Update(func(tx *Tx) error {
					for _, input := range inputs {
						Set(tx, input, 0)
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				sub, err := Subscribe(g, out, SubscribeOptions{}, func(Event[int]) {})
				if err != nil {
					b.Fatal(err)
				}
				defer sub.Unsubscribe()
				value := 0
				before := g.Stats()
				b.ReportAllocs()
				for b.Loop() {
					value++
					if err := g.Update(func(tx *Tx) error { Set(tx, inputs[0], value); return nil }); err != nil {
						b.Fatal(err)
					}
				}
				after := g.Stats()
				b.ReportMetric(float64(after.InputChecks-before.InputChecks)/float64(b.N), "input-checks/op")
				b.ReportMetric(float64(after.InputReads-before.InputReads)/float64(b.N), "input-reads/op")
			})
		}
	}
}

package reco_test

import (
	"fmt"

	"github.com/bradfitz/reco"
)

func ExampleIn() {
	input := reco.Data[int]("input")
	doubled := reco.Func("doubled", reco.Deps(struct{ N reco.Node[int] }{input}),
		func(_ reco.Eval, in struct{ N int }) reco.Result[int] {
			return reco.OK(2 * in.N)
		})
	var left, right reco.Scope
	a, b := reco.In(&left, doubled), reco.In(&right, doubled)
	g := reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true})
	if err := g.Register(a, b); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.Set(tx, reco.In(&left, input), 3)
		reco.Set(tx, reco.In(&right, input), 7)
		return nil
	}); err != nil {
		panic(err)
	}
	// A one-shot read computes current state without keeping it active.
	av, err := reco.Read(g, a)
	if err != nil {
		panic(err)
	}
	bv, err := reco.Read(g, b)
	if err != nil {
		panic(err)
	}
	fmt.Println(av.Value(), bv.Value())
	fmt.Println("active functions:", g.Stats().ActiveFunctions)
	// Output:
	// 6 14
	// active functions: 0
}

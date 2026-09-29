package reco_test

import (
	"fmt"

	"github.com/bradfitz/reco"
)

func ExampleEval_ChangedInputs() {
	a, b, c := reco.Data[int]("a"), reco.Data[int]("b"), reco.Data[int]("c")
	sum := reco.Operator("sum", []reco.Dependency{a, b, c}, func() reco.Compute[int] {
		previous := make(map[reco.Node[int]]int)
		total := 0
		return func(e reco.Eval) reco.Result[int] {
			// All three inputs on first evaluation; afterwards only the inputs
			// that changed. Duplicate declarations would be reported once.
			for input := range e.ChangedInputs() {
				n := input.(reco.Node[int]) // This operator declares only int inputs.
				current := reco.Input(e, n).Value()
				total += current - previous[n]
				previous[n] = current
			}
			return reco.OK(total)
		}
	})
	g := reco.NewGraph()
	if err := g.Register(sum); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.Set(tx, a, 10)
		reco.Set(tx, b, 20)
		reco.Set(tx, c, 30)
		return nil
	}); err != nil {
		panic(err)
	}
	value, _ := reco.Read(g, sum)
	fmt.Println(value.Value())
	if err := g.Update(func(tx *reco.Tx) error { reco.Set(tx, b, 25); return nil }); err != nil {
		panic(err)
	}
	value, _ = reco.Read(g, sum)
	fmt.Println(value.Value())
	// Output:
	// 60
	// 65
}

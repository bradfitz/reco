package reco_test

import (
	"fmt"
	"strings"

	"github.com/bradfitz/reco"
)

func ExampleOperator() {
	words := reco.SetData[string]("words")
	// This is an external-package implementation of a key-preserving map.
	uppercase := reco.Operator("uppercase", []reco.Dependency{words}, func() reco.Compute[reco.MapSnapshot[string, string]] {
		// These caches belong to one graph, not to the reusable definition.
		var previous reco.SetSnapshot[string]
		var result reco.MapSnapshot[string, string]
		return func(eval reco.Eval) reco.Result[reco.MapSnapshot[string, string]] {
			current := reco.Input(eval, words).Value()
			var delta reco.MapDelta[string, string]
			for c := range current.ChangesSince(previous) {
				if c.Present {
					// Only newly added words are converted.
					delta.Put = append(delta.Put, reco.MapEntry[string, string]{Key: c.Key, Value: strings.ToUpper(c.Key)})
				} else {
					delta.Remove = append(delta.Remove, c.Key)
				}
			}
			previous = current
			result = result.WithDelta(delta) // One atomic, structurally shared batch.
			return reco.OK(result)
		}
	})
	g := reco.NewGraph()
	if err := g.Register(uppercase); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, words, reco.SetDelta[string]{Add: []string{"oak", "elm"}})
		return nil
	}); err != nil {
		panic(err)
	}
	first, err := reco.Read(g, uppercase)
	if err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, words, reco.SetDelta[string]{Remove: []string{"elm"}, Add: []string{"pine"}})
		return nil
	}); err != nil {
		panic(err)
	}
	current, err := reco.Read(g, uppercase)
	if err != nil {
		panic(err)
	}
	fmt.Println(current.Value().Get("oak"))
	fmt.Println(current.Value().Get("pine"))
	fmt.Println(first.Value().Get("elm")) // Previously published values stay intact.
	// Output:
	// OAK true
	// PINE true
	// ELM true
}

func ExampleMapSnapshot_WithDelta() {
	var names reco.MapSnapshot[int, string] // An empty map, no graph required.
	names = names.WithDelta(reco.MapDelta[int, string]{
		Put: []reco.MapEntry[int, string]{{Key: 1, Value: "old"}},
	})
	replacement := names.WithDelta(reco.MapDelta[int, string]{
		Clear: true,
		Put:   []reco.MapEntry[int, string]{{Key: 2, Value: "new"}},
	})
	fmt.Println(names.Get(1))
	fmt.Println(replacement.Get(2))
	// Output:
	// old true
	// new true
}

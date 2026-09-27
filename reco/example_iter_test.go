package reco_test

import (
	"fmt"
	"slices"

	"recontrol/reco"
)

func ExampleSetSnapshot_All() {
	words := reco.SetData[string]("words")
	g := reco.NewGraph()
	if err := g.Register(words); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, words, reco.SetDelta[string]{Add: []string{"cedar", "birch"}})
		return nil
	}); err != nil {
		panic(err)
	}
	snapshot, err := reco.Read(g, words)
	if err != nil {
		panic(err)
	}
	var names []string
	for word := range snapshot.Value().All() {
		names = append(names, word)
	}
	slices.Sort(names) // Iteration order is unspecified.
	fmt.Println(names)
	// Output: [birch cedar]
}

func ExampleMapSnapshot_All() {
	lengths := reco.MapData[string, int]("lengths")
	g := reco.NewGraph()
	if err := g.Register(lengths); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.MapPut(tx, lengths, "cedar", 5)
		reco.MapPut(tx, lengths, "oak", 3)
		return nil
	}); err != nil {
		panic(err)
	}
	snapshot, err := reco.Read(g, lengths)
	if err != nil {
		panic(err)
	}
	var entries []string
	for word, length := range snapshot.Value().All() {
		entries = append(entries, fmt.Sprintf("%s:%d", word, length))
	}
	slices.Sort(entries) // Iteration order is unspecified.
	fmt.Println(entries)
	// Output: [cedar:5 oak:3]
}

package reco_test

import (
	"fmt"

	"github.com/bradfitz/reco"
)

func ExampleMultisetSnapshot() {
	var counts reco.MultisetSnapshot[string]
	counts, err := counts.WithDelta(reco.MultisetDelta[string]{
		Put: []reco.MultisetEntry[string]{{Key: "web", Count: 2}},
	})
	if err != nil {
		panic(err)
	}
	counts, err = counts.WithDelta(reco.MultisetDelta[string]{Adjust: map[string]int64{"web": -1}})
	if err != nil {
		panic(err)
	}
	fmt.Println(counts.Len(), counts.Count("web"))
	// Decrementing below zero fails atomically; the previous value is returned.
	unchanged, err := counts.WithDelta(reco.MultisetDelta[string]{Adjust: map[string]int64{"web": -2}})
	fmt.Println(err != nil, unchanged.Count("web"))
	// Output:
	// 1 1
	// true 1
}

package nodes_test

import (
	"fmt"
	"slices"
	"strings"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func Example() {
	a, b := reco.SetData[string]("a"), reco.SetData[string]("b")
	words := nodes.Union("words", a, b)
	uppercase := nodes.MapSet("uppercase", words, strings.ToUpper)
	g := reco.NewGraph()
	if err := g.Register(uppercase); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, a, reco.SetDelta[string]{Add: []string{"oak", "elm"}})
		reco.ApplySetDelta(tx, b, reco.SetDelta[string]{Add: []string{"elm", "pine"}})
		return nil
	}); err != nil {
		panic(err)
	}
	snapshot, err := reco.Read(g, uppercase)
	if err != nil {
		panic(err)
	}
	var entries []string
	for word, upper := range snapshot.Value().All() {
		entries = append(entries, word+":"+upper)
	}
	slices.Sort(entries) // Snapshot iteration order is unspecified.
	fmt.Println(entries)
	// Output: [elm:ELM oak:OAK pine:PINE]
}

func Example_setOperations() {
	a, b := reco.SetData[string]("a"), reco.SetData[string]("b")
	common := nodes.Intersection("common", a, b)
	exclusive := nodes.Xor("exclusive", a, b)
	onlyA := nodes.Difference("only-a", a, b)
	g := reco.NewGraph()
	if err := g.Register(common, exclusive, onlyA); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, a, reco.SetDelta[string]{Add: []string{"oak", "elm"}})
		reco.ApplySetDelta(tx, b, reco.SetDelta[string]{Add: []string{"elm", "pine"}})
		return nil
	}); err != nil {
		panic(err)
	}
	for _, n := range []reco.Node[reco.SetSnapshot[string]]{common, exclusive, onlyA} {
		snapshot, err := reco.Read(g, n)
		if err != nil {
			panic(err)
		}
		fmt.Println(n.ClassName(), slices.Sorted(snapshot.Value().All()))
	}
	// Output:
	// common [elm]
	// exclusive [oak pine]
	// only-a [oak]
}

func ExampleMapValues() {
	colors := reco.MapData[string, string]("colors")
	keys := nodes.MapKeys("keys", colors)
	values := nodes.MapValues("values", colors)
	g := reco.NewGraph()
	if err := g.Register(keys, values); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.MapPut(tx, colors, "first", "red")
		reco.MapPut(tx, colors, "second", "red")
		reco.MapPut(tx, colors, "third", "blue")
		return nil
	}); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.MapDelete(tx, colors, "first") // "red" still has another contributor.
		return nil
	}); err != nil {
		panic(err)
	}
	for _, n := range []reco.Node[reco.SetSnapshot[string]]{keys, values} {
		snapshot, err := reco.Read(g, n)
		if err != nil {
			panic(err)
		}
		fmt.Println(n.ClassName(), slices.Sorted(snapshot.Value().All()))
	}
	// Output:
	// keys [second third]
	// values [blue red]
}

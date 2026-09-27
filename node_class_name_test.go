package reco_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

type namedNode interface {
	ClassName() reco.NodeClassName
}

func TestNodeClassNameConstructors(t *testing.T) {
	a, b := reco.SetData[int]("a"), reco.SetData[int]("b")
	m := reco.MapData[int, int]("m")
	factory := func() reco.Compute[int] {
		return func(reco.Eval) reco.Result[int] { return reco.OK(1) }
	}
	constructors := []struct {
		name string
		new  func(reco.NodeClassName) namedNode
	}{
		{"Data", func(n reco.NodeClassName) namedNode { return reco.Data[int](n) }},
		{"SetData", func(n reco.NodeClassName) namedNode { return reco.SetData[int](n) }},
		{"MapData", func(n reco.NodeClassName) namedNode { return reco.MapData[int, int](n) }},
		{"StructData", func(n reco.NodeClassName) namedNode { return reco.StructData[struct{ Value int }](n) }},
		{"Struct", func(n reco.NodeClassName) namedNode { return nodes.Struct[struct{}](n, struct{}{}) }},
		{"Func", func(n reco.NodeClassName) namedNode {
			return reco.Func(n, struct{}{}, func(reco.Eval, struct{}) reco.Result[int] { return reco.OK(1) })
		}},
		{"Operator", func(n reco.NodeClassName) namedNode { return reco.Operator(n, nil, factory) }},
		{"Union", func(n reco.NodeClassName) namedNode { return nodes.Union(n, a, b) }},
		{"Intersection", func(n reco.NodeClassName) namedNode { return nodes.Intersection(n, a, b) }},
		{"Xor", func(n reco.NodeClassName) namedNode { return nodes.Xor(n, a, b) }},
		{"Difference", func(n reco.NodeClassName) namedNode { return nodes.Difference(n, a, b) }},
		{"MapSet", func(n reco.NodeClassName) namedNode { return nodes.MapSet(n, a, func(k int) int { return k }) }},
		{"MapKeys", func(n reco.NodeClassName) namedNode { return nodes.MapKeys(n, m) }},
		{"MapValues", func(n reco.NodeClassName) namedNode { return nodes.MapValues(n, m) }},
	}
	for _, ctor := range constructors {
		t.Run(ctor.name, func(t *testing.T) {
			const name reco.NodeClassName = " example/Words:v1 "
			if got := ctor.new(name).ClassName(); got != name {
				t.Fatalf("ClassName() = %q, want %q", got, name)
			}
			defer func() {
				if got := recover(); got == nil || !strings.Contains(fmt.Sprint(got), "empty node class name") {
					t.Errorf("panic = %v, want empty node class name", got)
				}
			}()
			ctor.new("")
		})
	}
	var zero reco.Node[int]
	if name := zero.ClassName(); name != "" {
		t.Fatalf("zero node ClassName() = %q", name)
	}
}

func TestNodeClassNameExactStrings(t *testing.T) {
	g := reco.NewGraph()
	for _, raw := range []string{"Words", "words", " words ", " ", "pkg/words:v1", "é", "e\u0301"} {
		n := reco.Data[int](reco.NodeClassName(raw))
		if string(n.ClassName()) != raw {
			t.Fatalf("name %q changed to %q", raw, n.ClassName())
		}
		if err := g.Register(n); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodeClassNameScope(t *testing.T) {
	leaf := reco.Data[int]("value")
	derived := reco.Operator("derived", []reco.Dependency{leaf}, func() reco.Compute[int] {
		return func(eval reco.Eval) reco.Result[int] { return reco.OK(reco.Input(eval, leaf).Value()) }
	})
	for _, other := range []struct {
		name string
		node any
	}{
		{"same-type", reco.Data[int]("value")},
		{"different-type", reco.Data[string]("value")},
		{"different-constructor", reco.SetData[int]("value")},
	} {
		t.Run(other.name, func(t *testing.T) {
			g := reco.NewGraph()
			if err := g.Register(leaf); err != nil {
				t.Fatal(err)
			}
			if err := g.Register(other.node); err == nil || !strings.Contains(err.Error(), `duplicate node class name "value"`) {
				t.Fatalf("duplicate registration: %v", err)
			}
		})
	}
	t.Run("recursive-dependency", func(t *testing.T) {
		g := reco.NewGraph()
		if err := g.Register(reco.Data[int]("value")); err != nil {
			t.Fatal(err)
		}
		if err := g.Register(derived); err == nil || !strings.Contains(err.Error(), `duplicate node class name "value"`) {
			t.Fatalf("duplicate dependency registration: %v", err)
		}
	})

	// Repeated handles and reused definitions are valid; values remain graph-local.
	g1, g2 := reco.NewGraph(), reco.NewGraph()
	for i, g := range []*reco.Graph{g1, g2} {
		for range 2 {
			if err := g.Register(derived, leaf, leaf); err != nil {
				t.Fatal(err)
			}
		}
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, leaf, i+1) })
	}
	if readValue(t, g1, derived) != 1 || readValue(t, g2, derived) != 2 {
		t.Fatal("reused definition did not retain graph-local values")
	}
	if err := reco.NewGraph().Register(reco.Data[string]("value")); err != nil {
		t.Fatalf("independent definition reusing name: %v", err)
	}
}

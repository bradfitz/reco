package reco_test

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"

	"recontrol/reco"
)

func TestOperatorFactoryAndReadiness(t *testing.T) {
	a, b, unrelated := reco.Data[int]("a"), reco.Data[int]("b"), reco.Data[int]("unrelated")
	deps := []reco.Dependency{a, b}
	factories, computes := 0, 0
	out := reco.Operator("sum", deps, func() reco.Compute[int] {
		factories++
		return func(eval reco.Eval) reco.Result[int] {
			computes++
			return reco.OK(reco.Input(eval, a).Value() + reco.Input(eval, b).Value())
		}
	})
	deps[0] = unrelated // The declaration must own its dependency list.
	for graphIndex := range 2 {
		g := reco.NewGraph()
		if err := g.Register(out, unrelated); err != nil {
			t.Fatal(err)
		}
		if factories != graphIndex {
			t.Fatal("factory ran before the first update")
		}
		change(t, g, func(*reco.Tx) {})
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, 10) })
		if s, err := reco.Read(g, out); err != nil || s.Valid() || computes != graphIndex*2 {
			t.Fatal("operator ran before all inputs were initialized")
		}
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, b, 20) })
		if got := readValue(t, g, out); got != 30 {
			t.Fatalf("sum=%d", got)
		}
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, 11); reco.Set(tx, b, 21) })
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, a, 11); reco.Set(tx, unrelated, 99) })
		if err := g.Register(out); err != nil {
			t.Fatal(err)
		}
		change(t, g, func(*reco.Tx) {})
		if factories != graphIndex+1 || computes != (graphIndex+1)*2 || readValue(t, g, out) != 32 {
			t.Fatalf("factories=%d computes=%d", factories, computes)
		}
	}
}

func TestOperatorWithoutInputs(t *testing.T) {
	factories, computes := 0, 0
	constant := reco.Operator("constant", nil, func() reco.Compute[int] {
		factories++
		return func(reco.Eval) reco.Result[int] {
			computes++
			return reco.OK(42)
		}
	})
	other := reco.Data[int]("other")
	g := reco.NewGraph()
	if err := g.Register(constant, other); err != nil {
		t.Fatal(err)
	}
	change(t, g, func(*reco.Tx) {})
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, other, 1) })
	if factories != 1 || computes != 1 || readValue(t, g, constant) != 42 {
		t.Fatalf("factories=%d computes=%d", factories, computes)
	}
}

func TestOperatorInputMetadata(t *testing.T) {
	number, label, nullable := reco.Data[int]("number"), reco.Data[string]("label"), reco.Data[any]("nullable")
	problem := errors.New("incomplete")
	partial := reco.Operator("partial", []reco.Dependency{number}, func() reco.Compute[int] {
		return func(eval reco.Eval) reco.Result[int] { return reco.Partial(reco.Input(eval, number).Value(), problem) }
	})
	var observed reco.Dep[int]
	out := reco.Operator("out", []reco.Dependency{partial, label, nullable}, func() reco.Compute[string] {
		return func(eval reco.Eval) reco.Result[string] {
			observed = reco.Input(eval, partial)
			v := reco.Input(eval, nullable)
			if !v.Valid() || v.Value() != nil || v.Version() == 0 {
				t.Fatal("nil interface input was not initialized")
			}
			return reco.OK(fmt.Sprintf("%s:%d", reco.Input(eval, label).Value(), observed.Value()))
		}
	})
	// Ordinary Func computations can also read their declared inputs via Input.
	inline := reco.Func("inline", reco.Deps(struct{ Value reco.Node[any] }{nullable}), func(eval reco.Eval, in struct{ Value any }) reco.Result[any] {
		if in.Value != nil || reco.Input(eval, nullable).Value() != nil {
			t.Fatal("nil input to Func changed")
		}
		return reco.OK[any](nil)
	})
	g := reco.NewGraph()
	if err := g.Register(out, inline); err != nil {
		t.Fatal(err)
	}
	events := 0
	sub, err := reco.Subscribe(g, nullable, reco.SubscribeOptions{}, func(ev reco.Event[any]) {
		events++
		if ev.Previous.Valid() || !ev.Current.Valid() || ev.Current.Value() != nil {
			t.Fatal("nil value notification was lost")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	change(t, g, func(tx *reco.Tx) {
		reco.Set(tx, number, 7)
		reco.Set(tx, label, "value")
		reco.Set[any](tx, nullable, nil)
	})
	if readValue(t, g, out) != "value:7" || !observed.Valid() || !observed.IsPartial() || observed.Err() != problem || observed.Version() == 0 {
		t.Fatalf("Input lost value or result metadata: %+v", observed)
	}
	if events != 1 || readValue(t, g, nullable) != nil || readValue(t, g, inline) != nil {
		t.Fatal("nil interface read failed")
	}
}

func TestOperatorValidation(t *testing.T) {
	expectPanic := func(t *testing.T, want string, fn func()) {
		t.Helper()
		defer func() {
			if got := recover(); got == nil || !strings.Contains(fmt.Sprint(got), want) {
				t.Errorf("panic=%v, want containing %q", got, want)
			}
		}()
		fn()
	}
	n := reco.Data[int]("n")
	factory := func() reco.Compute[int] { return func(reco.Eval) reco.Result[int] { return reco.OK(1) } }
	expectPanic(t, "empty node class name", func() { reco.Operator("", nil, factory) })
	expectPanic(t, "nil operator factory", func() { reco.Operator[int]("bad", nil, nil) })
	expectPanic(t, "expected Node", func() { reco.Operator("bad", []reco.Dependency{nil}, factory) })
	expectPanic(t, "zero node", func() { reco.Operator("bad", []reco.Dependency{reco.Node[int]{}}, factory) })
	expectPanic(t, "zero input", func() { reco.Input(reco.Eval{}, reco.Node[int]{}) })
	expectPanic(t, "not declared", func() { reco.Input(reco.Eval{}, n) })
	for _, test := range []struct {
		name string
		new  func() reco.Compute[int]
		want string
	}{
		{"nil compute", func() reco.Compute[int] { return nil }, "factory returned nil"},
		{"undeclared input", func() reco.Compute[int] {
			return func(eval reco.Eval) reco.Result[int] { return reco.OK(reco.Input(eval, n).Value()) }
		}, "not declared"},
	} {
		t.Run(test.name, func(t *testing.T) {
			g := reco.NewGraph()
			if err := g.Register(reco.Operator("bad", nil, test.new)); err != nil {
				t.Fatal(err)
			}
			expectPanic(t, test.want, func() { change(t, g, func(*reco.Tx) {}) })
		})
	}
}

type customValue struct {
	root    *int
	version reco.Version
}

func (v customValue) RecoValueEqual(other any) bool {
	o, ok := other.(customValue)
	return ok && v.root == o.root
}

func (v customValue) RecoWithVersion(version reco.Version) any {
	v.version = version
	return v
}

var _ reco.ValueEqualer = customValue{}
var _ reco.VersionedValue = customValue{}

func TestCustomValueHooks(t *testing.T) {
	leaf := reco.Data[customValue]("leaf")
	calls := 0
	out := reco.Operator("out", []reco.Dependency{leaf}, func() reco.Compute[customValue] {
		return func(eval reco.Eval) reco.Result[customValue] {
			calls++
			dep := reco.Input(eval, leaf)
			if dep.Value().version != dep.Version() {
				t.Fatal("custom leaf version was not stamped")
			}
			return reco.OK(dep.Value())
		}
	})
	g := reco.NewGraph()
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	root := 1
	original := customValue{root: &root}
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, leaf, original) })
	first, err := reco.Read(g, out)
	if err != nil || !first.Valid() || first.Value().version != first.Version() || first.Version() == 0 || original.version != 0 {
		t.Fatal("custom output versioning modified the receiver or failed")
	}
	oldVersion := first.Version()
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, leaf, original) })
	if calls != 1 || readValue(t, g, out).version != oldVersion {
		t.Fatal("version metadata bypassed custom equality")
	}
	secondRoot := 2
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, leaf, customValue{root: &secondRoot}) })
	if calls != 2 || readValue(t, g, out).version <= oldVersion || first.Value().version != oldVersion || *first.Value().root != 1 {
		t.Fatal("changed root not propagated, or previous value mutated")
	}
}

type invalidVersionValue int

func (invalidVersionValue) RecoWithVersion(reco.Version) any { return "wrong type" }

func TestVersionHookMustPreserveType(t *testing.T) {
	n := reco.Data[invalidVersionValue]("bad")
	g := reco.NewGraph()
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if got := recover(); got == nil || !strings.Contains(fmt.Sprint(got), "changed value type") {
			t.Errorf("panic=%v, want type validation failure", got)
		}
	}()
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, n, invalidVersionValue(1)) })
}

func TestSubscribeMapReconcilesOperatorOutputs(t *testing.T) {
	trigger := reco.Data[int]("trigger")
	out := reco.Operator("map", []reco.Dependency{trigger}, func() reco.Compute[reco.MapSnapshot[string, int]] {
		var previous reco.MapSnapshot[string, int]
		return func(eval reco.Eval) reco.Result[reco.MapSnapshot[string, int]] {
			v := reco.Input(eval, trigger).Value()
			if v == 2 {
				previous = reco.MapSnapshot[string, int]{} // Arbitrary replacement.
			}
			// Two chained batches mean Changes alone describes only the second.
			previous = previous.WithDelta(reco.MapDelta[string, int]{Put: []reco.MapEntry[string, int]{{Key: "a", Value: v}}})
			previous = previous.WithDelta(reco.MapDelta[string, int]{Put: []reco.MapEntry[string, int]{{Key: "b", Value: v}}})
			return reco.OK(previous)
		}
	})
	g := reco.NewGraph()
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	var events []reco.MapEvent[string, int]
	initial, sub, err := reco.SubscribeMap(g, out, reco.SubscribeOptions{}, func(ev reco.MapEvent[string, int]) { events = append(events, ev) })
	if err != nil || initial.Valid() {
		t.Fatalf("initial snapshot valid=%v err=%v", initial.Valid(), err)
	}
	defer sub.Unsubscribe()
	for v := range 3 {
		change(t, g, func(tx *reco.Tx) { reco.Set(tx, trigger, v) })
		if len(events) != v+1 {
			t.Fatalf("missing event: %d", len(events))
		}
		event := events[v]
		got := make(map[string]reco.MapChange[string, int])
		for _, c := range event.Changes {
			got[c.Key] = c
		}
		before := 0
		if v > 0 {
			before = v - 1
		}
		want := map[string]reco.MapChange[string, int]{
			"a": {Key: "a", Before: before, BeforeValid: v > 0, After: v, AfterValid: true},
			"b": {Key: "b", Before: before, BeforeValid: v > 0, After: v, AfterValid: true},
		}
		if len(event.Changes) != 2 || !maps.Equal(got, want) || event.Version != event.Current.Version() {
			t.Fatalf("v=%d event=%+v want changes=%v", v, event, want)
		}
	}
}

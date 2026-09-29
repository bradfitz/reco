package reco

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
)

func inputSet(inputs []Dependency) map[Dependency]bool {
	out := make(map[Dependency]bool)
	for _, n := range inputs {
		out[n] = true
	}
	return out
}

func TestChangedInputs(t *testing.T) {
	for _, demand := range []bool{false, true} {
		t.Run(fmt.Sprint(demand), func(t *testing.T) {
			g := NewGraphWithOptions(GraphOptions{DemandDriven: demand})
			a, b := Data[int]("a"), Data[string]("b")
			var batches [][]Dependency
			out := Operator("out", []Dependency{a, b, a}, func() Compute[int] {
				return func(e Eval) Result[int] {
					got := slices.Collect(e.ChangedInputs())
					if !slices.Equal(got, slices.Collect(e.ChangedInputs())) || len(inputSet(got)) != len(got) {
						t.Fatal("iterator is not reusable/deduplicated")
					}
					count := 0
					for range e.ChangedInputs() {
						count++
						break
					}
					if len(got) != 0 && count != 1 {
						t.Fatal("early termination")
					}
					batches = append(batches, got)
					return OK(Input(e, a).Value() + len(Input(e, b).Value()))
				}
			})
			if err := g.Register(out); err != nil {
				t.Fatal(err)
			}
			sub, err := Subscribe(g, out, SubscribeOptions{}, func(Event[int]) {})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			update(t, g, func(tx *Tx) { Set(tx, a, 1) })
			update(t, g, func(tx *Tx) { Set(tx, a, 2) })
			if len(batches) != 0 {
				t.Fatal("computed before readiness")
			}
			update(t, g, func(tx *Tx) { Set(tx, b, "x") })
			update(t, g, func(tx *Tx) { Set(tx, a, 3); Set(tx, a, 4) })
			update(t, g, func(tx *Tx) { Set(tx, a, 5); Set(tx, a, 4); Set(tx, b, "x") }) // no net change
			abort := errors.New("abort")
			if err := g.Update(func(tx *Tx) error { Set(tx, b, "abort"); return abort }); err != abort {
				t.Fatal(err)
			}
			update(t, g, func(tx *Tx) { Set(tx, a, 6); Set(tx, b, "yy") })
			want := [][]Dependency{{a, b}, {a}, {a, b}}
			if len(batches) != len(want) {
				t.Fatalf("batches=%v", batches)
			}
			for i := range want {
				if !maps.Equal(inputSet(batches[i]), inputSet(want[i])) {
					t.Fatalf("batch %d = %v, want %v", i, batches[i], want[i])
				}
			}
			if mustRead(t, g, out) != 8 {
				t.Fatal("inputs did not expose settled values")
			}
		})
	}
}

func TestChangedInputsScopedAliases(t *testing.T) {
	var scope Scope
	a := Data[int]("a")
	sa := In(&scope, a)
	var batches [][]Dependency
	out := In(&scope, Operator("out", []Dependency{a, sa, a}, func() Compute[int] {
		return func(e Eval) Result[int] {
			batches = append(batches, slices.Collect(e.ChangedInputs()))
			return OK(Input(e, a).Value() + Input(e, sa).Value())
		}
	}))
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, sa, 1) })
	sub, err := Subscribe(g, out, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, sa, 2) })
	sub.Unsubscribe()
	if g.Stats().CachedFunctions != 0 {
		t.Fatal("input indexes outlived demand")
	}
	update(t, g, func(tx *Tx) { Set(tx, sa, 3) })
	if mustRead(t, g, out) != 6 || len(batches) != 3 {
		t.Fatal("cold rebuild")
	}
	for _, batch := range batches {
		if len(batch) != 2 || !maps.Equal(inputSet(batch), inputSet([]Dependency{a, sa})) {
			t.Fatalf("lost declared alias: %v", batch)
		}
	}
}

func TestChangedInputsErrorsAndDiamond(t *testing.T) {
	a := Data[int]("a")
	problem := errors.New("partial")
	parity := Operator("parity", []Dependency{a}, func() Compute[int] {
		return func(e Eval) Result[int] {
			v := Input(e, a).Value()
			if v < 0 {
				return Partial(0, problem)
			}
			return OK(v % 2)
		}
	})
	copy := Operator("copy", []Dependency{a}, copyFactory(a))
	var batches [][]Dependency
	out := Operator("out", []Dependency{parity, copy}, func() Compute[int] {
		return func(e Eval) Result[int] {
			batches = append(batches, slices.Collect(e.ChangedInputs()))
			if v := Input(e, parity); v.Err() != nil {
				return Result[int]{Err: v.Err()}
			}
			return OK(Input(e, copy).Value())
		}
	})
	g := NewGraph()
	if err := g.Register(out); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int{0, 2, -2, -4, 4} {
		update(t, g, func(tx *Tx) { Set(tx, a, v) })
	}
	want := [][]Dependency{{parity, copy}, {copy}, {parity, copy}, {copy}, {parity, copy}}
	if len(batches) != len(want) {
		t.Fatal("diamond evaluated more than once per commit")
	}
	for i := range want {
		if !maps.Equal(inputSet(batches[i]), inputSet(want[i])) {
			t.Fatalf("batch %d = %v, want %v", i, batches[i], want[i])
		}
	}
	if mustRead(t, g, out) != 4 {
		t.Fatal("failed recovery")
	}
}

func TestChangedInputsReconfigure(t *testing.T) {
	a, b := Data[int]("a"), Data[int]("b")
	var got []Dependency
	factory := func(input Node[int]) func() Compute[int] {
		return func() Compute[int] {
			return func(e Eval) Result[int] {
				got = slices.Collect(e.ChangedInputs())
				return OK(Input(e, input).Value())
			}
		}
	}
	scope := new(Scope)
	out := In(scope, Operator("out", []Dependency{a}, factory(a)))
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	if err := g.Register(out, b); err != nil {
		t.Fatal(err)
	}
	// Initialize the original scoped input, then replace it with an explicitly
	// unscoped input. Reconfigure must not reuse the old scope's name mapping.
	update(t, g, func(tx *Tx) { Set(tx, In(scope, a), 1); Set(tx, b, 2) })
	sub, err := Subscribe(g, out, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if !slices.Equal(got, []Dependency{a}) {
		t.Fatal("initial handles")
	}
	if err := g.Update(func(tx *Tx) error { return Reconfigure(tx, out, []Dependency{b, b}, factory(b)) }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []Dependency{b}) || mustRead(t, g, out) != 2 {
		t.Fatal("replacement did not reset input index and changed handles")
	}
}

func TestWideInputWork(t *testing.T) {
	for _, width := range []int{8, 8192} {
		for _, scoped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/scoped=%t", width, scoped), func(t *testing.T) {
				inputs := make([]Node[int], width)
				deps := make([]Dependency, width)
				for i := range inputs {
					inputs[i] = Data[int](NodeClassName(fmt.Sprint("in-", i)))
					deps[i] = inputs[i]
				}
				visits := 0
				out := Operator("sum", deps, func() Compute[int] {
					previous := make(map[Node[int]]int)
					sum := 0
					return func(e Eval) Result[int] {
						for input := range e.ChangedInputs() {
							visits++
							n := input.(Node[int])
							v := Input(e, n).Value()
							sum += v - previous[n]
							previous[n] = v
						}
						return OK(sum)
					}
				})
				if scoped {
					scope := new(Scope)
					out = In(scope, out)
					for i := range inputs {
						inputs[i] = In(scope, inputs[i])
					}
				}
				g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
				if err := g.Register(out); err != nil {
					t.Fatal(err)
				}
				update(t, g, func(tx *Tx) {
					for _, n := range inputs {
						Set(tx, n, 0)
					}
				})
				sub, err := Subscribe(g, out, SubscribeOptions{}, func(Event[int]) {})
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				if visits != width {
					t.Fatal("first evaluation missed inputs")
				}
				before := g.Stats()
				visits = 0
				for i := range 20 {
					update(t, g, func(tx *Tx) { Set(tx, inputs[i%width], i+1) })
				}
				after := g.Stats()
				if visits != 20 || after.InputChecks-before.InputChecks != 20 || after.InputReads-before.InputReads != 20 {
					t.Fatalf("visits=%d checks=%d reads=%d", visits, after.InputChecks-before.InputChecks, after.InputReads-before.InputReads)
				}
			})
		}
	}
}

func TestChangedInputsFuncAndHandleCopy(t *testing.T) {
	a, b := Data[int]("a"), Data[int]("b")
	ptr := a
	var saved Eval
	var got []Dependency
	out := Func("out", Deps(struct{ A *Node[int] }{&ptr}), func(e Eval, in struct{ A int }) Result[int] {
		saved = e
		got = slices.Collect(e.ChangedInputs())
		return OK(in.A + Input(e, a).Value())
	})
	ptr = b // The declaration must not retain a mutable pointer handle.
	g := NewGraph()
	if err := g.Register(out, b); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *Tx) { Set(tx, a, 2); Set(tx, b, 100) })
	if mustRead(t, g, out) != 4 || !slices.Equal(got, []Dependency{a}) {
		t.Fatal("Func changed-input handles do not match its declared inputs")
	}
	for _, e := range []Eval{{}, saved} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("ChangedInputs outside computation did not panic")
				}
			}()
			for range e.ChangedInputs() {
			}
		}()
	}
}

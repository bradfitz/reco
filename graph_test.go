package recontrol

import "testing"

func TestTypedStructDependenciesRecomputeAndSubscribe(t *testing.T) {
	first := Data[int]("first")
	second := Data[int]("second")

	type sumDeps struct {
		First  Dep[int]
		Second Dep[int]
	}
	sum := Func("sum", struct {
		First  Node[int]
		Second Node[int]
	}{First: first, Second: second}, func(ctx Context, in sumDeps) Result[int] {
		return OK(in.First.Value() + in.Second.Value())
	})

	g := NewGraph()
	if err := g.Register(sum); err != nil {
		t.Fatal(err)
	}

	var events []Event[int]
	if _, err := Subscribe(g, sum, SubscribeOptions{FixedPointOnly: true}, func(ev Event[int]) {
		events = append(events, ev)
	}); err != nil {
		t.Fatal(err)
	}

	if err := g.Update(func(tx *Tx) error {
		Set(tx, first, 2)
		Set(tx, second, 3)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if got := events[0].Current.Value(); got != 5 {
		t.Fatalf("sum = %d, want 5", got)
	}
	if events[0].Previous.Valid() {
		t.Fatalf("previous unexpectedly valid")
	}

	if err := g.Update(func(tx *Tx) error {
		Set(tx, first, 10)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if got := events[1].Previous.Value(); got != 5 {
		t.Fatalf("previous = %d, want 5", got)
	}
	if got := events[1].Current.Value(); got != 13 {
		t.Fatalf("current = %d, want 13", got)
	}
}

func TestInlineDependencies(t *testing.T) {
	name := Data[string]("name")
	greeting := Func("greeting",
		Deps(struct {
			Name Node[string]
		}{Name: name}),
		func(ctx Context, in struct {
			Name string
		}) Result[string] {
			return OK("hello " + in.Name)
		},
	)

	g := NewGraph()
	if err := g.Register(greeting); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error {
		Set(tx, name, "tailnet")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	got := g.nodes[greeting.def].value.(string)
	if got != "hello tailnet" {
		t.Fatalf("greeting = %q", got)
	}
}

func TestSetAndMapTransactions(t *testing.T) {
	visible := SetData[string]("visible")
	peers := MapData[string, int]("peers")

	g := NewGraph()
	if err := g.Register(visible, peers); err != nil {
		t.Fatal(err)
	}

	if err := g.Update(func(tx *Tx) error {
		SetUpsert(tx, visible, "a")
		SetUpsert(tx, visible, "b")
		SetDelete(tx, visible, "b")
		MapPut(tx, peers, "a", 1)
		MapPut(tx, peers, "b", 2)
		MapDelete(tx, peers, "b")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	set := g.nodes[visible.def].value.(SetSnapshot[string])
	if !set.Contains("a") || set.Contains("b") {
		t.Fatalf("unexpected set contents")
	}
	mp := g.nodes[peers.def].value.(MapSnapshot[string, int])
	if got, ok := mp.Get("a"); !ok || got != 1 {
		t.Fatalf("map[a] = %d, %v; want 1, true", got, ok)
	}
	if _, ok := mp.Get("b"); ok {
		t.Fatalf("map[b] unexpectedly present")
	}
}

func TestCycleRejected(t *testing.T) {
	a := Data[int]("a")
	type deps struct {
		B Dep[int]
	}
	b := Func("b", struct{ B Node[int] }{B: a}, func(ctx Context, in deps) Result[int] {
		return OK(in.B.Value())
	})
	a.def.kind = nodeFunc
	a.def.deps = []depBinding{{name: "B", node: b.def, typ: typeOf[int]()}}
	a.def.compute = func(Context, map[*nodeDef]nodeValue) nodeValue {
		return nodeValue{value: 1, valid: true}
	}

	g := NewGraph()
	if err := g.Register(a, b); err == nil {
		t.Fatalf("cycle was accepted")
	}
}

func TestReflectionFailureExplodesEarly(t *testing.T) {
	n := Data[int]("n")
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Func did not panic")
		}
	}()
	_ = Func("bad", struct {
		N Node[int]
	}{N: n}, func(ctx Context, in struct {
		N string
	}) Result[string] {
		return OK(in.N)
	})
}

func TestSubscriptionUnsubscribe(t *testing.T) {
	n := Data[int]("n")
	g := NewGraph()
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	count := 0
	sub, err := Subscribe(g, n, SubscribeOptions{}, func(Event[int]) {
		count++
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error {
		Set(tx, n, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error {
		Set(tx, n, 2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("events = %d, want 1", count)
	}
}

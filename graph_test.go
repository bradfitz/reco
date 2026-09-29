package reco

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
	}{First: first, Second: second}, func(eval Eval, in sumDeps) Result[int] {
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
		func(eval Eval, in struct {
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
		Set(tx, name, "graph")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	got := g.nodes[greeting.def].value.(string)
	if got != "hello graph" {
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

func TestMapSubscriptionReportsOnlyTouchedKeys(t *testing.T) {
	items := MapData[int, int]("items")
	g := NewGraph()
	if err := g.Register(items); err != nil {
		t.Fatal(err)
	}
	if err := g.Update(func(tx *Tx) error {
		for i := range 10_000 {
			MapPut(tx, items, i, i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var got MapEvent[int, int]
	initial, sub, err := SubscribeMap(g, items, SubscribeOptions{}, func(ev MapEvent[int, int]) {
		got = ev
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if initial.Value().Len() != 10_000 {
		t.Fatalf("initial size = %d, want 10000", initial.Value().Len())
	}
	if err := g.Update(func(tx *Tx) error {
		MapPut(tx, items, 42, -1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got.Changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(got.Changes))
	}
	change := got.Changes[0]
	if change.Key != 42 || !change.BeforeValid || change.Before != 42 || !change.AfterValid || change.After != -1 {
		t.Fatalf("change = %+v", change)
	}
}

func TestFunctionPreservesMapChanges(t *testing.T) {
	items := MapData[int, int]("items")
	type deps struct {
		Items Dep[MapSnapshot[int, int]]
	}
	view := Func("view", struct {
		Items Node[MapSnapshot[int, int]]
	}{Items: items}, func(_ Eval, in deps) Result[MapSnapshot[int, int]] {
		return OK(in.Items.Value())
	})
	g := NewGraph()
	if err := g.Register(view); err != nil {
		t.Fatal(err)
	}
	var got MapEvent[int, int]
	_, sub, err := SubscribeMap(g, view, SubscribeOptions{}, func(ev MapEvent[int, int]) {
		got = ev
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := g.Update(func(tx *Tx) error {
		MapPut(tx, items, 7, 11)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got.Changes) != 1 || got.Changes[0].Key != 7 || got.Changes[0].After != 11 {
		t.Fatalf("changes = %+v", got.Changes)
	}
	if got.Current.Version() != got.Version {
		t.Fatalf("snapshot version = %d, event version = %d", got.Current.Version(), got.Version)
	}
}

func TestCycleRejected(t *testing.T) {
	a := Data[int]("a")
	type deps struct {
		B Dep[int]
	}
	b := Func("b", struct{ B Node[int] }{B: a}, func(eval Eval, in deps) Result[int] {
		return OK(in.B.Value())
	})
	a.def.kind = nodeFunc
	a.def.deps = []depBinding{{name: "B", node: b.def, typ: typeOf[int]()}}
	a.def.compute = func(Eval) nodeValue {
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
	}{N: n}, func(eval Eval, in struct {
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

func TestRead(t *testing.T) {
	n := Data[int]("n")
	g := NewGraph()
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	if got, err := Read(g, n); err != nil {
		t.Fatal(err)
	} else if got.Valid() {
		t.Fatal("uninitialized node is valid")
	}
	if err := g.Update(func(tx *Tx) error {
		Set(tx, n, 42)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := Read(g, n)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Valid() || got.Value() != 42 {
		t.Fatalf("Read = (%v, %v), want (42, valid)", got.Value(), got.Valid())
	}
}

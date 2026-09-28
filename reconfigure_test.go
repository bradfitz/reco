package reco

import (
	"errors"
	"testing"
)

func copyFactory(n Node[int]) func() Compute[int] {
	return func() Compute[int] { return func(e Eval) Result[int] { return OK(Input(e, n).Value()) } }
}

func TestReconfigureLifetimeAndRollback(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	a, b := Data[int]("a"), Data[int]("b")
	f := Operator("f", []Dependency{a}, copyFactory(a))
	if err := g.Register(f, b); err != nil {
		t.Fatal(err)
	}
	loadsA, loadsB := 0, 0
	fail := false
	BindDurable(g, a, func() (int, error) { loadsA++; return 1, nil })
	BindDurable(g, b, func() (int, error) {
		loadsB++
		if fail {
			return 0, errors.New("load")
		}
		return 2, nil
	})
	var values []int
	sub, err := Subscribe(g, f, SubscribeOptions{}, func(e Event[int]) { values = append(values, e.Current.Value()) })
	if err != nil {
		t.Fatal(err)
	}
	abort := errors.New("abort")
	if err := g.Update(func(tx *Tx) error {
		if err := Reconfigure(tx, f, []Dependency{b}, copyFactory(b)); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if len(values) != 0 || g.Stats().CachedDurableNodes != 1 {
		t.Fatal("rollback changed demand/output")
	}
	fail = true
	if err := g.Update(func(tx *Tx) error { Reconfigure(tx, f, []Dependency{b}, copyFactory(b)); return nil }); err == nil {
		t.Fatal("ignored load error committed")
	}
	fail = false
	if err := g.Update(func(tx *Tx) error { return Reconfigure(tx, f, []Dependency{b, b}, copyFactory(b)) }); err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != 2 || loadsA != 1 || loadsB != 3 || g.Stats().CachedDurableNodes != 1 {
		t.Fatalf("values=%v loads=%d/%d stats=%+v", values, loadsA, loadsB, g.Stats())
	}
	sub.Unsubscribe()
	if g.Stats().CachedDurableNodes != 0 || g.Stats().ActiveFunctions != 0 {
		t.Fatal("retained demand")
	}
	if err := g.Update(func(tx *Tx) error { return Reconfigure(tx, f, []Dependency{a}, copyFactory(a)) }); err != nil {
		t.Fatal(err)
	}
	if loadsA != 1 {
		t.Fatal("cold configure loaded")
	}
	v, err := Read(g, f)
	if err != nil || v.Value() != 1 || loadsA != 2 {
		t.Fatal(v, err)
	}
	// Shared definitions in another runtime are unchanged.
	g2 := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	g2.Register(f, b)
	g2.Update(func(tx *Tx) error { Set(tx, a, 10); Set(tx, b, 20); return nil })
	v, err = Read(g2, f)
	if err != nil || v.Value() != 10 {
		t.Fatal("definition mutated")
	}
	if err := g.Update(func(tx *Tx) error { return Reconfigure(tx, f, []Dependency{f}, copyFactory(f)) }); err == nil {
		t.Fatal("cycle accepted")
	}
}

func TestReconfigureActivatesAnotherStagedBinding(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
		a, b := Data[int]("a"), Data[int]("b")
		low := Operator("low", []Dependency{a}, copyFactory(a))
		padding := Operator("padding", []Dependency{a}, copyFactory(a))
		high := Operator("high", []Dependency{padding}, copyFactory(padding))
		g.Register(high, low, b)
		g.Update(func(tx *Tx) error { Set(tx, a, 1); return nil })
		loads := 0
		BindDurable(g, b, func() (int, error) { loads++; return 7, nil })
		sub, err := Subscribe(g, high, SubscribeOptions{}, func(Event[int]) {})
		if err != nil {
			t.Fatal(err)
		}
		err = g.Update(func(tx *Tx) error {
			lo := func() error { return Reconfigure(tx, low, []Dependency{b}, copyFactory(b)) }
			hi := func() error { return Reconfigure(tx, high, []Dependency{low}, copyFactory(low)) }
			if reverse {
				if err := hi(); err != nil {
					return err
				}
				return lo()
			}
			if err := lo(); err != nil {
				return err
			}
			return hi()
		})
		if err != nil {
			t.Fatal(err)
		}
		v, err := Read(g, high)
		if err != nil || v.Value() != 7 || loads != 1 {
			t.Fatalf("v=%v err=%v loads=%d", v, err, loads)
		}
		sub.Unsubscribe()
		if g.Stats().ActiveFunctions != 0 || g.Stats().CachedDurableNodes != 0 {
			t.Fatal("retained caches")
		}
	}
}

func TestReconfigureDropsStagedDurableCacheEdit(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	a, b := Data[int]("a"), Data[int]("b")
	f := Operator("f", []Dependency{a}, copyFactory(a))
	if err := g.Register(f, b); err != nil {
		t.Fatal(err)
	}
	backing := 1
	if err := BindDurable(g, a, func() (int, error) { return backing, nil }); err != nil {
		t.Fatal(err)
	}
	g.Update(func(tx *Tx) error { Set(tx, b, 2); return nil })
	sub, err := Subscribe(g, f, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	err = g.Update(func(tx *Tx) error {
		if err := Reconfigure(tx, f, []Dependency{b}, copyFactory(b)); err != nil {
			return err
		}
		backing = 3
		UpdateDurable(tx, a, func(int) int { return backing })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := ReadCached(g, a); err != nil || v.Valid() || g.Stats().CachedDurableNodes != 0 {
		t.Fatal("revived evicted cache", v, err, g.Stats())
	}
	if v, err := Read(g, a); err != nil || v.Value() != 3 {
		t.Fatal("lost backing write", v, err)
	}
}

func TestReconfigureRejectsUninitializedActiveInput(t *testing.T) {
	g := NewGraphWithOptions(GraphOptions{DemandDriven: true})
	a, b := Data[int]("a"), Data[int]("b")
	f := Operator("f", []Dependency{a}, copyFactory(a))
	g.Register(f, b)
	g.Update(func(tx *Tx) error { Set(tx, a, 1); return nil })
	sub, err := Subscribe(g, f, SubscribeOptions{}, func(Event[int]) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := g.Update(func(tx *Tx) error { return Reconfigure(tx, f, []Dependency{b}, copyFactory(b)) }); err == nil {
		t.Fatal("accepted uninitialized input")
	}
	err = g.Update(func(tx *Tx) error { Set(tx, b, 2); return Reconfigure(tx, f, []Dependency{b}, copyFactory(b)) })
	if err != nil {
		t.Fatal(err)
	}
	if v, err := Read(g, f); err != nil || v.Value() != 2 {
		t.Fatal(v, err)
	}
}

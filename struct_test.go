package reco_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

type record struct {
	Label    string
	Enabled  bool
	Optional *int
	Any      any
	Peers    reco.MapSnapshot[int, string]
	Members  reco.SetSnapshot[int]
}

var (
	labelField    = reco.Field[record, string]("Label")
	enabledField  = reco.Field[record, bool]("Enabled")
	optionalField = reco.Field[record, *int]("Optional")
	anyField      = reco.Field[record, any]("Any")
	peersField    = reco.Field[record, reco.MapSnapshot[int, string]]("Peers")
	membersField  = reco.Field[record, reco.SetSnapshot[int]]("Members")
)

func peerEdit(key int, value string) reco.StructEdit[record] {
	return peersField.Update(func(m reco.MapSnapshot[int, string]) reco.MapSnapshot[int, string] {
		return m.WithDelta(reco.MapDelta[int, string]{Put: []reco.MapEntry[int, string]{{Key: key, Value: value}}})
	})
}

func memberEdit(key int, present bool) reco.StructEdit[record] {
	return membersField.Update(func(s reco.SetSnapshot[int]) reco.SetSnapshot[int] {
		if present {
			return s.WithDelta(reco.SetDelta[int]{Add: []int{key}})
		}
		return s.WithDelta(reco.SetDelta[int]{Remove: []int{key}})
	})
}

func TestStructSnapshotAndAtomicDelta(t *testing.T) {
	one := 1
	before := reco.NewStruct(record{Label: "old", Enabled: true, Optional: &one, Any: "old"})
	batch := reco.StructDelta[record]{
		labelField.Set("intermediate"), labelField.Set(""), enabledField.Set(false),
		optionalField.Set(nil), anyField.Set(nil), peerEdit(1, "first"), peerEdit(2, "second"),
		peerEdit(1, "final"), memberEdit(1, true), memberEdit(1, false), memberEdit(2, true),
	}
	after := before.WithDelta(batch)
	batch[0] = labelField.Set("mutated edit")
	value := after.Value()
	value.Label = "mutated copy"
	if before.Value().Label != "old" || before.Value().Peers.Len() != 0 || after.Value().Label != "" {
		t.Fatal("snapshot was mutated")
	}
	c := after.ChangesSince(before)
	if got := slices.Collect(c.Fields()); !slices.Equal(got, []string{"Label", "Enabled", "Optional", "Any", "Peers", "Members"}) {
		t.Fatalf("changed fields: %v", got)
	}
	if a, b, changed := labelField.Change(c); !changed || a != "old" || b != "" {
		t.Fatalf("label: %q %q %v", a, b, changed)
	}
	if a, b, changed := enabledField.Change(c); !changed || !a || b {
		t.Fatal("lost explicit false")
	}
	if a, b, changed := optionalField.Change(c); !changed || a != &one || b != nil {
		t.Fatal("lost explicit nil")
	}
	if a, b, changed := anyField.Change(c); !changed || a != "old" || b != nil {
		t.Fatal("lost nil interface")
	}
	if got := peerChanges(c); !maps.Equal(got, map[int]reco.MapChange[int, string]{
		1: {Key: 1, After: "final", AfterValid: true}, 2: {Key: 2, After: "second", AfterValid: true},
	}) {
		t.Fatalf("peer changes: %v", got)
	}
	if got := slices.Collect(reco.SetFieldChanges(membersField, c)); !slices.Equal(got, []reco.SetChange[int]{{Key: 2, Present: true}}) {
		t.Fatalf("members: %v", got)
	}
	// Nested collection snapshots also retain the batch's original base.
	if got := slices.Collect(after.Value().Peers.ChangesSince(before.Value().Peers)); len(got) != 2 {
		t.Fatalf("nested changes: %v", got)
	}
	if after.ChangesSince(after).Len() != 0 || !after.WithDelta(nil).RecoValueEqual(after) {
		t.Fatal("no-op changed record")
	}
	count := 0
	for range c.Fields() {
		count++
		break
	}
	if count != 1 || len(slices.Collect(c.Fields())) != 6 {
		t.Fatal("field iterator did not stop/restart")
	}
	count = 0
	for range reco.MapFieldChanges(peersField, c) {
		count++
		break
	}
	if count != 1 || len(peerChanges(c)) != 2 {
		t.Fatal("map iterator did not stop/restart")
	}
}

func peerChanges(c reco.StructChanges[record]) map[int]reco.MapChange[int, string] {
	m := make(map[int]reco.MapChange[int, string])
	for change := range reco.MapFieldChanges(peersField, c) {
		m[change.Key] = change
	}
	return m
}

func TestStructCoalescing(t *testing.T) {
	initial := reco.NewStruct(record{}).WithDelta(reco.StructDelta[record]{peerEdit(1, "a"), memberEdit(1, true)})
	current := initial
	var pending reco.StructChanges[record]
	batches := []reco.StructDelta[record]{
		{peerEdit(1, "b"), peerEdit(2, "new"), labelField.Set("middle"), memberEdit(2, true)},
		{peerEdit(1, "a"), labelField.Set(""), memberEdit(2, false)},
		{peerEdit(1, "c"), memberEdit(2, true)},
	}
	var retained reco.StructChanges[record]
	for i, batch := range batches {
		next := current.WithDelta(batch)
		var err error
		pending, err = pending.Then(next.ChangesSince(current))
		if err != nil {
			t.Fatal(err)
		}
		current = next
		if i == 0 {
			retained = pending
		}
	}
	if pending.Len() != 2 {
		t.Fatalf("fields=%v", slices.Collect(pending.Fields()))
	}
	if pending.ChangeCount() != 3 {
		t.Fatalf("change count=%d", pending.ChangeCount())
	}
	want := map[int]reco.MapChange[int, string]{1: {Key: 1, Before: "a", BeforeValid: true, After: "c", AfterValid: true}, 2: {Key: 2, After: "new", AfterValid: true}}
	if !maps.Equal(peerChanges(pending), want) {
		t.Fatalf("peers=%v", peerChanges(pending))
	}
	if got := peerChanges(retained)[1].After; got != "b" {
		t.Fatalf("mutated earlier batch: %q", got)
	}
	if _, _, changed := labelField.Change(pending); changed {
		t.Fatal("net-zero label reported")
	}
	if !pending.Before().RecoValueEqual(initial) || !pending.After().RecoValueEqual(current) {
		t.Fatal("wrong endpoints")
	}
	if !maps.Equal(peerChanges(pending.After().ChangesSince(pending.Before())), want) {
		t.Fatal("composed lineage lost")
	}
	if _, err := pending.Then(retained); err == nil {
		t.Fatal("accepted noncontiguous batch")
	}
	if got, err := pending.Then(reco.StructChanges[record]{}); err != nil || got.Len() != pending.Len() {
		t.Fatal("zero accumulator is not identity")
	}
}

func TestStructChangeEpochAndReplacement(t *testing.T) {
	a := reco.NewStruct(record{})
	b := a.WithDelta(reco.StructDelta[record]{peerEdit(1, "a")})
	// Reusing a value at a later graph version must not accept old stream changes.
	a1 := a.RecoWithVersion(1).(reco.StructSnapshot[record])
	b2 := b.RecoWithVersion(2).(reco.StructSnapshot[record])
	a3 := a.RecoWithVersion(3).(reco.StructSnapshot[record])
	if _, err := a3.ChangesSince(b2).Then(b2.ChangesSince(a1)); err == nil {
		t.Fatal("accepted replay with reused root")
	}
	// Unrelated replacement roots use reconciliation, still yielding exact changes.
	replacement := reco.NewStruct(record{Peers: (reco.MapSnapshot[int, string]{}).WithDelta(reco.MapDelta[int, string]{Put: []reco.MapEntry[int, string]{{Key: 2, Value: ""}}})})
	want := map[int]reco.MapChange[int, string]{1: {Key: 1, Before: "a", BeforeValid: true}, 2: {Key: 2, AfterValid: true}}
	if got := peerChanges(replacement.ChangesSince(b)); !maps.Equal(got, want) {
		t.Fatalf("replacement: %v", got)
	}
	cleared := replacement.WithDelta(reco.StructDelta[record]{peersField.Update(func(m reco.MapSnapshot[int, string]) reco.MapSnapshot[int, string] {
		return m.WithDelta(reco.MapDelta[int, string]{Clear: true})
	})})
	if got := peerChanges(cleared.ChangesSince(replacement)); len(got) != 1 || !got[2].BeforeValid || got[2].AfterValid {
		t.Fatalf("clear: %v", got)
	}
}

func TestStructAtomicInitialWatchConcurrentUpdates(t *testing.T) {
	type counter struct{ N int }
	field := reco.Field[counter, int]("N")
	g := reco.NewGraph()
	n := reco.StructData[counter]("counter")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	change(t, g, func(tx *reco.Tx) { reco.Set(tx, n, reco.NewStruct(counter{})) })
	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		<-start
		for i := 1; i <= 100; i++ {
			if err := g.Update(func(tx *reco.Tx) error {
				reco.ApplyStructDelta(tx, n, reco.StructDelta[counter]{field.Set(i)})
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	close(start)
	// The writer can be anywhere in its loop when snapshot/watch setup wins the lock.
	events := make(chan reco.StructEvent[counter], 100)
	initial, sub, err := reco.SubscribeStruct(g, n, reco.SubscribeOptions{}, func(ev reco.StructEvent[counter]) { events <- ev })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	// Do not process deltas until the writer finishes (simulating slow initial encoding).
	<-done
	current := initial.Value()
	var pending reco.StructChanges[counter]
	for len(events) > 0 {
		ev := <-events
		if !ev.Previous.RecoValueEqual(current) {
			t.Fatal("gap after initial snapshot")
		}
		pending, err = pending.Then(ev.Changes)
		if err != nil {
			t.Fatal(err)
		}
		current = ev.Current
	}
	if current.Value().N != 100 {
		t.Fatalf("missed update: %v", current.Value())
	}
}

func TestStructMapNilValuesAndCustomEquality(t *testing.T) {
	type nullable struct{ Peers reco.MapSnapshot[int, *int] }
	field := reco.Field[nullable, reco.MapSnapshot[int, *int]]("Peers")
	var before reco.StructSnapshot[nullable]
	after := before.WithDelta(reco.StructDelta[nullable]{field.Update(func(m reco.MapSnapshot[int, *int]) reco.MapSnapshot[int, *int] {
		return m.WithDelta(reco.MapDelta[int, *int]{Put: []reco.MapEntry[int, *int]{{Key: 1}}})
	})})
	cs := slices.Collect(reco.MapFieldChanges(field, after.ChangesSince(before)))
	if len(cs) != 1 || !cs[0].AfterValid || cs[0].After != nil {
		t.Fatal("nil map value treated as deletion")
	}
	type counted struct {
		Values reco.MapSnapshot[int, countedValue]
	}
	values := reco.Field[counted, reco.MapSnapshot[int, countedValue]]("Values")
	reads := 0
	var seed reco.MapDelta[int, countedValue]
	for i := range 10000 {
		seed.Put = append(seed.Put, reco.MapEntry[int, countedValue]{Key: i, Value: countedValue{i, &reads}})
	}
	base := reco.NewStruct(counted{Values: (reco.MapSnapshot[int, countedValue]{}).WithDelta(seed)})
	reads = 0
	one := base.WithDelta(reco.StructDelta[counted]{values.Update(func(m reco.MapSnapshot[int, countedValue]) reco.MapSnapshot[int, countedValue] {
		return m.WithDelta(reco.MapDelta[int, countedValue]{Put: []reco.MapEntry[int, countedValue]{{Key: 5, Value: countedValue{-1, &reads}}}})
	})})
	if reads > 10 {
		t.Fatalf("point edit compared %d values", reads)
	}
	reads = 0
	changes := one.ChangesSince(base)
	for range 5 {
		if changes.Len() != 1 || changes.ChangeCount() != 1 || len(slices.Collect(changes.Fields())) != 1 || len(slices.Collect(reco.MapFieldChanges(values, changes))) != 1 {
			t.Fatal("wrong change counts")
		}
	}
	if reads != 0 {
		t.Fatalf("reading delta compared %d values", reads)
	}
}

func TestStructRandomCoalescedDeltas(t *testing.T) {
	rng := rand.New(rand.NewPCG(31, 72))
	var current reco.StructSnapshot[record]
	for batch := 0; batch < 80; batch++ {
		before := current
		var pending reco.StructChanges[record]
		for range 12 {
			mapDelta := reco.MapDelta[int, string]{Clear: rng.IntN(20) == 0}
			setDelta := reco.SetDelta[int]{Clear: rng.IntN(20) == 0}
			for range 3 {
				k := rng.IntN(20)
				mapDelta.Remove = append(mapDelta.Remove, k)
				setDelta.Remove = append(setDelta.Remove, k)
				k = rng.IntN(20)
				mapDelta.Put = append(mapDelta.Put, reco.MapEntry[int, string]{Key: k, Value: fmt.Sprint(rng.IntN(5))})
				setDelta.Add = append(setDelta.Add, k)
			}
			next := current.WithDelta(reco.StructDelta[record]{
				peersField.Update(func(m reco.MapSnapshot[int, string]) reco.MapSnapshot[int, string] { return m.WithDelta(mapDelta) }),
				membersField.Update(func(s reco.SetSnapshot[int]) reco.SetSnapshot[int] { return s.WithDelta(setDelta) }),
				labelField.Set(fmt.Sprint(rng.IntN(3))),
			})
			var err error
			pending, err = pending.Then(next.ChangesSince(current))
			if err != nil {
				t.Fatal(err)
			}
			current = next
		}
		want := make(map[int]reco.MapChange[int, string])
		for k := range 20 {
			a, aok := before.Value().Peers.Get(k)
			b, bok := current.Value().Peers.Get(k)
			if aok != bok || a != b {
				want[k] = reco.MapChange[int, string]{Key: k, Before: a, BeforeValid: aok, After: b, AfterValid: bok}
			}
		}
		if !maps.Equal(peerChanges(pending), want) {
			t.Fatalf("batch %d map delta differs", batch)
		}
		gotSet := make(map[int]bool)
		for c := range reco.SetFieldChanges(membersField, pending) {
			gotSet[c.Key] = c.Present
		}
		wantSet := make(map[int]bool)
		for k := range 20 {
			if before.Value().Members.Contains(k) != current.Value().Members.Contains(k) {
				wantSet[k] = current.Value().Members.Contains(k)
			}
		}
		if !maps.Equal(gotSet, wantSet) {
			t.Fatalf("batch %d set delta differs", batch)
		}
	}
}

func TestStructTransactionsAndWatch(t *testing.T) {
	g := reco.NewGraph()
	n := reco.StructData[record]("record")
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	var events []reco.StructEvent[record]
	initial, sub, err := reco.SubscribeStruct(g, n, reco.SubscribeOptions{}, func(ev reco.StructEvent[record]) { events = append(events, ev) })
	if err != nil || initial.Valid() {
		t.Fatalf("initial=%v err=%v", initial, err)
	}
	defer sub.Unsubscribe()
	change(t, g, func(tx *reco.Tx) {
		reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{peerEdit(1, "a"), memberEdit(1, true)})
		reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{peerEdit(2, "b"), labelField.Set("ready")})
	})
	if len(events) != 1 || events[0].Changes.Len() != 3 || len(peerChanges(events[0].Changes)) != 2 {
		t.Fatalf("events=%+v", events)
	}
	if events[0].Current.Version() != events[0].Version {
		t.Fatal("record version not stamped")
	}
	before := readValue(t, g, n)
	change(t, g, func(tx *reco.Tx) {
		reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{peerEdit(1, "temporary"), memberEdit(2, true)})
		reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{peerEdit(1, "a"), memberEdit(2, false)})
	})
	if len(events) != 1 || readValue(t, g, n).Version() != before.Version() {
		t.Fatal("net-zero transaction notified")
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{labelField.Set("rolled back")})
		return fmt.Errorf("rollback")
	}); err == nil {
		t.Fatal("expected rollback")
	}
	if len(events) != 1 || readValue(t, g, n).Value().Label != "ready" {
		t.Fatal("rollback published")
	}
	sub.Unsubscribe()
	sub.Unsubscribe()
	change(t, g, func(tx *reco.Tx) { reco.ApplyStructDelta(tx, n, reco.StructDelta[record]{labelField.Set("unwatched")}) })
	if len(events) != 1 {
		t.Fatal("unsubscribe failed")
	}
}

func TestStructNodeAssemblyAndGraphIsolation(t *testing.T) {
	type schema struct {
		Label string
		Peers reco.MapSnapshot[int, string]
	}
	label := reco.Data[string]("label")
	peers := reco.MapData[int, string]("peers")
	n := nodes.Struct[schema]("mrm", struct {
		Label reco.Node[string]
		Peers reco.Node[reco.MapSnapshot[int, string]]
	}{label, peers})
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			g := reco.NewGraph()
			if err := g.Register(n); err != nil {
				t.Error(err)
				return
			}
			change(t, g, func(tx *reco.Tx) { reco.Set(tx, label, fmt.Sprint(i)) })
			if s, err := reco.Read(g, n); err != nil || s.Valid() {
				t.Errorf("premature record: %v %v", s, err)
			}
			var events []reco.StructEvent[schema]
			_, sub, err := reco.SubscribeStruct(g, n, reco.SubscribeOptions{}, func(ev reco.StructEvent[schema]) { events = append(events, ev) })
			if err != nil {
				t.Error(err)
				return
			}
			defer sub.Unsubscribe()
			change(t, g, func(tx *reco.Tx) { reco.MapPut(tx, peers, i, "a") })
			change(t, g, func(tx *reco.Tx) { reco.MapPut(tx, peers, i, "b"); reco.Set(tx, label, "changed") })
			if len(events) != 2 || events[1].Changes.Len() != 2 || events[1].Current.Value().Peers.Len() != 1 {
				t.Errorf("events=%v", events)
			}
			field := reco.Field[schema, reco.MapSnapshot[int, string]]("Peers")
			if cs := slices.Collect(reco.MapFieldChanges(field, events[1].Changes)); len(cs) != 1 || cs[0].Key != i || cs[0].Before != "a" || cs[0].After != "b" {
				t.Errorf("changes=%v", cs)
			}
		})
	}
	wg.Wait()
}

func TestStructValidation(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   func()
	}{
		{"non-struct", func() { reco.NewStruct(1) }},
		{"private-field", func() { reco.NewStruct(struct{ private int }{}) }},
		{"embedded-field", func() { reco.NewStruct(struct{ record }{}) }},
		{"unknown-field", func() { reco.Field[record, int]("Missing") }},
		{"wrong-type", func() { reco.Field[record, int]("Label") }},
		{"zero-field", func() { var f reco.StructField[record, string]; f.Set("x") }},
		{"zero-edit", func() { reco.NewStruct(record{}).WithDelta(reco.StructDelta[record]{{}}) }},
		{"nil-update", func() { labelField.Update(nil) }},
		{"missing-binding", func() { nodes.Struct[record]("bad", struct{}{}) }},
		{"wrong-binding-name", func() { nodes.Struct[struct{ A int }]("bad", struct{ B reco.Node[int] }{reco.Data[int]("b")}) }},
		{"wrong-binding-type", func() { nodes.Struct[struct{ A int }]("bad", struct{ A reco.Node[string] }{reco.Data[string]("a")}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			test.fn()
		})
	}
	var zero reco.StructSnapshot[record]
	if zero.Version() != 0 || !reflect.DeepEqual(zero.Value(), record{}) || zero.ChangesSince(zero).Len() != 0 {
		t.Fatal("invalid zero snapshot")
	}
	if !strings.Contains(peersField.Name(), "Peers") {
		t.Fatal("wrong field name")
	}
	for _, n := range []reco.Node[reco.StructSnapshot[record]]{{}, reco.StructData[record]("unregistered")} {
		if _, _, err := reco.SubscribeStruct(reco.NewGraph(), n, reco.SubscribeOptions{}, func(reco.StructEvent[record]) {}); err == nil {
			t.Fatal("invalid subscription accepted")
		}
	}
	if _, _, err := reco.SubscribeStruct(reco.NewGraph(), reco.StructData[record]("n"), reco.SubscribeOptions{}, nil); err == nil {
		t.Fatal("nil subscriber accepted")
	}
}

func TestStructEmptyInitialEvent(t *testing.T) {
	g := reco.NewGraph()
	n := nodes.Struct[struct{}]("empty", struct{}{})
	if err := g.Register(n); err != nil {
		t.Fatal(err)
	}
	var events []reco.StructEvent[struct{}]
	initial, sub, err := reco.SubscribeStruct(g, n, reco.SubscribeOptions{}, func(ev reco.StructEvent[struct{}]) { events = append(events, ev) })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if initial.Valid() {
		t.Fatal("uncomputed initial record is valid")
	}
	change(t, g, func(*reco.Tx) {})
	if len(events) != 1 || events[0].Changes.ChangeCount() != 0 || events[0].Version == 0 {
		t.Fatalf("empty initialization must still emit: %v", events)
	}
	change(t, g, func(*reco.Tx) {})
	if len(events) != 1 {
		t.Fatal("empty no-op emitted again")
	}
}

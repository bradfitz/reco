package reco_test

import (
	"maps"
	"slices"
	"testing"

	"recontrol/reco"
)

func TestSnapshotWithDelta(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		var empty reco.SetSnapshot[string]
		add := []string{"a", "b", "a"}
		before := empty.WithDelta(reco.SetDelta[string]{Add: add})
		add[0] = "mutated"
		if empty.Len() != 0 || before.Len() != 2 || !before.Contains("a") || before.Version() != 0 {
			t.Fatal("construction mutated the base, aliased the delta, or assigned a version")
		}
		for _, d := range []reco.SetDelta[string]{
			{}, {Remove: []string{"absent"}, Add: []string{"a"}},
			{Remove: []string{"a"}, Add: []string{"a"}},
			{Clear: true, Add: []string{"b", "a"}},
		} {
			after := before.WithDelta(d)
			if !before.RecoValueEqual(after) || len(slices.Collect(after.ChangesSince(before))) != 0 {
				t.Fatalf("net-zero batch did not reuse root: %+v", d)
			}
		}
		after := before.WithDelta(reco.SetDelta[string]{Clear: true, Remove: []string{"c"}, Add: []string{"b", "c", "c"}})
		if after.Len() != 2 || !after.Contains("b") || !after.Contains("c") || before.Contains("c") {
			t.Fatal("clear/remove/add order or immutability violated")
		}
		assertSetChanges(t, after, before, map[string]bool{"a": false, "c": true})
		assertSetChanges(t, after.WithDelta(reco.SetDelta[string]{Clear: true}), after, map[string]bool{"b": false, "c": false})
		copied := after.Changes()
		copied[0].Key = "mutated"
		assertSetChanges(t, after, before, map[string]bool{"a": false, "c": true})
	})
	t.Run("map", func(t *testing.T) {
		var empty reco.MapSnapshot[string, *int]
		one, two := 1, 2
		put := []reco.MapEntry[string, *int]{{Key: "a", Value: &one}, {Key: "b"}}
		before := empty.WithDelta(reco.MapDelta[string, *int]{Put: put})
		put[0].Key = "mutated"
		if empty.Len() != 0 || before.Len() != 2 || before.Version() != 0 {
			t.Fatal("bad construction")
		}
		if v, ok := before.Get("b"); !ok || v != nil {
			t.Fatal("nil value was treated as deletion")
		}
		for _, d := range []reco.MapDelta[string, *int]{
			{}, {Remove: []string{"absent"}},
			{Remove: []string{"a"}, Put: []reco.MapEntry[string, *int]{{Key: "a", Value: &one}}},
			{Put: []reco.MapEntry[string, *int]{{Key: "a", Value: &two}, {Key: "a", Value: &one}}},
			{Clear: true, Put: []reco.MapEntry[string, *int]{{Key: "b"}, {Key: "a", Value: &one}}},
		} {
			after := before.WithDelta(d)
			if !before.RecoValueEqual(after) || len(slices.Collect(after.ChangesSince(before))) != 0 {
				t.Fatalf("net-zero batch did not reuse root: %+v", d)
			}
		}
		after := before.WithDelta(reco.MapDelta[string, *int]{
			Clear: true, Remove: []string{"c"},
			Put: []reco.MapEntry[string, *int]{{Key: "b"}, {Key: "c", Value: &one}, {Key: "c", Value: &two}},
		})
		want := map[string]reco.MapChange[string, *int]{
			"a": {Key: "a", Before: &one, BeforeValid: true},
			"c": {Key: "c", After: &two, AfterValid: true},
		}
		assertMapChanges(t, after, before, want)
		if v, ok := after.Get("c"); !ok || v != &two || after.Len() != 2 {
			t.Fatal("clear/remove/put or last-put-wins order violated")
		}
		if v, ok := before.Get("a"); !ok || v != &one || before.Len() != 2 {
			t.Fatal("old snapshot was mutated")
		}
		copied := after.Changes()
		copied[0].Key = "mutated"
		assertMapChanges(t, after, before, want)
		assertMapChanges(t, after.WithDelta(reco.MapDelta[string, *int]{Clear: true}), after, map[string]reco.MapChange[string, *int]{
			"b": {Key: "b", BeforeValid: true},
			"c": {Key: "c", Before: &two, BeforeValid: true},
		})
	})
}

func assertSetChanges[K comparable](t testing.TB, current, previous reco.SetSnapshot[K], want map[K]bool) {
	t.Helper()
	got := make(map[K]bool)
	for c := range current.ChangesSince(previous) {
		if _, ok := got[c.Key]; ok {
			t.Fatalf("duplicate change for %v", c.Key)
		}
		got[c.Key] = c.Present
	}
	if !maps.Equal(got, want) {
		t.Fatalf("changes=%v want=%v", got, want)
	}
}

func assertMapChanges[K, V comparable](t testing.TB, current, previous reco.MapSnapshot[K, V], want map[K]reco.MapChange[K, V]) {
	t.Helper()
	got := make(map[K]reco.MapChange[K, V])
	for c := range current.ChangesSince(previous) {
		if _, ok := got[c.Key]; ok {
			t.Fatalf("duplicate change for %v", c.Key)
		}
		got[c.Key] = c
	}
	if !maps.Equal(got, want) {
		t.Fatalf("changes=%v want=%v", got, want)
	}
}

func TestChangesSinceLineage(t *testing.T) {
	var empty reco.SetSnapshot[int]
	a := empty.WithDelta(reco.SetDelta[int]{Add: []int{1, 2}})
	b := a.WithDelta(reco.SetDelta[int]{Remove: []int{1}, Add: []int{3}})
	c := b.WithDelta(reco.SetDelta[int]{Remove: []int{2}, Add: []int{1, 4}})
	replacement := empty.WithDelta(reco.SetDelta[int]{Add: []int{4, 5}})
	assertSetChanges(t, a, empty, map[int]bool{1: true, 2: true})
	assertSetChanges(t, b, a, map[int]bool{1: false, 3: true})
	assertSetChanges(t, c, a, map[int]bool{2: false, 3: true, 4: true})
	assertSetChanges(t, replacement, c, map[int]bool{1: false, 3: false, 5: true})
	assertSetChanges(t, empty, replacement, map[int]bool{4: false, 5: false})
	assertSetChanges(t, c, c, nil)
	// A shared graph version is not evidence of shared lineage.
	assertSetChanges(t, b.RecoWithVersion(42).(reco.SetSnapshot[int]), replacement.RecoWithVersion(42).(reco.SetSnapshot[int]), map[int]bool{2: true, 3: true, 4: false, 5: false})
	for _, previous := range []reco.SetSnapshot[int]{a, empty, replacement} {
		calls := 0
		b.ChangesSince(previous)(func(reco.SetChange[int]) bool { calls++; return false })
		if calls != 1 {
			t.Fatalf("set iterator ignored early stop: %d calls", calls)
		}
	}

	var zero reco.MapSnapshot[int, int]
	x := zero.WithDelta(reco.MapDelta[int, int]{Put: []reco.MapEntry[int, int]{{Key: 1, Value: 10}, {Key: 2, Value: 20}}})
	y := x.WithDelta(reco.MapDelta[int, int]{Remove: []int{1}, Put: []reco.MapEntry[int, int]{{Key: 2, Value: 21}, {Key: 3, Value: 30}}})
	z := y.WithDelta(reco.MapDelta[int, int]{Remove: []int{3}, Put: []reco.MapEntry[int, int]{{Key: 1, Value: 10}, {Key: 4, Value: 40}}})
	assertMapChanges(t, x, zero, map[int]reco.MapChange[int, int]{1: {Key: 1, After: 10, AfterValid: true}, 2: {Key: 2, After: 20, AfterValid: true}})
	assertMapChanges(t, y, x, map[int]reco.MapChange[int, int]{
		1: {Key: 1, Before: 10, BeforeValid: true},
		2: {Key: 2, Before: 20, BeforeValid: true, After: 21, AfterValid: true},
		3: {Key: 3, After: 30, AfterValid: true},
	})
	assertMapChanges(t, z, x, map[int]reco.MapChange[int, int]{
		2: {Key: 2, Before: 20, BeforeValid: true, After: 21, AfterValid: true},
		4: {Key: 4, After: 40, AfterValid: true},
	})
	assertMapChanges(t, zero, x, map[int]reco.MapChange[int, int]{1: {Key: 1, Before: 10, BeforeValid: true}, 2: {Key: 2, Before: 20, BeforeValid: true}})
	assertMapChanges(t, z, z, nil)
	for _, previous := range []reco.MapSnapshot[int, int]{x, zero, z} {
		calls := 0
		y.ChangesSince(previous)(func(reco.MapChange[int, int]) bool { calls++; return false })
		if calls != 1 {
			t.Fatalf("map iterator ignored early stop: %d calls", calls)
		}
	}
}

type countedValue struct {
	n     int
	reads *int
}

func (v countedValue) RecoValueEqual(other any) bool {
	*v.reads++
	o, ok := other.(countedValue)
	return ok && v.n == o.n
}

func TestPublicMapDeltaOnlyComparesTouchedValues(t *testing.T) {
	for _, size := range []int{1000, 10000} {
		reads := 0
		var seed reco.MapDelta[int, countedValue]
		for i := range size {
			seed.Put = append(seed.Put, reco.MapEntry[int, countedValue]{Key: i, Value: countedValue{i, &reads}})
		}
		before := (reco.MapSnapshot[int, countedValue]{}).WithDelta(seed)
		reads = 0
		after := before.WithDelta(reco.MapDelta[int, countedValue]{Put: []reco.MapEntry[int, countedValue]{{Key: 0, Value: countedValue{-1, &reads}}}})
		if reads == 0 || reads > 4 {
			t.Fatalf("N=%d: one edit compared %d values", size, reads)
		}
		reads = 0
		if changes := slices.Collect(after.ChangesSince(before)); len(changes) != 1 || changes[0].Key != 0 || reads != 0 {
			t.Fatalf("N=%d: adjacent delta compared %d values: %v", size, reads, changes)
		}
		assertExternalSharing(t, before, after, 1)
	}
}

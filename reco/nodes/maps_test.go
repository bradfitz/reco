package nodes_test

import (
	"math/rand/v2"
	"sync"
	"testing"

	"recontrol/reco"
	"recontrol/reco/nodes"
)

func TestMapProjectionsRandomBatches(t *testing.T) {
	source := reco.MapData[int, int]("source")
	keys, values := nodes.MapKeys("keys", source), nodes.MapValues("values", source)
	g := reco.NewGraph()
	if err := g.Register(keys, values); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.Set(tx, source, reco.MapSnapshot[int, int]{}) })
	var keyEvents, valueEvents []reco.Event[reco.SetSnapshot[int]]
	keySub, err := reco.Subscribe(g, keys, reco.SubscribeOptions{}, func(ev reco.Event[reco.SetSnapshot[int]]) { keyEvents = append(keyEvents, ev) })
	if err != nil {
		t.Fatal(err)
	}
	defer keySub.Unsubscribe()
	valueSub, err := reco.Subscribe(g, values, reco.SubscribeOptions{}, func(ev reco.Event[reco.SetSnapshot[int]]) { valueEvents = append(valueEvents, ev) })
	if err != nil {
		t.Fatal(err)
	}
	defer valueSub.Unsubscribe()
	want := make(map[int]int)
	rng := rand.New(rand.NewPCG(123, 456))
	for range 400 {
		oldKeys, oldValues := mustRead(t, g, keys), mustRead(t, g, values)
		keyEvents, valueEvents = nil, nil
		update(t, g, func(tx *reco.Tx) {
			for range 5 {
				k, v := rng.IntN(40), rng.IntN(12)
				switch rng.IntN(16) {
				case 0:
					reco.Set(tx, source, reco.MapSnapshot[int, int]{})
					clear(want)
				case 1:
					reco.Set(tx, source, (reco.MapSnapshot[int, int]{}).WithDelta(reco.MapDelta[int, int]{Put: []reco.MapEntry[int, int]{{Key: k, Value: v}}}))
					want = map[int]int{k: v}
				case 2, 3, 4:
					reco.MapDelete(tx, source, k)
					delete(want, k)
				default:
					reco.MapPut(tx, source, k, v)
					want[k] = v
				}
			}
		})
		wantKeys, wantValues := make(map[int]bool), make(map[int]bool)
		for k, v := range want {
			wantKeys[k] = true
			wantValues[v] = true
		}
		newKeys, newValues := mustRead(t, g, keys), mustRead(t, g, values)
		assertMembers(t, newKeys, wantKeys)
		assertMembers(t, newValues, wantValues)
		assertSetEvents(t, oldKeys, newKeys, keyEvents)
		assertSetEvents(t, oldValues, newValues, valueEvents)
	}
}

func TestMapValuesDuplicatesTransfersAndSwaps(t *testing.T) {
	source := reco.MapData[int, string]("source")
	keys, values := nodes.MapKeys("keys", source), nodes.MapValues("values", source)
	calls := 0
	mapped := nodes.MapSet("mapped", values, func(v string) int { calls++; return len(v) })
	g := reco.NewGraph()
	if err := g.Register(keys, mapped); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) {
		reco.MapPut(tx, source, 1, "a")
		reco.MapPut(tx, source, 2, "a")
		reco.MapPut(tx, source, 3, "b")
		reco.MapPut(tx, source, 4, "")
	})
	if calls != 3 {
		t.Fatalf("initial mapper calls=%d, want 3", calls)
	}
	for _, test := range []struct {
		name                           string
		mutate                         func(*reco.Tx)
		want                           map[string]bool
		calls                          int
		keysUnchanged, valuesUnchanged bool
	}{
		{"remove-one-duplicate", func(tx *reco.Tx) { reco.MapDelete(tx, source, 1) }, map[string]bool{"a": true, "b": true, "": true}, 0, false, true},
		{"transfer-between-keys", func(tx *reco.Tx) { reco.MapDelete(tx, source, 2); reco.MapPut(tx, source, 5, "a") }, map[string]bool{"a": true, "b": true, "": true}, 0, false, true},
		{"swap-values", func(tx *reco.Tx) { reco.MapPut(tx, source, 3, "a"); reco.MapPut(tx, source, 5, "b") }, map[string]bool{"a": true, "b": true, "": true}, 0, true, true},
		{"change-last-contributor", func(tx *reco.Tx) { reco.MapPut(tx, source, 3, "c") }, map[string]bool{"c": true, "b": true, "": true}, 1, true, false},
		{"remove-zero-value", func(tx *reco.Tx) { reco.MapDelete(tx, source, 4) }, map[string]bool{"c": true, "b": true}, 0, false, false},
		{"clear", func(tx *reco.Tx) { reco.Set(tx, source, reco.MapSnapshot[int, string]{}) }, map[string]bool{}, 0, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldKeys, oldValues := mustRead(t, g, keys), mustRead(t, g, values)
			calls = 0
			update(t, g, test.mutate)
			newKeys, newValues := mustRead(t, g, keys), mustRead(t, g, values)
			assertMembers(t, newValues, test.want)
			if calls != test.calls || oldKeys.RecoValueEqual(newKeys) != test.keysUnchanged || oldValues.RecoValueEqual(newValues) != test.valuesUnchanged {
				t.Fatalf("calls=%d; unexpected root change or missed update", calls)
			}
		})
	}
}

func TestMapKeysNonComparableValues(t *testing.T) {
	source := reco.MapData[int, []string]("source")
	keys := nodes.MapKeys("keys", source)
	g := reco.NewGraph()
	if err := g.Register(keys); err != nil {
		t.Fatal(err)
	}
	update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, 1, nil); reco.MapPut(tx, source, 2, []string{"a"}) })
	before := mustRead(t, g, keys)
	update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, 1, []string{"b"}); reco.MapPut(tx, source, 2, nil) })
	if !before.RecoValueEqual(mustRead(t, g, keys)) {
		t.Fatal("value-only update changed key set")
	}
	assertMembers(t, before, map[int]bool{1: true, 2: true})
}

func TestMapValuesNil(t *testing.T) {
	source := reco.MapData[string, *int]("source")
	values := nodes.MapValues("values", source)
	g := reco.NewGraph()
	if err := g.Register(values); err != nil {
		t.Fatal(err)
	}
	value := 7
	update(t, g, func(tx *reco.Tx) {
		reco.MapPut(tx, source, "a", nil)
		reco.MapPut(tx, source, "b", nil)
		reco.MapPut(tx, source, "c", &value)
	})
	assertMembers(t, mustRead(t, g, values), map[*int]bool{nil: true, &value: true})
	update(t, g, func(tx *reco.Tx) { reco.MapDelete(tx, source, "a") })
	assertMembers(t, mustRead(t, g, values), map[*int]bool{nil: true, &value: true})
	update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, "b", &value) })
	assertMembers(t, mustRead(t, g, values), map[*int]bool{&value: true})
}

func TestMapValuesPreservesComparableIdentity(t *testing.T) {
	source := reco.MapData[string, *int]("source")
	values := nodes.MapValues("values", source)
	g := reco.NewGraph()
	if err := g.Register(values); err != nil {
		t.Fatal(err)
	}
	a, b := 7, 7 // Equal pointees but distinct set keys.
	update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, "key", &a) })
	update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, "key", &b) })
	assertMembers(t, mustRead(t, g, values), map[*int]bool{&b: true})
	// Replacements reconcile using the same equality as adjacent deltas.
	update(t, g, func(tx *reco.Tx) {
		reco.Set(tx, source, (reco.MapSnapshot[string, *int]{}).WithDelta(reco.MapDelta[string, *int]{Put: []reco.MapEntry[string, *int]{{Key: "key", Value: &a}}}))
	})
	assertMembers(t, mustRead(t, g, values), map[*int]bool{&a: true})
	before := mustRead(t, g, source)
	update(t, g, func(tx *reco.Tx) {
		reco.Set(tx, source, before.WithDelta(reco.MapDelta[string, *int]{Put: []reco.MapEntry[string, *int]{{Key: "key", Value: &b}}}))
	})
	assertMembers(t, mustRead(t, g, values), map[*int]bool{&b: true})
}

func TestMapProjectionsGraphIsolation(t *testing.T) {
	source := reco.MapData[int, int]("source")
	keys, values := nodes.MapKeys("keys", source), nodes.MapValues("values", source)
	var wg sync.WaitGroup
	for graphID := range 4 {
		wg.Go(func() {
			g := reco.NewGraph()
			if err := g.Register(keys, values); err != nil {
				t.Error(err)
				return
			}
			wantKeys, wantValues := make(map[int]bool), make(map[int]bool)
			for i := range 40 {
				k, v := graphID*100+i, graphID*100+i/2
				update(t, g, func(tx *reco.Tx) { reco.MapPut(tx, source, k, v) })
				wantKeys[k], wantValues[v] = true, true
			}
			assertMembers(t, mustRead(t, g, keys), wantKeys)
			assertMembers(t, mustRead(t, g, values), wantValues)
		})
	}
	wg.Wait()
}

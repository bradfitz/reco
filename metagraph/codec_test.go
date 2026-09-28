package metagraph

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/bradfitz/reco"
)

func TestSetCodecAtomicOperations(t *testing.T) {
	g := reco.NewGraph()
	n := reco.SetData[string]("set")
	must(t, g.Register(n))
	c := Set[string]("v1")
	events := 0
	_, err := reco.Subscribe(g, n, reco.SubscribeOptions{}, func(reco.Event[reco.SetSnapshot[string]]) { events++ })
	must(t, err)
	for i, step := range []struct {
		payload string
		full    bool
		want    []string
	}{
		{`{"clear":true,"add":["a","b"]}`, true, []string{"a", "b"}},
		{`{"remove":["a","b"],"add":["b","c"]}`, false, []string{"b", "c"}},
		{`{"clear":true,"remove":["c"],"add":["z"]}`, false, []string{"z"}},
		{`{"clear":true}`, false, nil},
	} {
		must(t, g.Update(func(tx *reco.Tx) error { return c.Apply(tx, n, json.RawMessage(step.payload), step.full) }))
		s, err := reco.Read(g, n)
		must(t, err)
		got := slices.Collect(s.Value().All())
		slices.Sort(got)
		if !slices.Equal(got, step.want) || events != i+1 {
			t.Fatalf("step %d: %v, events %d", i, got, events)
		}
	}
	if err := g.Update(func(tx *reco.Tx) error { return c.Apply(tx, n, json.RawMessage(`{"add":["bad"]}`), true) }); err == nil {
		t.Fatal("snapshot without clear accepted")
	}
}

func TestMapCodecTupleKeysNilAndReplacement(t *testing.T) {
	type key struct {
		Partition string
		ID        int
	}
	g := reco.NewGraph()
	n := reco.MapData[key, *int]("map")
	must(t, g.Register(n))
	c := Map[key, *int]("v1")
	value := 42
	var previous reco.MapSnapshot[key, *int]
	first := previous.WithDelta(reco.MapDelta[key, *int]{Put: []reco.MapEntry[key, *int]{{Key: key{"p", 1}, Value: &value}, {Key: key{"p", 2}, Value: nil}}})
	second := first.WithDelta(reco.MapDelta[key, *int]{Remove: []key{{"p", 1}}, Put: []reco.MapEntry[key, *int]{{Key: key{"p", 3}, Value: nil}}})
	for i, cur := range []reco.MapSnapshot[key, *int]{first, second, {}} {
		raw, err := c.Encode(previous, cur, i != 1)
		must(t, err)
		must(t, g.Update(func(tx *reco.Tx) error { return c.Apply(tx, n, raw, i != 1) }))
		got, err := reco.Read(g, n)
		must(t, err)
		if got.Value().Len() != cur.Len() {
			t.Fatal("map length differs")
		}
		for k, v := range cur.All() {
			gv, ok := got.Value().Get(k)
			if !ok || (v == nil) != (gv == nil) || (v != nil && *v != *gv) {
				t.Fatalf("key %v: %v", k, gv)
			}
		}
		previous = cur
	}
}

var encodedKeys atomic.Int64

type countedKey int

func (k countedKey) MarshalJSON() ([]byte, error) {
	encodedKeys.Add(1)
	return []byte(fmt.Sprint(int(k))), nil
}

func TestCollectionCodecsEncodeOnlyDelta(t *testing.T) {
	for _, size := range []int{10, 10000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var empty reco.SetSnapshot[countedKey]
			keys := make([]countedKey, size)
			entries := make([]reco.MapEntry[countedKey, int], size)
			for i := range size {
				keys[i] = countedKey(i)
				entries[i] = reco.MapEntry[countedKey, int]{Key: keys[i], Value: i}
			}
			old := empty.WithDelta(reco.SetDelta[countedKey]{Add: keys})
			cur := old.WithDelta(reco.SetDelta[countedKey]{Add: []countedKey{countedKey(size)}, Remove: []countedKey{0}})
			encodedKeys.Store(0)
			raw, err := Set[countedKey]("v1").Encode(old, cur, false)
			must(t, err)
			if count := encodedKeys.Load(); count != 2 {
				t.Fatalf("set marshaled %d keys, want 2", count)
			}
			if len(raw) > 100 {
				t.Fatalf("large point delta: %s", raw)
			}
			var em reco.MapSnapshot[countedKey, int]
			om := em.WithDelta(reco.MapDelta[countedKey, int]{Put: entries})
			cm := om.WithDelta(reco.MapDelta[countedKey, int]{Put: []reco.MapEntry[countedKey, int]{{Key: 1, Value: -1}}, Remove: []countedKey{0}})
			encodedKeys.Store(0)
			_, err = Map[countedKey, int]("v1").Encode(om, cm, false)
			must(t, err)
			if count := encodedKeys.Load(); count != 2 {
				t.Fatalf("map marshaled %d keys, want 2", count)
			}
		})
	}
}

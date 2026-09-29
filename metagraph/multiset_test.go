package metagraph

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/bradfitz/reco"
)

func TestMultisetCodec(t *testing.T) {
	g := reco.NewGraph()
	n := reco.MultisetData[int]("counts")
	must(t, g.Register(n))
	codec := Multiset[int]("v1")
	var prev reco.MultisetSnapshot[int]
	first, err := prev.WithDelta(reco.MultisetDelta[int]{Put: []reco.MultisetEntry[int]{{Key: 1, Count: 3}, {Key: 2, Count: 9}}})
	must(t, err)
	second, err := first.WithDelta(reco.MultisetDelta[int]{Adjust: map[int]int64{1: -3, 2: 1, 3: 4}})
	must(t, err)
	for i, cur := range []reco.MultisetSnapshot[int]{first, second, {}} {
		raw, err := codec.Encode(prev, cur, i != 1)
		must(t, err)
		for range 2 { // Replayed absolute count assignments do not add twice.
			must(t, g.Update(func(tx *reco.Tx) error { return codec.Apply(tx, n, raw, i != 1) }))
			got, err := reco.Read(g, n)
			must(t, err)
			if !maps.Equal(maps.Collect(got.Value().All()), maps.Collect(cur.All())) {
				t.Fatal("codec lost multiplicities")
			}
		}
		prev = cur
	}
	for _, bad := range []string{`null`, `{}`, `{"clear":true,"put":[{"key":1,"value":3},{"key":2,"value":-1}]}`, `{"clear":true,"put":[{"key":1,"value":9223372036854775808}]}`} {
		err := g.Update(func(tx *reco.Tx) error { return codec.Apply(tx, n, json.RawMessage(bad), true) })
		if err == nil {
			t.Fatalf("accepted invalid snapshot: %s", bad)
		}
		got, err := reco.Read(g, n)
		must(t, err)
		if got.Value().Len() != 0 {
			t.Fatal("invalid count partially committed")
		}
	}
}

func TestMultisetCodecTouchesOnlyDelta(t *testing.T) {
	var d reco.MultisetDelta[countedKey]
	for k := range 10000 {
		d.Put = append(d.Put, reco.MultisetEntry[countedKey]{Key: countedKey(k), Count: 2})
	}
	a, err := (reco.MultisetSnapshot[countedKey]{}).WithDelta(d)
	must(t, err)
	b, err := a.WithDelta(reco.MultisetDelta[countedKey]{Adjust: map[countedKey]int64{1: -1}})
	must(t, err)
	encodedKeys.Store(0)
	_, err = Multiset[countedKey]("v1").Encode(a, b, false)
	must(t, err)
	if n := encodedKeys.Load(); n != 1 {
		t.Fatalf("encoded %d keys for one changed count", n)
	}
}

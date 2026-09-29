package reco

import (
	"fmt"
	"slices"
	"testing"

	"github.com/benbjohnson/immutable"
)

type GroupCountsTestFunc func(NodeClassName, Node[MapSnapshot[int, SetSnapshot[int]]], Node[MapSnapshot[int, int]]) Node[MapSnapshot[int, MultisetSnapshot[int]]]
type InvertSetsTestFunc func(NodeClassName, Node[MapSnapshot[int, SetSnapshot[int]]]) Node[MapSnapshot[int, SetSnapshot[int]]]

// CheckIndexDeltaWork instruments outer and nested collections. A one-member
// edit must not hash unrelated keys or copy the output's untouched HAMT branches.
func CheckIndexDeltaWork(t *testing.T, grouped GroupCountsTestFunc, invert InvertSetsTestFunc) {
	for _, size := range []int{1024, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			in := MapData[int, SetSnapshot[int]]("input")
			groups := MapData[int, int]("groups")
			out, inv := grouped("counted", in, groups), invert("inverted", in)
			g := NewGraph()
			if err := g.Register(out, inv); err != nil {
				t.Fatal(err)
			}
			hOuter, hGroups, hInner := &countingHasher{}, &countingHasher{}, &countingHasher{}
			hOuter.reset()
			hGroups.reset()
			hInner.reset()
			large := makeSet(size, hInner)
			mb := immutable.NewMapBuilder[int, SetSnapshot[int]](hOuter)
			gb := immutable.NewMapBuilder[int, int](hGroups)
			for i := range size {
				mb.Set(i, (SetSnapshot[int]{}).WithDelta(SetDelta[int]{Add: []int{i}}))
				gb.Set(i, i%2)
			}
			mb.Set(0, large)
			input := MapSnapshot[int, SetSnapshot[int]]{items: mb.Map()}
			update(t, g, func(tx *Tx) { Set(tx, in, input); Set(tx, groups, MapSnapshot[int, int]{items: gb.Map()}) })
			before, beforeInv := mustRead(t, g, out), mustRead(t, g, inv)
			hOuter.reset()
			hGroups.reset()
			hInner.reset()
			large = large.WithDelta(SetDelta[int]{Add: []int{size}})
			update(t, g, func(tx *Tx) { MapPut(tx, in, 0, large) })
			hOuter.check(t, []int{0}, 128)
			hGroups.check(t, []int{0}, 128)
			hInner.check(t, []int{size}, 64)
			after, afterInv := mustRead(t, g, out), mustRead(t, g, inv)
			if c := slices.Collect(after.ChangesSince(before)); len(c) != 1 || c[0].Key != 0 {
				t.Fatal("not one counted bucket")
			}
			a, _ := before.Get(0)
			b, _ := after.Get(0)
			if c := slices.Collect(b.ChangesSince(a)); len(c) != 1 || c[0] != (MultisetChange[int]{size, 0, 1}) {
				t.Fatal("not one counted member")
			}
			assertSharedHAMT(t, a.counts.items, b.counts.items, 1)
			assertSharedHAMT(t, beforeInv.items, afterInv.items, 1)
		})
	}
}

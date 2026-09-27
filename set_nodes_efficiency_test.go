package reco

import (
	"fmt"
	"testing"

	"github.com/benbjohnson/immutable"
)

// These test-only entry points receive constructors from reco_test so the
// instrumentation can inspect core storage without importing reco/nodes here.
type SetOperationTestFunc func(NodeClassName, ...Node[SetSnapshot[int]]) Node[SetSnapshot[int]]
type MapProjectionTestFunc func(NodeClassName, Node[MapSnapshot[int, int]]) Node[SetSnapshot[int]]

func CheckSetOperationDeltaWork(t *testing.T, build SetOperationTestFunc) {
	for _, size := range []int{1024, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a, b := SetData[int]("a"), SetData[int]("b")
			out := build("out", a, b)
			computations := 0
			countCompute(out, &computations)
			g := NewGraph()
			if err := g.Register(out); err != nil {
				t.Fatal(err)
			}
			ha, hb := &countingHasher{}, &countingHasher{}
			ha.reset()
			hb.reset()
			builder := immutable.NewMapBuilder[int, struct{}](hb)
			for k := size; k < 3*size; k++ {
				builder.Set(k, struct{}{})
			}
			update(t, g, func(tx *Tx) {
				Set(tx, a, makeSet(2*size, ha))
				Set(tx, b, SetSnapshot[int]{items: builder.Map()})
			})
			before := mustRead(t, g, out)
			events := 0
			_, err := Subscribe(g, out, SubscribeOptions{}, func(ev Event[SetSnapshot[int]]) {
				events++
				if len(ev.Current.Value().Changes()) != 2 {
					t.Fatal("expected one two-key delta")
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			ha.reset()
			hb.reset()
			computations = 0
			update(t, g, func(tx *Tx) {
				SetDelete(tx, a, 0)    // Only A had this member.
				SetDelete(tx, b, size) // Both inputs had this member.
				SetUpsert(tx, a, 3*size+1)
				SetUpsert(tx, b, 3*size+1)
			})
			for _, h := range []*countingHasher{ha, hb} {
				h.check(t, []int{0, size, 3*size + 1}, 256)
			}
			if computations != 1 || events != 1 {
				t.Fatalf("computations=%d events=%d", computations, events)
			}
			after := mustRead(t, g, out)
			assertSharedHAMT(t, before.items, after.items, 3)
			ha.reset()
			hb.reset()
			computations, events = 0, 0
			update(t, g, func(tx *Tx) { SetDelete(tx, a, 1); SetUpsert(tx, a, 1) })
			ha.check(t, []int{1}, 64)
			hb.check(t, nil, 0)
			if computations != 0 || events != 0 || mustRead(t, g, out).items != after.items {
				t.Fatal("net-zero input propagated")
			}
		})
	}
}

func CheckMapProjectionDeltaWork(t *testing.T, build MapProjectionTestFunc, keysOnly bool) {
	for _, size := range []int{1024, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			source := MapData[int, int]("source")
			out := build("out", source)
			computations := 0
			countCompute(out, &computations)
			g := NewGraph()
			if err := g.Register(out); err != nil {
				t.Fatal(err)
			}
			h := &countingHasher{}
			h.reset()
			update(t, g, func(tx *Tx) { Set(tx, source, makeMap(size, h)) })
			events, changes := 0, 0
			_, err := Subscribe(g, out, SubscribeOptions{}, func(ev Event[SetSnapshot[int]]) { events++; changes = len(ev.Current.Value().Changes()) })
			if err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name                                   string
				mutate                                 func(*Tx)
				keys                                   []int
				computations, keyChanges, valueChanges int
			}{
				{"add", func(tx *Tx) { MapPut(tx, source, size, size) }, []int{size}, 1, 1, 1},
				{"value-only", func(tx *Tx) { MapPut(tx, source, 0, -1) }, []int{0}, 1, 0, 2},
				{"duplicate-value", func(tx *Tx) { MapPut(tx, source, size+1, 1) }, []int{size + 1}, 1, 1, 0},
				{"remove-one-contributor", func(tx *Tx) { MapDelete(tx, source, 1) }, []int{1}, 1, 1, 0},
				{"remove-last-contributor", func(tx *Tx) { MapDelete(tx, source, size+1) }, []int{size + 1}, 1, 1, 1},
				{"swap", func(tx *Tx) { MapPut(tx, source, 2, 3); MapPut(tx, source, 3, 2) }, []int{2, 3}, 1, 0, 0},
				{"net-zero", func(tx *Tx) { MapPut(tx, source, 4, -4); MapPut(tx, source, 4, 4) }, []int{4}, 0, 0, 0},
			} {
				t.Run(test.name, func(t *testing.T) {
					before := mustRead(t, g, out)
					h.reset()
					computations, events, changes = 0, 0, 0
					update(t, g, test.mutate)
					h.check(t, test.keys, 64*len(test.keys))
					want := test.valueChanges
					if keysOnly {
						want = test.keyChanges
					}
					wantEvents := 0
					if want != 0 {
						wantEvents = 1
					}
					if computations != test.computations || events != wantEvents || changes != want {
						t.Fatalf("computations=%d events=%d changes=%d; want %d,%d,%d", computations, events, changes, test.computations, wantEvents, want)
					}
					after := mustRead(t, g, out)
					if want == 0 {
						if before.items != after.items {
							t.Fatal("unchanged output replaced root")
						}
					} else {
						assertSharedHAMT(t, before.items, after.items, want)
					}
				})
			}
		})
	}
}

func BenchSetOperationDelta(b *testing.B, build SetOperationTestFunc, both bool) {
	for _, size := range []int{1000, 100000} {
		for _, delta := range []int{1, 16} {
			b.Run(fmt.Sprintf("N=%d/delta=%d", size, delta), func(b *testing.B) {
				a, other := SetData[int]("a"), SetData[int]("b")
				g := NewGraph()
				if err := g.Register(build("out", a, other)); err != nil {
					b.Fatal(err)
				}
				if err := g.Update(func(tx *Tx) error {
					Set(tx, a, makeSet(size, newComparableHasher[int]()))
					if both {
						Set(tx, other, makeSet(size, newComparableHasher[int]()))
					} else {
						Set(tx, other, SetSnapshot[int]{})
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := g.Update(func(tx *Tx) error {
						for k := size; k < size+delta; k++ {
							if i%2 == 0 {
								SetUpsert(tx, a, k)
								if both {
									SetUpsert(tx, other, k)
								}
							} else {
								SetDelete(tx, a, k)
								if both {
									SetDelete(tx, other, k)
								}
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchMapProjectionDelta(b *testing.B, build MapProjectionTestFunc) {
	for _, size := range []int{1000, 100000} {
		for _, delta := range []int{1, 16} {
			b.Run(fmt.Sprintf("N=%d/delta=%d", size, delta), func(b *testing.B) {
				source := MapData[int, int]("source")
				g := NewGraph()
				if err := g.Register(build("out", source)); err != nil {
					b.Fatal(err)
				}
				if err := g.Update(func(tx *Tx) error { Set(tx, source, makeMap(size, newComparableHasher[int]())); return nil }); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := g.Update(func(tx *Tx) error {
						for k := size; k < size+delta; k++ {
							if i%2 == 0 {
								MapPut(tx, source, k, k)
							} else {
								MapDelete(tx, source, k)
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

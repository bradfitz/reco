package nodes

import "github.com/bradfitz/reco"

// Union creates the union of two or more sets. A key remains present while any
// input contains it. Repeated inputs have the same effect as a single input.
func Union[K comparable](className reco.NodeClassName, inputs ...reco.Node[reco.SetSnapshot[K]]) reco.Node[reco.SetSnapshot[K]] {
	return combineSets(className, "Union", inputs, func(n int) bool { return n > 0 })
}

// Intersection contains the keys present in every input. At least two inputs
// are required. Repeated inputs do not change the result.
func Intersection[K comparable](className reco.NodeClassName, inputs ...reco.Node[reco.SetSnapshot[K]]) reco.Node[reco.SetSnapshot[K]] {
	total := len(inputs)
	return combineSets(className, "Intersection", inputs, func(n int) bool { return n == total })
}

// Xor creates the symmetric difference of two or more sets: keys present in an
// odd number of inputs. With two inputs, this means present in exactly one.
// Repeated inputs count separately, so Xor(a, a) is empty.
func Xor[K comparable](className reco.NodeClassName, inputs ...reco.Node[reco.SetSnapshot[K]]) reco.Node[reco.SetSnapshot[K]] {
	return combineSets(className, "Xor", inputs, func(n int) bool { return n%2 != 0 })
}

// combineSets accumulates changes across all inputs before updating membership.
// Point edits visit only delta keys, regardless of total collection size.
func combineSets[K comparable](className reco.NodeClassName, name string, inputs []reco.Node[reco.SetSnapshot[K]], present func(int) bool) reco.Node[reco.SetSnapshot[K]] {
	if len(inputs) < 2 {
		panic("nodes: " + name + " needs at least two sets")
	}
	inputs = append([]reco.Node[reco.SetSnapshot[K]](nil), inputs...)
	deps := make([]reco.Dependency, len(inputs))
	for i, n := range inputs {
		deps[i] = n
	}
	return reco.Operator(className, deps, func() reco.Compute[reco.SetSnapshot[K]] {
		previous := make([]reco.SetSnapshot[K], len(inputs))
		var out countedSet[K]
		return func(eval reco.Eval) reco.Result[reco.SetSnapshot[K]] {
			changes := make(map[K]int)
			for i, input := range inputs {
				cur := reco.Input(eval, input).Value()
				for c := range cur.ChangesSince(previous[i]) {
					if c.Present {
						changes[c.Key]++
					} else {
						changes[c.Key]--
					}
				}
				previous[i] = cur
			}
			return reco.OK(out.apply(changes, present))
		}
	})
}

// countedSet retains contributor counts even for keys absent from the output.
// Both the counts and published set use persistent storage.
type countedSet[K comparable] struct {
	counts reco.MultisetSnapshot[K]
	value  reco.SetSnapshot[K]
}

func (s *countedSet[K]) apply(changes map[K]int, present func(int) bool) reco.SetSnapshot[K] {
	adjust := make(map[K]int64, len(changes))
	for k, diff := range changes {
		adjust[k] = int64(diff)
	}
	counts, err := s.counts.WithDelta(reco.MultisetDelta[K]{Adjust: adjust})
	if err != nil {
		panic(err) // An operator bug: contributors are nonnegative and fit int.
	}
	var delta reco.SetDelta[K]
	for k, diff := range changes {
		if diff == 0 {
			continue
		}
		was, now := present(int(s.counts.Count(k))), present(int(counts.Count(k)))
		if was == now {
			continue
		}
		if now {
			delta.Add = append(delta.Add, k)
		} else {
			delta.Remove = append(delta.Remove, k)
		}
	}
	s.counts = counts
	s.value = s.value.WithDelta(delta)
	return s.value
}

// Difference contains keys in first that are absent from every other input:
// Difference(a, b, c) means a minus the union of b and c. At least one other
// input is required. Difference(universe, a) gives a relative complement.
func Difference[K comparable](className reco.NodeClassName, first reco.Node[reco.SetSnapshot[K]], others ...reco.Node[reco.SetSnapshot[K]]) reco.Node[reco.SetSnapshot[K]] {
	if len(others) == 0 {
		panic("nodes: Difference needs at least two sets")
	}
	others = append([]reco.Node[reco.SetSnapshot[K]](nil), others...)
	deps := []reco.Dependency{first}
	for _, n := range others {
		deps = append(deps, n)
	}
	return reco.Operator(className, deps, func() reco.Compute[reco.SetSnapshot[K]] {
		var previousFirst reco.SetSnapshot[K]
		previousOthers := make([]reco.SetSnapshot[K], len(others))
		var counts reco.MapSnapshot[K, int]
		var out reco.SetSnapshot[K]
		return func(eval reco.Eval) reco.Result[reco.SetSnapshot[K]] {
			current := reco.Input(eval, first).Value()
			touched := make(map[K]bool)
			for c := range current.ChangesSince(previousFirst) {
				touched[c.Key] = true
			}
			previousFirst = current
			net := make(map[K]int)
			for i, n := range others {
				cur := reco.Input(eval, n).Value()
				for c := range cur.ChangesSince(previousOthers[i]) {
					if c.Present {
						net[c.Key]++
					} else {
						net[c.Key]--
					}
				}
				previousOthers[i] = cur
			}
			var countDelta reco.MapDelta[K, int]
			for k, diff := range net {
				if diff == 0 {
					continue
				}
				before, _ := counts.Get(k)
				after := before + diff
				if after == 0 {
					countDelta.Remove = append(countDelta.Remove, k)
				} else {
					countDelta.Put = append(countDelta.Put, reco.MapEntry[K, int]{Key: k, Value: after})
				}
				touched[k] = true
			}
			counts = counts.WithDelta(countDelta)
			var delta reco.SetDelta[K]
			for k := range touched {
				exclusions, _ := counts.Get(k)
				if current.Contains(k) && exclusions == 0 {
					delta.Add = append(delta.Add, k)
				} else {
					delta.Remove = append(delta.Remove, k)
				}
			}
			out = out.WithDelta(delta)
			return reco.OK(out)
		}
	})
}

// MapSet preserves the keys of input and computes fn(k) for each added key.
// fn must be pure and depend only on k. Per-key reactive dependencies are not
// supported yet. Deletions and unchanged members do not call fn.
// It is implemented entirely through the public [reco.Operator] and snapshot APIs.
func MapSet[K comparable, V any](className reco.NodeClassName, input reco.Node[reco.SetSnapshot[K]], fn func(K) V) reco.Node[reco.MapSnapshot[K, V]] {
	if fn == nil {
		panic("nodes: nil MapSet function")
	}
	return reco.Operator(className, []reco.Dependency{input}, func() reco.Compute[reco.MapSnapshot[K, V]] {
		var previous reco.SetSnapshot[K]
		var out reco.MapSnapshot[K, V]
		return func(eval reco.Eval) reco.Result[reco.MapSnapshot[K, V]] {
			cur := reco.Input(eval, input).Value()
			var delta reco.MapDelta[K, V]
			for change := range cur.ChangesSince(previous) {
				if change.Present {
					delta.Put = append(delta.Put, reco.MapEntry[K, V]{Key: change.Key, Value: fn(change.Key)})
				} else {
					delta.Remove = append(delta.Remove, change.Key)
				}
			}
			previous = cur
			out = out.WithDelta(delta)
			return reco.OK(out)
		}
	})
}

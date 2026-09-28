package metagraph

import (
	"encoding/json"
	"errors"

	"github.com/bradfitz/reco"
)

// Codec defines application-versioned wire values. Name must change when the
// payload's meaning changes. Functions must be deterministic, bounded CPU work:
// Encode runs in graph callbacks; Apply runs inside a local graph transaction.
// An Apply error aborts the transaction. Values, including nested map values,
// must obey reco's immutability rules. Codecs must not perform network I/O.
type Codec[T any] struct {
	Name   string
	Encode func(previous, current T, full bool) (json.RawMessage, error)
	Apply  func(*reco.Tx, reco.Node[T], json.RawMessage, bool) error
}

func (c Codec[T]) valid() bool { return c.Name != "" && c.Encode != nil && c.Apply != nil }

// JSON replaces a scalar (or an application-defined JSON value) on each change.
// schema is an explicit application schema/version, not a Go reflect type name.
func JSON[T any](schema string) Codec[T] {
	return Codec[T]{Name: "json/" + schema, Encode: func(_, cur T, _ bool) (json.RawMessage, error) { return json.Marshal(cur) },
		Apply: func(tx *reco.Tx, n reco.Node[T], raw json.RawMessage, _ bool) error {
			var v T
			if err := json.Unmarshal(raw, &v); err != nil {
				return err
			}
			reco.Set(tx, n, v)
			return nil
		}}
}

type setDelta[K comparable] struct {
	Clear  bool `json:"clear,omitempty"`
	Remove []K  `json:"remove,omitempty"`
	Add    []K  `json:"add,omitempty"`
}

// Set sends one clear/add snapshot, then only changed memberships. Each message
// applies clear, remove, add atomically. No collection scan for consecutive deltas.
func Set[K comparable](schema string) Codec[reco.SetSnapshot[K]] {
	return Codec[reco.SetSnapshot[K]]{Name: "set/" + schema,
		Encode: func(prev, cur reco.SetSnapshot[K], full bool) (json.RawMessage, error) {
			d := setDelta[K]{Clear: full}
			if full {
				for k := range cur.All() {
					d.Add = append(d.Add, k)
				}
			} else {
				for c := range cur.ChangesSince(prev) {
					if c.Present {
						d.Add = append(d.Add, c.Key)
					} else {
						d.Remove = append(d.Remove, c.Key)
					}
				}
			}
			return json.Marshal(d)
		}, Apply: func(tx *reco.Tx, n reco.Node[reco.SetSnapshot[K]], raw json.RawMessage, full bool) error {
			var d *setDelta[K]
			if err := json.Unmarshal(raw, &d); err != nil {
				return err
			}
			if d == nil {
				return errors.New("null set delta")
			}
			if full && !d.Clear {
				return errors.New("set snapshot lacks clear")
			}
			reco.ApplySetDelta(tx, n, reco.SetDelta[K]{Clear: d.Clear, Remove: d.Remove, Add: d.Add})
			return nil
		}}
}

type mapEntry[K comparable, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}
type mapDelta[K comparable, V any] struct {
	Clear  bool             `json:"clear,omitempty"`
	Remove []K              `json:"remove,omitempty"`
	Put    []mapEntry[K, V] `json:"put,omitempty"`
}

// Map sends entry arrays, not JSON object keys, allowing arbitrary JSON keys.
// Changed values are whole entry values, not recursively diffed objects.
func Map[K comparable, V any](schema string) Codec[reco.MapSnapshot[K, V]] {
	return Codec[reco.MapSnapshot[K, V]]{Name: "map/" + schema,
		Encode: func(prev, cur reco.MapSnapshot[K, V], full bool) (json.RawMessage, error) {
			d := mapDelta[K, V]{Clear: full}
			if full {
				for k, v := range cur.All() {
					d.Put = append(d.Put, mapEntry[K, V]{k, v})
				}
			} else {
				for c := range cur.ChangesSince(prev) {
					if c.AfterValid {
						d.Put = append(d.Put, mapEntry[K, V]{c.Key, c.After})
					} else {
						d.Remove = append(d.Remove, c.Key)
					}
				}
			}
			return json.Marshal(d)
		}, Apply: func(tx *reco.Tx, n reco.Node[reco.MapSnapshot[K, V]], raw json.RawMessage, full bool) error {
			var d *mapDelta[K, V]
			if err := json.Unmarshal(raw, &d); err != nil {
				return err
			}
			if d == nil {
				return errors.New("null map delta")
			}
			if full && !d.Clear {
				return errors.New("map snapshot lacks clear")
			}
			if d.Clear {
				reco.Set(tx, n, reco.MapSnapshot[K, V]{})
			}
			for _, k := range d.Remove {
				reco.MapDelete(tx, n, k)
			}
			for _, e := range d.Put {
				reco.MapPut(tx, n, e.Key, e.Value)
			}
			return nil
		}}
}

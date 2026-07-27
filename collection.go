package recontrol

// MapSnapshot is a stable snapshot of a map node value.
type MapSnapshot[K comparable, V any] struct {
	items   map[K]V
	version Version
}

// Get returns a value by key.
func (s MapSnapshot[K, V]) Get(k K) (V, bool) {
	v, ok := s.items[k]
	return v, ok
}

// Len returns the number of map entries.
func (s MapSnapshot[K, V]) Len() int {
	return len(s.items)
}

// Range calls fn for each entry until fn returns false.
func (s MapSnapshot[K, V]) Range(fn func(K, V) bool) {
	for k, v := range s.items {
		if !fn(k, v) {
			return
		}
	}
}

// Version returns the graph-local version at which this snapshot was observed.
func (s MapSnapshot[K, V]) Version() Version {
	return s.version
}

// SetSnapshot is a stable snapshot of a set node value.
type SetSnapshot[K comparable] struct {
	items   map[K]struct{}
	version Version
}

// Contains reports whether k is present.
func (s SetSnapshot[K]) Contains(k K) bool {
	_, ok := s.items[k]
	return ok
}

// Len returns the number of set entries.
func (s SetSnapshot[K]) Len() int {
	return len(s.items)
}

// Range calls fn for each key until fn returns false.
func (s SetSnapshot[K]) Range(fn func(K) bool) {
	for k := range s.items {
		if !fn(k) {
			return
		}
	}
}

// Version returns the graph-local version at which this snapshot was observed.
func (s SetSnapshot[K]) Version() Version {
	return s.version
}

// MapData creates a mutable map data node.
func MapData[K comparable, V any](key string) Node[MapSnapshot[K, V]] {
	return Data[MapSnapshot[K, V]](key)
}

// SetData creates a mutable set data node.
func SetData[K comparable](key string) Node[SetSnapshot[K]] {
	return Data[SetSnapshot[K]](key)
}

// MapPut upserts one map entry.
func MapPut[K comparable, V any](tx *Tx, node Node[MapSnapshot[K, V]], key K, value V) {
	tx.mustData(node.def)
	cur, _ := tx.current(node.def).value.(MapSnapshot[K, V])
	next := MapSnapshot[K, V]{items: cloneMap(cur.items)}
	next.items[key] = value
	tx.writes[node.def] = next
}

// MapDelete removes one map entry.
func MapDelete[K comparable, V any](tx *Tx, node Node[MapSnapshot[K, V]], key K) {
	tx.mustData(node.def)
	cur, _ := tx.current(node.def).value.(MapSnapshot[K, V])
	next := MapSnapshot[K, V]{items: cloneMap(cur.items)}
	delete(next.items, key)
	tx.writes[node.def] = next
}

// SetUpsert adds one set key.
func SetUpsert[K comparable](tx *Tx, node Node[SetSnapshot[K]], key K) {
	tx.mustData(node.def)
	cur, _ := tx.current(node.def).value.(SetSnapshot[K])
	next := SetSnapshot[K]{items: cloneSet(cur.items)}
	next.items[key] = struct{}{}
	tx.writes[node.def] = next
}

// SetDelete removes one set key.
func SetDelete[K comparable](tx *Tx, node Node[SetSnapshot[K]], key K) {
	tx.mustData(node.def)
	cur, _ := tx.current(node.def).value.(SetSnapshot[K])
	next := SetSnapshot[K]{items: cloneSet(cur.items)}
	delete(next.items, key)
	tx.writes[node.def] = next
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneSet[K comparable](m map[K]struct{}) map[K]struct{} {
	out := make(map[K]struct{}, len(m)+1)
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

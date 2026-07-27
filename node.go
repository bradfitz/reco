package recontrol

import "fmt"

// Node is a typed handle to a value in a graph.
type Node[T any] struct {
	def *nodeDef
}

// Key returns the stable node key.
func (n Node[T]) Key() string {
	if n.def == nil {
		return ""
	}
	return n.def.key
}

// Valid reports whether this handle references a node definition.
func (n Node[T]) Valid() bool {
	return n.def != nil
}

func (n Node[T]) recontrolTypedNode() typedNode {
	if n.def == nil {
		panic("recontrol: zero node handle")
	}
	return typedNode{def: n.def, typ: typeOf[T]()}
}

// Data creates a mutable data node.
func Data[T any](key string) Node[T] {
	mustKey(key)
	return Node[T]{def: &nodeDef{
		key:  key,
		kind: nodeData,
	}}
}

// Func creates a function node using either a typed binding struct or an inline
// dependency declaration created with Deps.
func Func[DepsT any, Out any](key string, deps any, compute func(Context, DepsT) Result[Out]) Node[Out] {
	mustKey(key)
	if compute == nil {
		panic("recontrol: nil compute function")
	}
	bindings, adapter, err := compileFunc[DepsT, Out](deps, compute)
	if err != nil {
		panic(err)
	}
	return Node[Out]{def: &nodeDef{
		key:      key,
		kind:     nodeFunc,
		deps:     bindings,
		compute:  adapter,
		valueTyp: typeOf[Out](),
	}}
}

func mustKey(key string) {
	if key == "" {
		panic("recontrol: empty node key")
	}
}

func sameTypeNode[T any](n Node[T]) typedNode {
	if n.def == nil {
		panic("recontrol: zero node handle")
	}
	return typedNode{def: n.def, typ: typeOf[T]()}
}

type nodeKind int

const (
	nodeData nodeKind = iota
	nodeFunc
)

type nodeDef struct {
	key      string
	kind     nodeKind
	deps     []depBinding
	compute  computeFunc
	valueTyp typeID
}

type depBinding struct {
	name string
	node *nodeDef
	typ  typeID
}

type typedNode struct {
	def *nodeDef
	typ typeID
}

func (d *nodeDef) String() string {
	if d == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Node(%s)", d.key)
}

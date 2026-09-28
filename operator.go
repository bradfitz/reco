package reco

import "fmt"

// Dependency is a node that an operator declares as an input. Every Node[T]
// implements Dependency, allowing heterogeneous or variable-length input lists.
// Create nodes with Data, Func, or Operator; the runtime owns their handles.
type Dependency interface {
	nodeHandle
}

// Compute evaluates an operator using its declared inputs. Its closure may
// retain instance-local incremental caches, but must not mutate published values.
// The result must depend only on the inputs, not on the cache's history.
type Compute[T any] func(Eval) Result[T]

// Operator declares a reusable incremental node. newCompute is called lazily,
// per graph/node instance, to create its compute function and state. In demand-
// driven graphs, the state is discarded when last demand disappears and the
// factory runs again on reactivation. Caches must be rebuildable from inputs.
// Allocate mutable caches INSIDE newCompute, not in the shared definition.
// A factory must return a non-nil function. It can run concurrently with
// factories/computations in other graphs; calls within one graph are serialized.
//
// Compute reads declared inputs with Input. The runtime waits until they are
// initialized, schedules affected nodes in dependency order, and delivers the
// settled results. A compute function must not call Read, Update, or Subscribe
// on its graph, perform external side effects, or retain its Eval.
//
// For collections, use ChangesSince to consume input deltas and WithDelta to
// publish a new immutable output. A normal stateless computation can use Func.
func Operator[T any](className NodeClassName, deps []Dependency, newCompute func() Compute[T]) Node[T] {
	mustClassName(className)
	if newCompute == nil {
		panic("reco: nil operator factory")
	}
	bindings := make([]depBinding, len(deps))
	for i, dep := range deps {
		n, err := nodeFromAny(dep)
		if err != nil {
			panic(err)
		}
		bindings[i] = depBinding{node: n.def, typ: n.typ}
	}
	return Node[T]{def: &nodeDef{
		className: className, kind: nodeFunc, deps: bindings, valueTyp: typeOf[T](),
		newCompute: func() computeFunc {
			compute := newCompute()
			if compute == nil {
				panic(fmt.Sprintf("reco: operator %q factory returned nil", className))
			}
			return func(eval Eval, _ map[*nodeDef]nodeValue) nodeValue {
				result := compute(eval)
				return nodeValue{value: result.Value, valid: true, err: result.Err, isPartial: result.IsPartial}
			}
		},
	}}
}

// Input returns the current value, version, and result status of a declared
// dependency during a computation. It panics for a zero or undeclared node or
// outside an evaluation. It does not acquire a graph lock or create a watch.
// Unlike Changes(), collection ChangesSince(previous) safely handles both
// unchanged inputs and replacements since the operator last evaluated them.
func Input[T any](eval Eval, node Node[T]) Dep[T] {
	if node.def == nil {
		panic("reco: zero input node")
	}
	v, ok := eval.inputs[node.def]
	if !ok {
		panic(fmt.Sprintf("reco: input %q was not declared for this evaluation", node.ClassName()))
	}
	return Dep[T]{Value_: typedValue[T](v.value), Version_: v.version, Valid_: v.valid, Err_: v.err, IsPartial_: v.isPartial}
}

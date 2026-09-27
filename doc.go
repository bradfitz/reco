// Package reco is an experimental research prototype for reactive computation.
//
// # EXPERIMENTAL: DO NOT USE
//
// Nothing to see here; move along. This is unfinished research, not a supported
// library. Do not depend on it. APIs and behavior may change or disappear without
// notice. There are no compatibility or correctness guarantees.
//
// # Overview
//
// Reco stands for reactive computation. It provides typed, in-process
// reactive dataflow graphs.
// Declare mutable leaves with Data, SetData, MapData, or StructData, and derived
// nodes with Func. Register an output with Graph.Register to register its
// dependencies too.
// Graph.Update applies a transaction, recomputes affected nodes in dependency
// order, and delivers subscription events after the graph has settled.
// Node definitions can be reused across independent Graph instances.
//
// Each definition has a NodeClassName, supplied to its constructor and returned
// by Node.ClassName. Class names must be unique within a graph, including its
// dependencies; they are distinct from collection keys and future instance keys.
//
// # Incremental operators
//
// Package [github.com/bradfitz/reco/nodes] provides reusable incremental nodes,
// including set union and set-to-map computation. Packages can implement their own
// operators using Operator and Input.
// Allocate incremental caches inside the operator's per-graph compute factory.
// Consume collection changes with ChangesSince and publish one WithDelta batch
// per evaluation to preserve the incremental fast path. See the Operator example.
//
// Published values must be immutable. SetSnapshot and MapSnapshot provide
// structurally shared storage, native Go iterators, and atomic delta application.
// Custom immutable types may implement ValueEqualer and VersionedValue.
//
// # Typed records
//
// StructSnapshot[T] uses an ordinary Go struct as a fixed, typed schema. Field
// handles provide typed edits and change inspection. SubscribeStruct atomically
// obtains an initial snapshot and watches settled field/collection changes.
// StructChanges.Then coalesces contiguous events without scanning unchanged
// collection contents. Use MapFieldChanges and SetFieldChanges on the composed
// batch rather than comparing its endpoint collections. See the SubscribeStruct
// example for an incremental stream-encoding boundary.
package reco

# Recontrol Design

Status: living design document  
Last updated: 2026-07-26

This document is the working source of truth for the Recontrol prototype. It should be updated whenever the design, prototype scope, APIs, or unresolved questions change.

## Overview

Recontrol is intended to be a Go library for distributed reactive data binding.

The library should be generic enough for non-Tailscale applications. The first real application to keep in mind is a new Tailscale control/coordination server designed for easier maintenance and efficient scaling to large networks. In that environment, the main computed output is the rich, deep `tailcfg.MapResponse` value that a control server streams to clients. `MapResponse` includes the local node, peers, DNS configuration, packet filters, health/display messages, and other client-visible control-plane state.

The programming model is similar to a spreadsheet:

- Some nodes are mutable data cells.
- Some nodes are pure function cells.
- Function nodes depend on other nodes.
- When data changes, affected computations are scheduled and recomputed.
- Updates propagate through the dependency graph until the system reaches a fixed point.

Unlike a local spreadsheet, Recontrol is designed for a distributed system where data and computation are partitioned across shards. The initial prototype should validate the semantics in a single-process, multi-shard simulation before adding real networking.

For the Tailscale use case, some `MapResponse` values may involve hundreds of thousands of nodes. The design must therefore avoid full recomputation and full diffing work that is linear or quadratic in the whole tailnet whenever only a small part of the input changes.

## Core Goals

- Provide a Go library, not a standalone service.
- Support generic key and value types.
- Require all keys and values to be JSON-marshalable.
- Use canonical JSON bytes for identity, equality, hashing, and comparison.
- Model computation as a directed acyclic graph.
- Support local atomic transactions within one customer partition.
- Support reactive recomputation after transactions commit.
- Support subscriptions to both data values and computed function outputs.
- Support eventual consistency across shards.
- Support debouncing and coalescing of updates.
- Avoid unbounded goroutine creation.
- Provide explicit executor control over what computes when.
- Provide fairness between customers on the same machine.
- Make committed local data durable.
- Allow function nodes to return partial values with errors.
- Keep enough local replicated/cached input state for restart and outage tolerance.
- Support incremental propagation for maps and sets.
- Efficiently compute large Tailscale `tailcfg.MapResponse` values.
- Efficiently compute `MapResponse` deltas for the Tailscale wire protocol.
- Keep large structure updates and diffs sub-linear in the common case.
- Support fixed-point notifications so complete values are emitted only after reactive propagation settles.
- Coalesce or "Nagle" bursts of outbound deltas to avoid encoding redundant intermediate wire updates.

## Non-Goals For The First Prototype

- Real network transport.
- Production distributed consensus.
- Cross-customer atomic transactions.
- Cross-shard atomic transactions.
- Asking remote shards to perform computation for local nodes.
- Cross-shard subscriptions as a primary execution mechanism.
- Durable persistence of derived function values.
- Recursive nested patch semantics for collection values.
- A complete production scheduler.
- Automatic load balancing across shards.

The first prototype should prove the data model, graph semantics, transactions, subscriptions, executor shape, debouncing, durability boundary, local replicated-input behavior, and `MapResponse` delta behavior.

## Architecture Summary

The system consists of:

- Shards that own customer partitions.
- Data nodes that hold mutable durable values.
- Function nodes that hold pure computations over dependencies.
- A dependency graph that must be a DAG.
- An executor that schedules recomputation explicitly.
- A subscription layer for local updates and, later, replicated cross-shard inputs if needed.
- A persistence interface for committed data transactions.
- A routing layer supplied by the user.

For the prototype, multiple shards can run in one process and communicate through Go interfaces. That keeps distributed semantics testable without committing to a transport.

Current direction: for a given user/customer/tailnet, the owning shard should have all data needed to compute that user's outputs locally. If a node or tailnet is shared with another tailnet, the needed state should be replicated into the local shard. Earlier ideas about using shards as locators and asking a peer shard to compute a dependency are deferred because that model is likely too fragile for the initial design.

## Primary Use Case: Tailscale MapResponse

The main known workload is computing and streaming `tailcfg.MapResponse` values for Tailscale clients.

Decision: `MapResponse` is not a special library primitive. It should be implemented by the Tailscale application as a normal Recontrol node.

Decision: the Tailscale application may compute an internal map-shaped representation, roughly `map[string]any`, where each top-level `MapResponse` field is a map key for the initial full response shape.

Decision: Recontrol should not compute Tailscale wire deltas as graph nodes. The subscription watcher for the `MapResponse` node should diff two full snapshots cheaply and encode the appropriate delta `MapResponse` on the wire.

Important protocol facts from `tailcfg.MapResponse`:

- A streaming map poll begins with a complete `MapResponse`.
- Later responses are incremental updates with only changed information.
- The zero value for most fields means "unchanged".
- Some older fields predate that convention, especially slice fields with `omitempty`, so the real control server may need protocol-specific encoding types rather than blindly marshaling `tailcfg.MapResponse`.
- `Peers` is the complete peer list and is sorted by `Node.ID`.
- `PeersChanged` contains changed or added nodes and is also sorted by `Node.ID`.
- `PeersRemoved` contains removed node IDs.
- `PeersChangedPatch` is a lighter-weight peer patch mechanism for selected fields.
- `PacketFilters` and `DisplayMessages` already use map-shaped patch semantics where nil values can delete entries, and special keys can clear prior state.
- `CurrentCapabilityVersion` and older client versions matter. Different clients in the wild expect slightly different wire behavior.

Implications for Recontrol:

- The computed `MapResponse` node must know when it has reached its first fixed point so the server can serialize the initial complete response.
- After the first complete response, the system should prefer protocol deltas over complete responses.
- It is still desirable to invoke outbound callbacks only at fixed points, not for every intermediate recomputation.
- If many complete internal `MapResponse` states are produced in a wave, the wire encoder should coalesce pending work before producing deltas.
- The internal representation does not have to be exactly `tailcfg.MapResponse` if another representation makes incremental computation and diffing cheaper.
- The final encoder must preserve `tailcfg.MapResponse` value semantics and client capability behavior.
- The subscription API must expose enough snapshot/version identity to make old/new diffs cheap.

Open questions:

- Which `MapResponse` fields should be modeled as independent top-level map keys versus grouped state.
- Whether the internal representation should be `map[string]any`, a typed persistent map/tree, or a wrapper over one of those.
- How much of the existing Tailscale control-plane mapresponse marshaling logic can be reused.
- Whether `PeersChangedPatch` generation should be a first-class diff target.
- How client capability version selects output shape.

## Key Decisions So Far

### Library Scope

Decision: Recontrol should be a generic library that non-Tailscale applications can use.

Decision: Tailscale's control server is the first real application driving the design, performance requirements, and API validation.

Implication: the public API should not expose Tailscale-specific types, but it must be strong enough to model Tailscale's `MapResponse` workload without special cases in the library.

### Go And Types

Decision: the library will use Go generics in the public API where useful.

Decision: keys and values may be any Go type as long as they can be marshaled to JSON.

Decision: Go comparability is not required. Types that are not comparable in Go can still be used because identity is based on canonical JSON bytes.

Open detail: the exact canonical JSON implementation is not yet decided.

### Canonical Identity

All user keys and values are encoded into a canonical byte representation.

Those bytes are used for:

- Equality.
- Hashing.
- Map/set membership.
- Cache keys.
- Subscription identity.
- Delta application.
- Durable log identity.

This is especially important for non-comparable Go values, map keys, and set elements.

### Graph Model

Decision: the unit of a graph is one customer/tailnet.

Decision: one process may run many independent graphs, potentially up to millions.

Implications:

- Per-graph memory overhead must be small.
- Static node definitions should be shared across graph instances where possible.
- Per-graph runtime state must be separate from reusable node definitions.
- Transactions are serialized per graph.
- Scheduling fairness is needed between graphs/customers.

Decision: computation is represented as a DAG.

Decision: cycles are forbidden.

Decision: cycles should be rejected during graph construction or node registration, not discovered during propagation.

Reasoning: acyclic graphs make recomputation converge to a fixed point and avoid feedback loops.

### Data Nodes

Data nodes are mutable inputs.

Decision: data node updates happen through transactions.

Decision: data node committed state must be durable.

Decision: reactive propagation starts only after the transaction commits durably.

### Function Nodes

Function nodes are pure computations.

Requirements:

- Deterministic.
- Side-effect free.
- Fully determined by declared dependencies.
- Safe to cache.
- Safe to recompute.

Decision: function nodes may return partial results.

Decision: the preferred compute API should return `Result[T]` directly, without a separate `error` return.

Reasoning: a single `Result[T]` return discourages computation functions from using network or external side effects as hidden control flow. If richer failure control is needed later, the design can add explicit panic/sentinel values or a richer result type.

Example shape:

```go
type Result[T any] struct {
	Value     T
	Err       error
	IsPartial bool
}
```

Open detail: the structured error format for partial results is not yet defined.

Open detail: whether `Err` remains an `error`, becomes a structured JSON-marshalable value, or is split into library-managed status metadata.

### Function Versioning

Decision: functions should have explicit identity that includes a name and version.

Potential identity:

```go
type FunctionID struct {
	Name    string
	Version int
}
```

Decision: versions are independent. There is no implicit cache invalidation; new versions are distinct functions.

Open question: whether function names are globally scoped, package scoped, graph scoped, or customer scoped.

## Sharding Model

The keyspace is partitioned by customer ID or an equivalent key prefix.

Each customer partition is owned by one shard at a time.

Decision: shard ownership is determined by a user-supplied routing function.

Possible API:

```go
type ShardID string

type ShardMapper func(customerID string) ShardID
```

Requirements:

- Deterministic.
- Consistent across participants.
- Supplied by the user.
- Used by the indirection/routing layer.

Decision: shards may differ in compute capability, but this is acceptable because load is expected to be proportional to the number of customers assigned to each shard.

Open questions:

- How shard membership is represented.
- How routing changes are rolled out.
- Whether the library owns shard discovery or receives a complete routing table from the host application.
- What happens to in-flight subscriptions during customer migration.

## Transactions

Decision: a transaction may update multiple data cells atomically.

Decision: transactions are serialized per graph. Only one transaction runs at a time for a given customer/tailnet graph.

Transaction scope:

- One graph.
- One customer/tailnet.
- One owning shard/process instance.
- Local only.

Guarantees:

- Atomic.
- Durable before commit returns.
- Propagation begins after commit.

Non-goals:

- No multi-customer transactions.
- No cross-shard transactions.

Open questions:

- Exact transaction API.
- Whether transactions support compare-and-set preconditions.
- Whether transactions expose read-your-writes inside the transaction.
- Whether transaction commit returns updated versions for changed nodes.
- Whether durability is append-log-only in the prototype.
- Whether transaction serialization is enforced by a graph mutex, executor lane, or storage-level sequencing.

## Durability

Decision: committed data node updates must be durable.

Decision: derived values are not required to be durable. They can be recomputed from durable inputs and local replicated/cached inputs.

Prototype approach:

- Define a storage interface.
- Implement a simple file-backed append log or in-memory fake.
- Ensure transaction commit persists before applying visible state and triggering propagation.

Open questions:

- Storage interface shape.
- Snapshot/checkpoint strategy.
- Recovery ordering.
- Whether replicated input caches are persisted by the same storage layer.
- Whether the durable log stores typed JSON, canonical bytes, or both.

## Consistency Model

Decision: consistency is strong only within a local shard transaction.

Decision: cross-shard consistency is eventual.

Decision: subscribers do not need to observe every intermediate value.

Decision: debouncing and batching are allowed as long as subscribers converge to the final state.

Decision: out-of-order update delivery must be tolerated.

Decision: for externally visible computed outputs such as `MapResponse`, consumers should ideally be notified at fixed points rather than on every intermediate recomputation.

Implications:

- Updates need versions or sequence markers.
- Subscribers may discard stale updates.
- Subscribers may request or wait for snapshots when they miss required base versions.
- Intermediate values may be coalesced.
- The executor needs to know when a propagation wave has drained for a customer/output.
- The first complete `MapResponse` must not be serialized until the relevant graph has reached a fixed point.

Open questions:

- How fixed points are detected for one output node versus an entire customer graph.
- Whether fixed-point callbacks are default behavior or opt-in per subscription.
- How partial errors interact with fixed-point completion.

## Subscriptions

Clients and shards can subscribe to:

- Data node values.
- Function node outputs.

Decision: every subscription returns a handle that can unsubscribe.

Decision: the current preferred subscription shape exposes old and new immutable snapshots in the event so the caller can compute application-specific diffs cheaply.

Possible API:

```go
type SubscriptionHandle interface {
	Unsubscribe() error
}

type SubscribeOptions struct {
	FixedPointOnly bool
	Coalesce       bool
}

type Event[T any] struct {
	Previous Snapshot[T]
	Current  Snapshot[T]
	Version  uint64
}
```

Subscription behavior:

- Updates may arrive out of order.
- Subscribers should receive enough metadata to order, discard, or resync.
- Function output subscribers should have access to both old and new values so they can diff.
- Subscribers cache last-known-good values.
- Subscribers to externally encoded outputs can request fixed-point-only callbacks.
- Output encoders may coalesce multiple fixed-point updates before writing to the wire.

Open questions:

- Push callback API versus channel API.
- Whether unsubscribe is synchronous.
- Whether subscriptions are one-shot snapshot plus stream or stream-only.
- Backpressure behavior when subscribers are slow.
- Whether subscribers can request full snapshots explicitly.
- Whether fixed-point-only callbacks are part of the subscription API or a separate output-drain API.
- Exact shape of `Snapshot[T]` and whether scalar values use the same snapshot wrapper as collections.

## Cross-Shard Subscriptions And Caching

Current status: this section is retained as background, but it is not part of the first prototype direction.

Decision update: the first prototype should not depend on cross-shard remote computation. A user's owning shard should have all data needed to compute that user's values locally. Shared nodes or tailnets should be replicated into the local shard before they are needed for computation.

Decision: when one shard depends on another shard's value, the subscribing shard caches the results it receives.

Purpose:

- Restart recovery.
- Continued operation when a peer shard is down.
- Local fallback computation when possible.

Decision: explicit cache invalidation should not be necessary for pure function results. Input changes drive recomputation; function identity includes version.

Result precedence:

1. Authoritative remote result.
2. Cached remote result.
3. Local fallback result.

Open questions:

- Whether cached remote values are durable.
- How staleness is exposed to callers.
- Whether cached values have TTLs for observability, even if not for invalidation.
- Whether fallback results can be published downstream or are marked local-only.
- Whether any of this should remain in the design after the local-replication model is proven.

## Value Envelopes

Values propagated through the system need metadata.

Possible shape:

```go
type SourceKind int

const (
	SourceAuthoritative SourceKind = iota
	SourceCached
	SourceFallback
)

type ValueEnvelope[T any] struct {
	Value           T
	Previous        T
	FunctionName    string
	FunctionVersion int
	SourceKind      SourceKind
	IsPartial       bool
	Err             error
	Version         uint64
}
```

Open questions:

- Whether `Previous` belongs in every envelope or only subscription events.
- Whether errors must be JSON-marshalable.
- Whether `FunctionName` and `FunctionVersion` should be replaced by a general node ID.
- Whether source/provenance metadata should be part of durable cache state.

## Incremental Collections

Decision: maps and sets are first-class incremental collection values.

Reasoning: large collections often change by small deltas, and retransmitting the full collection on every update is too expensive.

Decision: maps are the general primitive.

Decision: sets are a specialization of maps, for example:

```go
map[K]struct{}
map[K]bool
```

Decision: the library must provide a first-class set type for string and integer keys, with upsert and delete operations.

Initial required set shapes:

- `Set[string]`
- `Set[int]` or a defined integer key type where needed

The exact API can remain generic, but these concrete cases must be well-supported and efficient because they match common Tailscale IDs and indexes.

### Map Updates

Map-valued nodes may propagate:

- A full snapshot.
- A versioned delta.

Possible mutation type:

```go
type MapOp int

const (
	MapPut MapOp = iota
	MapDelete
)

type MapMutation[K any, V any] struct {
	Op    MapOp
	Key   K
	Value V
}
```

Possible update envelope:

```go
type MapUpdate[K any, V any] struct {
	Version  uint64
	IsFull   bool
	Snapshot map[K]V
	Deltas   []MapMutation[K, V]
}
```

Semantics:

- Initial subscription usually sends a full snapshot.
- Steady-state updates may send deltas.
- Deltas apply only to a known prior version.
- Missing base versions require resynchronization from a snapshot.
- Full snapshots replace local cached state.
- Updates may be batched.
- Updates may be coalesced.

### Set Updates

Sets use equivalent semantics:

- Add element.
- Remove element.

For API purposes, set mutation should use upsert/delete language:

- `Upsert(k)` means the key is present after the mutation.
- `Delete(k)` means the key is absent after the mutation.

Internally, this can be represented as map put/delete with unit membership values.

### Map Operation Over Sets

Decision: the dataflow library must support a map operation over a set.

Conceptually:

```go
MapSet[K, V](input Set[K], fn SomeFunc[K, V]) Map[K, V]
```

For each item `k` in the input set, the output contains:

```go
k -> SomeFunc(k)
```

Required behavior:

- Adding `k` schedules computation of `SomeFunc(k)`.
- Deleting `k` deletes `k` from the output without recomputing the whole output.
- Updating inputs used by `SomeFunc(k)` should only dirty affected keys when the dependency relationship is known.
- Results should be stored in an incremental map-shaped node.

This is expected to be a central operation for computing per-peer or per-node parts of `MapResponse`.

Open questions:

- Whether `SomeFunc(k)` is a normal function node template, a per-key subgraph, or a specialized collection operator.
- How dependencies from `SomeFunc(k)` to other nodes are declared.
- Whether failures for individual keys produce partial map values.

### Sorted Values Of A Map

Decision: the system should likely provide a node type that produces sorted values from a map node using a configured sort key.

Example use case:

- `MapResponse.Peers` must be sorted by `tailcfg.Node.ID`.

Conceptually:

```go
SortedValues[K, V](input Map[K, V], less func(a, b V) bool) []V
```

or:

```go
SortedValuesByKey[K, V, S](input Map[K, V], sortKey func(V) S) []V
```

Requirements:

- Incremental map changes should not require rebuilding more than necessary.
- The node should preserve stable output ordering.
- It should support cheap delta generation for sorted full and changed peer lists.

Open questions:

- Whether sorted values are represented internally as a persistent ordered collection.
- Whether the first prototype can recompute sorted slices linearly while preserving the API shape for later optimization.
- How to expose sorted deltas versus full sorted snapshots.

### Canonical Collection Identity

Map keys and set elements are compared by canonical JSON bytes.

This allows collection operations even when Go values are not comparable.

### V1 Constraint

Decision: map entry mutation is only `Put(key, wholeValue)` or `Delete(key)`.

Decision: no recursive nested patch language in the first version.

Open questions:

- Whether public APIs expose dedicated set types.
- Whether map values can be partial values.
- Whether collection deltas are persisted in transaction logs or only used in propagation.
- Whether incremental collection support should be implemented in the first prototype or added after full-value propagation works.
- Whether `Map[K,V]` and `Set[K]` are nodes themselves, value types returned by nodes, or both.
- Whether collection mutation APIs live only on transactions or also on collection nodes.
- Whether snapshots expose `github.com/benbjohnson/immutable` types directly or through Recontrol-owned interfaces.

## Persistent Data Structures And Snapshotting

Decision: large map- and struct-shaped values should use persistent immutable data structures where that makes snapshots and diffs cheap.

Candidate library: `github.com/benbjohnson/immutable`, especially `immutable.Map`.

Motivation:

- Many consumers need a stable snapshot of the latest value.
- Large values such as `tailcfg.MapResponse` may have hundreds of thousands of nodes.
- Copying whole Go maps or large structs on every change is too expensive.
- Persistent maps can share unchanged structure between versions.
- Structural sharing may make it possible to identify changed subtrees cheaply.

Design direction:

- Model big "struct" outputs as persistent maps or trees where practical.
- Treat a large struct such as `MapResponse` as a structured collection of independently changing fields.
- Use immutable map snapshots for latest-value reads.
- Preserve enough identity/version metadata to compute deltas without walking the entire structure in the common case.

Open questions:

- Whether upstream `github.com/benbjohnson/immutable` exposes enough internals to compute structural diffs cheaply.
- Whether Recontrol needs a local fork or wrapper that tracks changed paths.
- Whether `MapResponse` should be represented as nested immutable maps internally.
- How to balance typed Go APIs with a persistent map/tree internal representation.
- Whether immutable snapshots are used only for collections or for all node values.

## MapResponse Diffing And Wire Encoding

Decision: cheap `MapResponse` diffing is a core design requirement, not an optimization.

Problem statement:

- Design for maintainability and efficient incremental updates in large tailnets.
- `MapResponse` can contain hundreds of thousands of peer nodes.
- Full recomputation plus full old/new diffing can become linear or quadratic in places.
- The target design must make common updates sub-linear in the size of the tailnet.

Design direction:

- Preserve change information and structural sharing as values flow through the reactive graph, so the subscription watcher can diff two full snapshots cheaply.
- Use map/set deltas for peer membership, display messages, packet filters, and other naturally keyed fields.
- Use sorted-values nodes to produce protocol-required ordering only where needed.
- Generate wire deltas at the subscription/encoding boundary from known dirty paths/keys or cheap persistent-structure diffs.
- Use capability-version-aware encoders at the boundary.

Tailscale wire considerations:

- First response in a stream must be complete.
- Later responses should encode unchanged fields as zero/nil/empty according to each field's historical semantics.
- Some fields require special marshal handling because `omitempty` cannot express non-nil empty slices.
- `Peers`, `PeersChanged`, and `PeersRemoved` have established delta semantics.
- `PeersChangedPatch` may be preferable for supported peer changes.
- `PacketFilters` and `DisplayMessages` already encode map-style patch semantics.

Open questions:

- Exact internal representation for `MapResponse`.
- Whether each `MapResponse` snapshot is a full persistent map-shaped value after every fixed point.
- Whether a separate application-level `MapResponseDelta` helper is useful outside the graph.
- How to choose between `PeersChanged` and `PeersChangedPatch`.
- How to test compatibility across old `CurrentCapabilityVersion` ranges.
- Which existing Tailscale mapresponse encoder code should be reused or adapted.

## Execution Model

Decision: the library must not create unbounded goroutines.

Decision: computation must go through an explicit executor/scheduler.

Requirements:

- Control what gets calculated when.
- Bound concurrency.
- Provide fairness between customers.
- Prevent one busy customer from consuming all local compute.
- Apply more debouncing to very busy customers.

Prototype approach:

- Model work items by customer ID.
- Maintain per-customer queues or dirty sets.
- Use a global scheduler to choose which customer gets compute next.
- Coalesce repeated invalidations for the same customer/node.
- Run with a small bounded worker count.

Open questions:

- Exact scheduler algorithm.
- Whether fairness is round-robin, weighted fair queuing, deficit round-robin, or priority based.
- How debounce windows are computed.
- Whether debounce configuration is per node, per customer, per shard, or global.
- How cancellation and deadlines interact with pure computations.
- Whether function execution receives a context.
- Whether long-running computations are preemptible only by context cancellation.

## Node Definition API

Goals:

- Lightweight node creation.
- Support Go generics.
- Support inline definitions.
- Support reusable typed definitions.
- Require explicit names.
- Require explicit dependencies.
- Compile all forms into the same internal representation.

Decision: static node definitions are acceptable for the first design.

Decision: static definitions should be reusable across many graph instances.

Decision: obvious/lightweight nodes should be definable inline with closures, similar in spirit to `http.HandlerFunc`, without requiring a package-level named type and formal methods.

Decision: node identity can be supplied explicitly by the node definition, likely via an interface. For inline or object-backed nodes, pointer identity may be acceptable where it is stable and intentional.

Potential identity interfaces:

```go
type NodeIdentity interface {
	NodeKey() string
}

type NodeDefinition interface {
	NodeIdentity
	// ...
}
```

Open detail: if pointer identity is allowed, the API must make clear that two separately allocated but otherwise identical inline definitions are distinct nodes.

Decision: the API should be strongly typed with Go generics where possible, while accepting that some validation happens at runtime, such as JSON round-trip validation for keys and values.

Decision: reflection is acceptable for API ergonomics and declaration-time validation.

Requirements for reflection use:

- Reflection should happen when node definitions are declared, bound, or first instantiated.
- Reflection should validate dependency names, field types, JSON compatibility where applicable, and compute function shape.
- Reflection failures should be reported early, preferably before the graph starts processing transactions.
- Reflection should not be on hot recomputation paths after a node definition has been checked and compiled into an internal representation.
- The runtime should cache any adapters, field indexes, and type metadata produced by reflection.
- Reflection-derived metadata should be memoized across declarations and graph instances where the reflected type/function shape is the same.
- Memoization keys should account for dependency struct type, compute function type, output type, and any binding shape needed to preserve safety.

Decision: function nodes should have named dependencies with declared types.

Decision: support both a full typed-struct dependency style and a lightweight inline dependency style if both can compile to the same internal node representation.

Preferred full style: typed struct dependencies. This is the main API direction for serious reusable nodes.

Preferred lightweight style to explore: inline anonymous-struct dependencies. This is intended for small local nodes where defining a package-level dependency struct feels too heavy.

Desired dependency model:

- A node definition declares a set of named dependencies.
- A graph instantiates that node by binding each dependency name to another node.
- The compute function is called once all dependencies are first available.
- The compute function is called again when any dependency changes.
- Dependency type mismatches should be caught at compile time where possible, and otherwise at graph construction time.

Typed struct dependency style:

```go
type PeerInputs struct {
	Node     Dep[NodeState]
	Profile  Dep[UserProfile]
	Settings Dep[TailnetSettings]
}

peerView := Func("peerView", Bind[PeerInputs]{
	Node:     nodeState,
	Profile:  userProfile,
	Settings: tailnetSettings,
}, func(ctx Context, in PeerInputs) Result[PeerView] {
	return OK(computePeerView(in.Node.Value(), in.Profile.Value(), in.Settings.Value()))
})
```

Inline dependency style:

```go
peerView := Func("peerView",
	Deps(struct {
		Node     Node[NodeState]
		Profile  Node[UserProfile]
		Settings Node[TailnetSettings]
	}{
		Node:     nodeState,
		Profile:  userProfile,
		Settings: tailnetSettings,
	}),
	func(ctx Context, in struct {
		Node     NodeState
		Profile  UserProfile
		Settings TailnetSettings
	}) Result[PeerView] {
		return OK(computePeerView(in.Node, in.Profile, in.Settings))
	},
)
```

The exact syntax above is illustrative. The important design intent is:

- Callers can choose a named dependency struct for clarity and reuse.
- Callers can choose an inline struct for lightweight local nodes.
- Both forms preserve named dependencies.
- Both forms compile to the same runtime representation.
- The runtime can still validate JSON compatibility, DAG constraints, and dependency binding.

Possible lower-level builder style, if needed:

```go
n := NewFuncNode[In, Out]("foo").
	DependsOn("node", nodeState).
	DependsOn("profile", userProfile).
	WithVersion(1).
	WithDebounce(debounce).
	Compute(func(ctx Context, in In) Result[Out] {
		// ...
	})
```

Possible type-based style:

```go
type FooNode struct{}

func (FooNode) Definition() NodeDefinition {
	return NewFuncNode[In, Out]("foo").
		DependsOn(a, b).
		WithVersion(1).
		Compute(computeFoo)
}
```

Open questions:

- Exact representation of node IDs.
- How dependencies are typed.
- Exact reflection rules and memoization keys for typed-struct and inline-struct dependency APIs.
- Whether dynamic graph changes are needed later.
- How generic APIs map to untyped internal canonical storage.
- Whether pointer identity is too implicit for durable/restartable node identity.
- Whether inline nodes must still provide stable string keys.

## Node Runtime State

Each node should track:

- Dependencies.
- Subscribers.
- Cached inputs.
- Cached outputs.
- Current value.
- Previous value where needed.
- Version or sequence.
- Dirty state.
- Active upstream subscription handles.
- Active downstream subscribers.

Decision: every function node must internally track all relevant subscriptions.

Open questions:

- Whether downstream subscriber state lives on nodes or in a separate subscription manager.
- How much previous-value history is retained.
- Whether old/new diff support is universal or only subscription-event metadata.

## Fallback Computation

Current status: this section is retained as background, but it is not part of the first prototype direction.

Decision update: the first prototype should assume the owning shard has replicated all data required for local computation. Remote fallback computation should not be a core design dependency.

Decision: computation normally runs on the owning shard.

Decision: another shard may compute a function locally as fallback if:

- The owning shard is unavailable.
- The local shard has the function implementation.
- The local shard has the required inputs cached.

Decision: fallback results must be marked as fallback, not authoritative.

Decision: authoritative remote results replace fallback results when available.

Open questions:

- Whether fallback computation is automatic or opt-in per node.
- Whether fallback computation can recursively depend on other fallback results.
- How partial fallback errors are represented.
- How callers distinguish stale cached values from newly computed fallback values.
- Whether this model should be deleted, simplified, or retained only for disaster recovery.

## Prototype Plan

## Prototype Status

Current implementation status as of 2026-07-26:

- A minimal Go module exists in the repository root.
- Package name is `recontrol`.
- `Result[T]`, `Node[T]`, `Dep[T]`, `Graph`, `Tx`, `Snapshot[T]`, `Event[T]`, and subscription option types exist.
- Scalar data nodes can be created with `Data[T](key)`.
- Function nodes can be created with `Func`.
- Both dependency API directions have an initial prototype:
  - typed dependency input structs using `Dep[T]`
  - inline dependency structs using `Deps(...)`
- Reflection validates dependency declarations early and memoizes reflected shape metadata.
- One graph serializes transactions through `Graph.Update`.
- Transactions can set scalar data nodes with package-level `Set(tx, node, value)`.
- The runtime recomputes function nodes synchronously until a fixed point after each transaction.
- Subscriptions deliver `Event[T]` values containing previous and current snapshots.
- Mutable map and set data nodes exist with copy-on-write prototype snapshots:
  - `MapData[K,V]`
  - `SetData[K]`
  - `MapPut`
  - `MapDelete`
  - `SetUpsert`
  - `SetDelete`
- DAG cycle rejection is implemented.

Current prototype simplifications:

- No executor, debounce, or fairness scheduler yet.
- No durable storage yet.
- No canonical JSON validation yet.
- Map/set snapshots use copy-on-write Go maps, not `github.com/benbjohnson/immutable`.
- No map-over-set operator yet.
- No sorted-values node yet.
- No Tailscale `MapResponse` example yet.
- Subscription options are accepted but not semantically implemented beyond fixed-point-by-transaction behavior.
- Recompute currently scans function nodes until stable rather than using a dirty queue.
- Reflection metadata is memoized, but the compute adapter still uses reflection to populate dependency structs.

Recommended first prototype sequence:

1. Build canonical JSON identity helpers.
2. Build local node registry and DAG validation.
3. Build first-class incremental map and set nodes, including `Set[string]` and integer sets.
4. Build map-over-set operation.
5. Build local data nodes and function nodes.
6. Build local transaction commit with durable storage interface.
7. Build local propagation with explicit executor.
8. Build fixed-point detection for selected output nodes.
9. Build subscriptions with old/new value events and fixed-point-only output callbacks.
10. Add immutable snapshot support for map-shaped nodes.
11. Add sorted-values-of-map node.
12. Add basic `MapResponse` internal representation and delta tracking.
13. Add single-process multi-shard routing with local replicated inputs.
14. Add debouncing and basic fairness across customers.
15. Add Tailscale `MapResponse` initial-complete and subsequent-delta encoder prototype.

The initial target should be a single-process, multi-shard simulation that can be tested deterministically.

## Testing Strategy

The prototype should include tests for:

- Canonical JSON equality across non-comparable values.
- DAG cycle rejection.
- Atomic multi-cell transaction commit.
- Durable commit before propagation.
- Function recomputation after data changes.
- Debouncing that skips intermediate values but converges to final state.
- Old/new subscription events.
- Subscribe and unsubscribe lifecycle.
- Replicated-input cache usage during local recomputation.
- Out-of-order update handling.
- Map/set snapshot and delta application.
- Set upsert/delete behavior for string and integer keys.
- Map-over-set incremental behavior.
- Sorted-values ordering and incremental invalidation.
- Fairness across customers.
- Partial result propagation.
- Fixed-point-only callback behavior.
- Complete first `MapResponse` emission.
- Subsequent `MapResponse` delta emission.
- Sub-linear dirty-key propagation for large peer maps.

Open questions:

- Whether tests should use fake time for scheduler/debounce behavior.
- Whether durability tests use temp files or an in-memory recorder.
- Whether the prototype should expose deterministic executor stepping for tests.
- Whether large-tailnet benchmarks should be part of the prototype from the start.

## Current Open Questions

### API

- What is the exact public package structure?
- What are the public node ID types?
- Are function/node names scoped globally within a graph definition, package scoped, or scoped by an explicit namespace?
- Are dependencies supplied as typed fields, positional inputs, or context lookups?
- Should named dependencies be represented by a struct type, builder calls, or both?
- How does one reusable static node definition get instantiated across many graph instances?
- When is pointer identity acceptable, and when is an explicit stable key required?
- What is the public API for set upsert/delete?
- What is the public API for map-over-set?
- What is the public API for sorted map values?
- What is the shape of `Result[T]`, and does it include structured status metadata instead of `error`?

### Encoding

- What canonical JSON implementation should be used?
- How are unsupported JSON values reported?
- Are NaN and infinity rejected explicitly?
- Are map key ordering and numeric normalization guaranteed by the chosen encoder?
- Does the durable log store canonical bytes, original typed JSON, or both?

### Persistence

- What is the storage interface?
- Is the first durable backend an append log?
- How are snapshots/checkpoints handled?
- Are replicated-input caches persisted?
- How is recovery sequenced relative to subscription restart?

### Scheduling

- What scheduler algorithm provides the right fairness?
- How are debounce durations configured and adapted?
- How is bounded concurrency configured?
- How are long-running computations cancelled?
- Does the executor expose manual stepping for tests?
- How are fixed points detected efficiently?
- How is outbound `MapResponse` delta encoding coalesced?

### Subscriptions

- Are subscription events delivered by callback, channel, or pull API?
- Is unsubscribe synchronous?
- How is backpressure handled?
- Can subscribers request snapshots?
- How are out-of-order events represented and reconciled?
- Are fixed-point-only callbacks implemented as subscription options?

### Partial Results And Errors

- What is the structured error type?
- Can partial results be cached?
- Can partial results be used as dependencies?
- How does a downstream function know whether an input is partial?
- Are errors part of the value identity or only metadata?

### Incremental Collections

- Are map/set deltas part of the first prototype?
- Should the public API expose sets directly?
- Are collection versions per node or per collection value?
- Are deltas persisted or only propagated?
- Can function nodes emit deltas directly, or only full values at first?
- Can persistent immutable maps provide cheap enough changed-path information?
- Does Recontrol need a fork or wrapper around `github.com/benbjohnson/immutable`?

### Tailscale MapResponse

- Should the internal representation be `map[string]any`, an immutable map/tree, or a custom typed structure?
- How are `tailcfg.MapResponse` field-level deltas represented internally?
- Which fields need custom zero-value/empty-slice marshal support?
- How should client `CapabilityVersion` select output behavior?
- How should `PeersChangedPatch` be generated and selected?
- How should `PacketFilters` and `DisplayMessages` patch semantics map to Recontrol map deltas?
- How does the subscription watcher cheaply diff two full `MapResponse` snapshots?
- What benchmarks define acceptable sub-linear behavior?

### Distribution

- What is the eventual transport abstraction?
- Who owns shard discovery?
- How are routing changes deployed?
- How do subscriptions survive customer migration?
- How does authentication/authorization fit if this becomes networked?
- Does the system need remote subscriptions at all if shared state is replicated locally?

## Decisions To Revisit

These decisions are good enough for prototyping but may need revision:

- Canonical JSON as the universal identity mechanism.
- JSON as the only serialization requirement.
- Function values are not durably persisted.
- Maps and sets are the only incremental collections in scope.
- No nested patch language in v1.
- Single-process multi-shard simulation before real networking.
- Fallback computation is allowed when cached inputs are available. This is now deprioritized and may be removed from the first prototype.
- The internal representation may differ from `tailcfg.MapResponse` as long as the encoder preserves Tailscale wire semantics.

## Update Policy

This document should remain current as the design evolves.

When a question is answered:

- Move it from an open-question list into the relevant design section.
- Record the decision clearly.
- Keep enough context to understand why the decision was made.

When prototype behavior differs from this document:

- Update this document first or in the same change.
- Prefer explicit "prototype behavior" notes over leaving ambiguity.

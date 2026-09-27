# Reco Design

Status: living design document  
Last updated: 2026-09-27

This document is the working source of truth for the Reco prototype. It should be updated whenever the design, prototype scope, APIs, or unresolved questions change.

Status convention: **implemented/current** describes the Go API today;
**target/proposed/deferred** describes future distributed, durable, or scheduled
behavior. Historical sketches are not additional supported APIs. The current
prototype is synchronous and in-process, with no durability or remote instances.

## Current Discussion: Core Primitives And Remote Watches

Direction as of 2026-09-26: set aside the control-server application and focus
on the generic library's primitive data types, mutation delta messages, and
cross-machine subscription watching.

The design-first discussion is now followed by a concrete implementation step:
build `cmd/webdemo` as a single-process educational playground. Implement the
local primitives it needs; peer transport and Locators remain deferred.
The earlier exclusion of real networking from the first prototype describes
the previous scope; remote watches are now a target for the next phase.
Nonlocal data and function nodes are resolved through a Locator and watched
over a bidirectional protocol. Exact Locator APIs, transport, and stream
recovery semantics remain undecided; WebSocket is a candidate, not a decision.

Discussion TODOs:

- [ ] Agree on the primitive value types and each type's mutation operations.
  - [x] Sets and maps support atomic clear-all, remove-items, add/put-items
    batches, applied in that order, for both local and remote subscribers.
  - [x] Include a built-in union of two or more sets.
  - [x] Include a key-preserving set-to-map operator with `F: K -> V`.
- [ ] Define snapshot and delta message semantics, including identity, versions,
  ordering, and atomic mutation batches.
- [ ] Define remote watch setup, initial state, updates, and unsubscribe.
  - [x] Route generic node instance keys through a user-supplied Locator;
    subscribe to remote data nodes and function outputs when nonlocal.
  - [x] Shard the application by customer/TailnetID, including when instance
    keys are tuples containing that partition identity.
  - [ ] Define the Locator's local/remote result and generic transport contract.
- [ ] Decide how reconnects, missing deltas, slow subscribers, and cached state
  should behave.
- [ ] Turn the agreed decisions into an implementation and test TODO list.

Preserve the existing requirement that small mutations must not require scans
or copies of entire collections. Checked discussion items record agreed semantics;
the local Go APIs are implemented as described below. Remote APIs, wire encoding,
and transport remain undecided.

Implementation TODOs (webdemo/local primitives now in scope; remote work deferred):

- [ ] Implement atomic set/map delta application and notification, including
  clear-all and replacement, with matching local and remote semantics.
  - [x] Local `SetDelta` and `ApplySetDelta`: clear/remove/add within one Tx.
  - [x] Public snapshot `WithDelta` for set/map batches, including `MapDelta`.
  - [ ] Transaction-level map batch helper and a compact clear marker on events.
- [x] Implement incremental union with overlap tracking across input sets.
- [x] Add incremental intersection, XOR, difference, map-key sets, and distinct
  comparable map-value sets in `reco/nodes`.
- [x] Add typed struct snapshots/deltas, nested set/map changes, composable
  change batches, atomic struct snapshot/watch, and `nodes.Struct` assembly.
- [x] Implement incremental set-to-map evaluation: add `k -> F(k)` for new
  members and remove the same key for deleted members.
- [ ] Test overlapping union membership, atomic replacements, remove-plus-add
  of the same key, and set-to-map additions, removals, and value recomputation.
  - [x] Cover overlap, atomic batches, diamond dependencies, randomized edits,
    snapshot replacement, rollback, and graph-local operator state.
  - [ ] Per-key declared dependencies and value recomputation remain future work.
- [ ] Implement generic keyed node resolution through the Locator for both
  data and function nodes, separating definition identity from instance keys.
- [ ] Implement bidirectional remote query/watch/unsubscribe and incremental
  state application using the agreed atomic delta semantics.
- [ ] Define and implement remote watch lifetime management, including shared
  dependencies, cancellation, and transport cleanup.
- [ ] Test multi-process resolution with tuple keys, remote data and function
  watches, unsubscribe, and the agreed reconnect/resync behavior.

### Reco-backed public test control server (implemented)

`recotestcontrol` is an experimental replacement for the public Tailscale
`tstest/integration/testcontrol` server, exercised through an `experiment.reco`
build-tag alias in a separate Tailscale checkout. It uses no private production
implementation. See [recotestcontrol/README.md](recotestcontrol/README.md) for
provenance, the graph, test commands, and limitations.

Each server owns one fixed graph: persistent node/profile map leaves and an
immutable configuration leaf feed a packet-policy operator and `nodes.Struct`
map metadata. HTTP streams use `SubscribeStruct` and coalesce with
`StructChanges.Then`; the encoder consumes `MapFieldChanges`, not deep snapshot
comparisons. Streams own subscriptions and unregister on exit; connections do
not allocate permanent graph node definitions. Normal lite updates return only
an HTTP acknowledgement and cause delta-sized peer/self updates on subscribers.
Packet policy ignores endpoint/disco changes, including with an explicit
signed-address allowlist. Raw response/ping FIFO queues and session replacement
remain outside the dataflow graph.

This is deliberately test-server easy mode: configuration setters still cause
full refreshes, changed peers are whole node records rather than field patches,
and a stream's pending change count is bounded but not its byte size. Existing
core APIs were sufficient; no application-specific machinery was added to reco.
The next possible application helpers are incremental per-node override joins
and finer-grained peer-patch encoding, not a production control implementation.

### Node Class Names

`reco.NodeClassName` is a defined string type naming a reusable node definition
(a node class). All node constructors take it; `Node.ClassName()` returns it,
replacing the former `Node.Key()` accessor. Collection keys and future generic instance keys are
separate concepts and retain their own types.

Class names are nonempty, exact, case-sensitive strings. There is no trimming,
normalization, or special meaning for separators and punctuation, and no required
namespace syntax. A zero node handle returns the empty class name.
Names are unique among all definitions registered in a graph, including recursive
dependencies, regardless of value type or constructor. Registering the same
definition again is allowed; distinct definitions with equal names are rejected,
not merged. Independent graphs can reuse both names and definitions.

This names the definition side of future class-plus-instance-key addressing;
it does not implement keyed instances or decide distributed namespacing,
function versioning, or the remote addressing format.

### Discussion: Live MapResponseMeta And Per-Stream Encoding

User's proposed application model (2026-09-27): maintain each recipient's final
MapResponseMeta (MRM) as live immutable graph state. A subscriber translates
its changes into the initial Tailscale MapResponse and subsequent wire deltas.
See [notes/map-protocol.md](notes/map-protocol.md) for the public-source protocol
report. This is a requirements exercise for generic reco primitives, not a
decision to resume control-server coding.

The single-process model now has StructSnapshot, nodes.Struct, SubscribeStruct,
and StructChanges.Then, in addition to Operator/Func, persistent snapshots,
ValueEqualer, and fixed-point events. Stream lifecycle and protocol adapter code
remain application-owned. MRM should hold materialized semantic
state, not a tailcfg.MapResponse whose omitted fields mean "retain old state".
Peers naturally form a map keyed by numeric NodeID; filters and display messages
are named maps. Initial encoding may enumerate/sort; steady-state encoding
should inspect only changed fields/entries and sort only changed peer IDs.

Progress and remaining follow-up work:

- [x] Add atomic initial snapshot/watch for typed records, including MRM, with
  SubscribeStruct. Callbacks can queue changes during initial encoding; the
  returned Snapshot.Valid reports whether an initial value exists.
- [ ] Generalize snapshot/watch beyond maps/structs; expose Result status and
  define application readiness separately from initialization.
- [x] Add typed composite snapshots that honor field equality hooks and expose
  field-aware changes without requiring map[string]any or a recursive JSON patch.
- [ ] Provide composable set/map deltas and bounded per-subscriber accumulation.
  Preserve the first Before and latest After per touched key, explicit presence,
  reset/clear semantics, and atomic cross-field batches. ChangesSince only has
  a fast path for identical roots or an adjacent retained delta base; discarding
  intervening changes can force O(N) reconciliation. Coalesce changes, not just
  latest snapshot pointers. An asynchronous executor will need this too.
  - [x] StructChanges.Then composes scalar and nested map/set net changes using
    persistent touched-key storage. It validates endpoint roots/versions.
  - [ ] Automatic bounded queues, compact clear markers, and async scheduling
    remain deferred; ChangeCount helps callers bound pending entry counts.
- [ ] Define safe slow-subscriber behavior and separate pending from in-flight
  changes. Current callbacks hold the graph lock, so encoding, compression, and
  network writes belong outside callbacks. Bounded queues must either preserve
  the transition or explicitly resync/terminate, never silently drop deltas.
- [ ] Expose Result error/partial status to watchers. Snapshot/Event currently
  expose value/version/validity, not that status; a local fixed point does not
  imply a complete, safe initial configuration. Application readiness may impose
  further requirements such as a self node and initialized policy inputs.
- [ ] Add incremental map restriction by a key set, entry-value mapping,
  lookups, and indexed joins where justified. Proposed names such as RestrictMap
  and MapEntries are placeholders. MapSet only computes F(k) on additions and
  cannot react to an existing peer's changing record. These operators can start
  as external implementations using Operator/Input/ChangesSince/WithDelta.
- [ ] Design keyed instances, dynamic per-key dependency tracking, indexes, and
  instance lifetime management. Whole-map dependencies currently dirty every
  consumer of that map; a helper that filters its delta does not itself make
  graph invalidation key-selective. Keep this separate from the first local
  MRM-to-stream adapter and from future Locator/peer transport APIs.
- [ ] Test end-to-end steady-state work, not just final delta sizes: nested
  equality, existing-peer edits with unchanged visibility, coalesced batches,
  mutation during initial encoding, slow writers, and first-ready state.
  - [x] Struct tests count hashes/equality work, exact node computations, and
    actual storage sharing at up to 100,000 entries; cover coalesced changes,
    atomic snapshot/watch races, initial encoding overlap, and uninitialized inputs.
  - [ ] Real protocol encoder/client compatibility and transport backpressure
    integration tests await the control-server implementation.

Protocol-specific responsibilities remain outside reco: explicit omitted vs
empty/null/false encoding, client capabilities, peer patch eligibility and
client quirks, sorted peer lists, framing/compression, and per-stream baseline.
Some transitions cannot be represented by an ordinary in-stream field update
(for example clearing Domain); the adapter needs an explicit fallback policy.
UserProfiles has upserts but no wire deletion. Peer removals require IDs, not
an empty Peers slice. Reco's generic clear marker does not imply a compact wire
clear exists for every protocol field. Test encoders against the client state
accumulator, including its normalization and peer patch conversion behavior.

The examined client starts a fresh map session on each poll, so reconnect must
send an initial snapshot, not resume from reco's graph-local Version. Keepalive,
ping/debug/browser commands, and similar events need stream/event handling;
equal-value suppression and state coalescing are not event-delivery guarantees.

Efficiency target: work proportional to changed data and affected recipients,
plus persistent-storage paths and protocol-required changed-entry sorting.
All-to-all visibility inherently entails N deliveries per peer update; with N
updating peers the output traffic can be quadratic. Avoid additional scans of
all peers per recipient and unnecessary invalidation of unaffected recipients.

### Typed Struct Values And Deltas

Implemented (2026-09-27): StructSnapshot[T] uses a normal Go struct as a fixed,
typed schema. All fields must be exported and non-embedded. NewStruct(value)
wraps a record without enumerating its collection contents; Value returns a
typed shallow copy. Referenced objects remain immutable by caller contract.
Zero snapshots contain T's zero value. There is no dynamic field creation or
deletion, and no public string/any setter.

Field[T, V]("FieldName") validates and caches a typed field handle at declaration
time. Names must exist and types match V exactly. Field.Get reads a value;
Field.Set and Field.Update create edits for StructDelta[T], an ordered batch.
StructSnapshot.WithDelta applies the batch atomically, with later edits seeing
earlier values. StructData and ApplyStructDelta integrate record mutations with
Tx; repeated helper calls retain the original transaction base. Net-zero edits
reuse old roots and suppress downstream computation/notifications.

StructChanges[T] describes the net changes between snapshots. Its Fields
iterator yields changed names in schema order; a typed field's Change returns
before/after values plus an explicit changed flag, preserving nil/zero/false.
MapFieldChanges and SetFieldChanges return typed per-entry mutations for direct
map/set fields, not merely "Peers changed". Clear/replacement inputs currently
normalize to point changes; clear can enumerate removed entries. Other field
types, including interface fields and nested record fields, use whole-value
replacement. Map-entry values are also replaced whole, not recursively patched.

Equality respects each field's ValueEqualer hook, so unchanged collections
compare root identities. Record overhead is a fixed-width copy/field walk;
scalar comparisons still have their own equality costs. Reflected schema
metadata is cached, but field access/assembly still uses reflection. Generated
field helpers remain a possible ergonomic/performance refinement.

ChangesSince uses retained record metadata or per-field differences. Collection
inputs with matching roots or adjacent delta bases avoid full scans. Arbitrary
replacements/skipped bases may need enumeration. Stream consumers should capture
each event and compose with StructChanges.Then rather than retaining only the
latest snapshot. Then validates endpoint root identity and observed version,
preserves the first Before/latest After, and stores pending map/set mutations in
persistent HAMTs. It visits incoming changed entries, not accumulated map contents.
Cancelled entries are removed; earlier published batches remain immutable. Only
endpoint snapshots and net changes are retained, not a linked event history.
ChangeCount counts pending scalar replacements/collection entries in O(fields),
useful for caller-enforced limits but not an exact byte/memory bound.

SubscribeStruct atomically returns an initial snapshot and installs callbacks
for settled StructEvents. Callbacks may start before setup returns and execute
under the graph lock; prepare queues/accumulators first and encode/write outside
callbacks. An invalid initial snapshot means use the first event as initial
state, even if its net changes are empty. Callers own queue limits, in-flight
batch separation, synchronization, and slow-writer resync/disconnect policy.
Result error/partial metadata is not yet exposed by Snapshot/StructEvent.

nodes.Struct[T](className, inputBindings) binds each record field to a Node of
the corresponding type. The binding struct must match field names/order/types
exactly. It assembles the record at a transaction fixed point without copying
collection contents; it runs only after all inputs initialize. Like an inline
Func, it assembles values rather than propagating input error/partial metadata;
application-specific readiness policies can use Operator. Core owns record
values/deltas/watch; nodes owns the convenience assembly operator.

Runnable godoc examples demonstrate atomic record editing and an MRM-like
initial-snapshot/stream-delta boundary. Tests cover field validation, explicit
nil/zero values, repeated/atomic/rolled-back edits, exact net changes, random
coalesced updates, stale/gapped batches, graph isolation, and snapshot/watch races.
Work-count and structural-sharing tests use up to 100,000 map/set members and
verify no collection work for scalar edits. BenchmarkStructDelta varies total
collection size (1,000/100,000) and delta size (1/16).

### Single-Process Webdemo

`cmd/webdemo` is a teaching and experimentation surface for the library, not
the control-server application. It embeds its HTML/CSS/JS and serves one shared
in-memory graph to all connected browsers. No peer connections or Locators.

Current graph:

- Editable word sets `a` and `b` feed `Union(a, b)`.
- `MapSet(union, F)` preserves each word key and computes uppercase/rune count.
- A demo-local incremental operator sums the map entries' rune counts into
  `totalRunes`. It subtracts old/removed counts and adds new counts from map
  deltas, without scanning unchanged entries. Another function multiplies this
  total by the editable points-per-rune weight; the final summary also depends
  on an editable label. Rune counts are Unicode code points, not bytes. Replacing
  words can change the score even when the number of unique words is unchanged.
- Set add/remove/clear/replace and scalar edits commit through reco transactions.
- A sparse dependency-ordered queue evaluates each dirty function at most once, then
  subscriptions publish the fixed-point result. Operator caches belong to graph
  instances, not reusable definitions. Rate limiting/debouncing is not built.

The browser protocol is deliberately an implementation detail, not a commitment
for the future peer protocol. Initial connection and reconnect receive a full
snapshot. Subsequent messages batch changed-node scalar values and set/map
touched-key deltas after each transaction. They do not resend whole collections.
The browser applies the whole batch before rendering, highlights changed values,
and exposes the last raw message plus a bounded activity log. Its rendering may
rebuild the small changed collection's DOM; incremental DOM rendering is separate
from incremental wire updates. Demo sets are limited to 64 words each.

Snapshot registration is serialized with updates. Each connection has a bounded
send queue; overflow disconnects it rather than dropping deltas. Reconnect uses
a fresh snapshot, including after server restart. Browser edits are disabled
while disconnected and are not replayed. Restart currently resets the graph;
this demonstrates connection recovery, not durable or distributed recovery.

Transaction UI: default mode auto-commits each edit. Start Tx stages an ordered
batch locally and marks leaf previews as drafts; computed nodes continue showing
committed state. Rollback Tx discards drafts. Commit Tx sends one bounded batch
that runs inside a single `Graph.Update`; any invalid edit rolls back the entire
batch. Other tabs only see the final settled delta batch. Commit applies the
staged operations to the latest server state, not a long-lived isolated snapshot.
No graph lock is held while a user edits. A rejected batch remains available to
roll back; disconnect discards drafts and never replays an uncertain commit.

Dependency names in each node footer are hoverable and keyboard-focusable.
Highlight the referenced node and exactly its edge to the consumer. On narrow
screens where edges are hidden, clicking a dependency scrolls to its source box.

### Efficiency Regression Coverage

- Data and generic function nodes suppress identical scalar writes; both typed
  and inline function adapters compute once for relevant changed inputs and
  stop propagation when their output is unchanged.
- `SetData`, `MapData`, and all `reco/nodes` collection operators use immutable
  HAMTs. Contributor counts also use `github.com/benbjohnson/immutable.Map`. Point
  updates copy paths, not whole backing stores. Transaction deltas normalize
  repeated touched keys, and net-zero batches reuse the previous root.
- Tests count actual `F(k)` invocations and hash/equality operations for small
  deltas in large collections. Separate white-box tests inspect HAMT branch
  reuse, catching full rebuilds that produce correct values but copy O(N) data.
- Graph propagation queues only affected consumers using precomputed topological
  ranks; old values and notifications are tracked only for changed nodes. No
  whole-registry copy or subscription scan on point updates.
- Benchmarks vary collection sizes (1,000 / 100,000), delta sizes (1 / 16), and
  unrelated graph sizes (10 / 1,000 / 100,000), reporting time and allocations.
  Run `go test ./reco -run '^$' -bench 'Delta' -benchmem`.
- The guarantee is touched-key work plus HAMT traversal and affected dependency
  edges, not constant total work for every possible mutation. Initial snapshots,
  arbitrary snapshot replacement, and clearing many memberships can require
  enumeration. A general user-supplied `Func` is invoked minimally but its own
  code remains responsible for incremental work inside that invocation.

### User-Defined Incremental Operators

Implemented (2026-09-27): callers can implement incremental operators in their
own packages, without adding private cases to reco. The `reco/nodes` package
provides Union, Intersection, Xor, Difference, MapSet, MapKeys, and MapValues,
using the same exported APIs:

- `Operator[T](className, []Dependency, func() Compute[T])` declares an operator.
  Its factory creates independent mutable caches for each graph instance.
  `Node[T]` implements `Dependency`; runtime node handles remain library-owned.
- `Input(eval, node)` reads a declared dependency as `Dep[T]`, including version,
  validity, error, and partial-result metadata. Heterogeneous inputs and
  variable-length dependency lists are supported. Undeclared inputs panic.
  The evaluation handle is named `Eval` to distinguish it from the standard
  library's `context.Context`; it does not carry cancellation or deadlines.
- Zero set/map snapshots are empty. `WithDelta` creates a new immutable snapshot
  with a clear/remove/add-or-put batch. It copies only touched HAMT paths for
  point edits, normalizes touched keys, and reuses the prior root for net-zero
  edits. Custom values stored inside a snapshot must also be immutable.
- `ChangesSince(previous)` uses persistent-root lineage to distinguish unchanged
  inputs from new deltas. An adjacent delta costs O(delta), not O(collection).
  Initial state, skipped snapshots, and arbitrary replacements may require O(N)
  reconciliation; graph-local versions alone cannot establish lineage.
- Build one output `WithDelta` batch per evaluation to preserve the fast path.
  Chained output batches remain correct, but consumers such as `SubscribeMap`
  may need reconciliation to include all changes since the previous output.
  `Changes()` describes a snapshot's own last delta, not what any particular
  consumer has missed; in particular, a no-op preserves that old metadata.
- Custom immutable value types can implement `ValueEqualer` and `VersionedValue`
  for fast equality and immutable graph-version stamping. Values without these
  optional hooks use ordinary comparison and the outer `Snapshot.Version`.

The runtime still owns scheduling, graph-local instances, dependency readiness,
fixed-point propagation, and subscriptions. Operators compute only from declared
inputs and must not perform external side effects, retain an Eval,
re-enter their graph, or mutate published snapshots. Definitions are reusable
across concurrent graphs, so mutable caches belong inside the factory. Resource
and remote-watch lifecycles are not part of this API yet.

External-package union/map implementations run the same conformance tests as the
built-ins: computation counts, overlapping inputs, atomic batches, random edits,
replacements, concurrent graph isolation, and actual HAMT structural sharing.
The public Operator godoc includes a runnable external-package map example.

Package boundary (2026-09-27): `reco` owns the graph runtime, extension API,
primitive collection values/deltas, transactions, and subscriptions.
`reco/nodes` provides supported reusable derived-node algorithms. It imports
`reco`, never the reverse, and has no privileged runtime access. Union and
MapSet live there without forwarding wrappers in `reco`, along with Intersection,
Xor, Difference, MapKeys, and MapValues. Future reusable
filtering, join, and sorted-view nodes belong there too;
application-specific examples remain with their applications.

Future webdemo TODOs (requested, not needed in this first version):

- [ ] Independent server-side execution rate controls: let nodes be dirtied
  repeatedly before recomputing, simulate load, and show coalescing/Nagle-like
  scheduling. The present executor is synchronous per transaction.
- [ ] Independent outbound delivery and client-side apply/render rate controls;
  distinguish computation coalescing, wire coalescing, and rendering delay.
- [ ] Visualize dirty/pending/computing/settled states and count input mutations
  versus recomputations, emitted patches, and renders.
- [ ] Rich per-message delta visualization and byte/key counts showing minimal
  patches rather than full O(N) sets/maps. Preserve the existing touched-key
  path and raw wire inspector when adding scheduling controls.
- [ ] Coalesce collection patches by key while retaining ordering, base versions,
  and atomic replacement semantics. Never silently discard unapplied deltas.
- [ ] Multi-process operation and demonstrations of ownership/peer recovery later.

## Overview

Reco is intended to be a Go library for distributed reactive data binding.

The library should be generic enough for non-Tailscale applications. The first real application to keep in mind is a new Tailscale control/coordination server designed for easier maintenance and efficient scaling to large networks. In that environment, the main computed output is the rich, deep `tailcfg.MapResponse` value that a control server streams to clients. `MapResponse` includes the local node, peers, DNS configuration, packet filters, health/display messages, and other client-visible control-plane state.

The programming model is similar to a spreadsheet:

- Some nodes are mutable data cells.
- Some nodes are pure function cells.
- Function nodes depend on other nodes.
- When data changes, affected computations are scheduled and recomputed.
- Updates propagate through the dependency graph until the system reaches a fixed point.

Unlike a local spreadsheet, Reco is designed for a distributed system where data and computation are partitioned across machines by customer. Normal node resolution includes local and remote nodes. An in-process multi-shard simulation is useful for deterministic tests, but cross-machine watches are part of the current target.

For the Tailscale use case, some `MapResponse` values may involve hundreds of thousands of nodes. The design must therefore avoid full recomputation and full diffing work that is linear or quadratic in the whole tailnet whenever only a small part of the input changes.

## Core Goals

- Provide a Go library, not a standalone service.
- Support generic key and value types.
- Target JSON-marshalable remote keys/values and canonical wire identity;
  the local API does not currently require or validate JSON encoding.
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

## Non-Goals For The Current Phase

- Production distributed consensus.
- Cross-customer atomic transactions.
- Cross-shard atomic transactions.
- Durable persistence of derived function values.
- Recursive nested patch semantics for collection values.
- A complete production scheduler.
- Automatic load balancing across shards.

The current phase focuses on core primitives, atomic mutation deltas, and local/remote subscriptions. The control-server application is on hold. Earlier plans excluding networking and remote function subscriptions are superseded.

## Architecture Summary

The system consists of:

- Shards that own customer partitions.
- Data nodes that hold mutable durable values.
- Function nodes that hold pure computations over dependencies.
- A dependency graph that must be a DAG.
- An executor that schedules recomputation explicitly.
- A subscription layer for local and remote data values and function outputs.
- A persistence interface for committed data transactions.
- A user-supplied Locator mapping generic instance keys to local ownership or
  a generic transport for reaching the remote owner.

For the prototype, multiple shards can run in one process and communicate through Go interfaces. That keeps distributed semantics testable without committing to a transport.

Current direction: resolve each data or function instance using its key. A
local instance is served locally; a nonlocal instance is subscribed to through
the Locator-selected transport. Remote function outputs are computed by their
owner. The runtime handles routing and watches, keeping network side effects
out of pure computation functions. Cached remote dependencies can feed local
computation; pre-replicating every input is not a prerequisite for resolution.

## Primary Use Case: Tailscale MapResponse

The main known workload is computing and streaming `tailcfg.MapResponse` values for Tailscale clients.

Decision: `MapResponse` is not a special library primitive. It should be implemented by the Tailscale application as a normal Reco node.

Decision: the Tailscale application may compute an internal map-shaped representation, roughly `map[string]any`, where each top-level `MapResponse` field is a map key for the initial full response shape.

Decision: Reco should not compute Tailscale wire deltas as graph nodes.
Incremental collection subscriptions should preserve the exact changed keys so
the watcher can encode a wire delta without scanning or diffing full snapshots.

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

Implications for Reco:

- The computed `MapResponse` node must know when it has reached its first fixed point so the server can serialize the initial complete response.
- After the first complete response, the system should prefer protocol deltas over complete responses.
- It is still desirable to invoke outbound callbacks only at fixed points, not for every intermediate recomputation.
- If many collection mutations are produced in a wave, the wire encoder should
  coalesce them by key before producing deltas.
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

Decision: Reco should be a generic library that non-Tailscale applications can use.

Decision: Tailscale's control server is the first real application driving the design, performance requirements, and API validation.

Decision (2026-09-27): the module is `github.com/bradfitz/reco`, with package
`reco` at the repository root and reusable operators in `nodes` (import
`github.com/bradfitz/reco/nodes`). Reco stands for reactive computation.
The control-server example remains in `cmd/recontrol` as package `main`;
the educational demo remains in `cmd/webdemo`.

Public-source application research lives under `notes/`. Private-source research
and local session transcripts remain untracked and ignored, not publication
material.

Implication: the public API should not expose Tailscale-specific types, but it must be strong enough to model Tailscale's `MapResponse` workload without special cases in the library.

### Go And Types

Decision: the library will use Go generics in the public API where useful.

Current API: scalar node values and map values may be any Go type, subject to
published-value immutability. Collection keys and set elements must be Go
`comparable`; `MapValues` also requires comparable map values. Interface-typed
keys/values used as set members must be dynamically comparable. Node definitions
are named by `NodeClassName`, not by a generic collection or instance key.

Deferred target: JSON-marshalable remote keys/values and canonical identity could
support non-comparable instance or collection keys. This is not implemented and
does not relax today's Go type constraints. The encoder and compatibility with
local equality semantics remain open.

### Canonical Identity

Deferred proposal, not current behavior: encode remotely addressed keys/values
into a canonical byte representation. Possible uses include:

- Equality.
- Hashing.
- Map/set membership.
- Cache keys.
- Subscription identity.
- Delta application.
- Durable log identity.

Current behavior: collection membership uses Go equality and HAMT hashing;
scalar values use `ValueEqualer` when provided, otherwise `reflect.DeepEqual`.
Map entry equality uses `ValueEqualer`, otherwise Go equality for comparable
values and `reflect.DeepEqual` for non-comparable values. Class names use exact
string equality. No canonical JSON encoding or validation occurs locally.

### Graph Model

Current API: a Graph is an independent runtime instance. The motivating control
application plans to use one graph per partition; the generic API imposes no
customer/tailnet semantics.

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

Deferred durability target: persist data node commits before propagation.
Current `Graph.Update` commits in memory and propagates synchronously; it does
not persist data or survive process restart.

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

Deferred target: remotely addressable function definitions should have explicit
identity including a class name and version. Current constructors take only a
`NodeClassName`; there is no function-version API.

Potential identity:

```go
type FunctionID struct {
	ClassName NodeClassName
	Version   int
}
```

Decision: versions are independent. There is no implicit cache invalidation; new versions are distinct functions.

Current class names are unique within each graph, including dependencies.
Open question: how distributed registration, namespaces, and function versions
extend that local contract.

## Sharding Model

The keyspace is partitioned by customer ID or an equivalent partition identity.
In the Tailscale application this is `TailnetID`, the unit of sharding.

Typical node instance keys are:

- `TailnetID`.
- `(TailnetID, NodeID)`.
- `(TailnetID, UserID)`.

These application keys always include the TailnetID, so instances belonging to
one customer route together regardless of the rest of the key. The library
must keep keys generic and must not bake in Tailscale types or tuple layouts.

Each customer partition is owned by one shard at a time.

Decision: shard ownership and remote resolution use a user-supplied Locator
interface. Conceptually it maps a node's generic instance key to local
ownership or a generic network transport for speaking to the owning process.
The exact Go signature is not yet decided; the earlier string-only
`ShardMapper` sketch is insufficient for this contract.

Requirements:

- Consistent ownership decisions across participants for a given routing view.
- Supplied by the user.
- Used by normal resolution of both leaf data nodes and function nodes.
- Supports generic keys containing the application's partition identity.
- Does not require the library to know concrete network addresses or commit
  to WebSocket as its transport.

The partition key chooses the owner, not the node definition. Multiple data
and function definitions may have instances with the same key. Remote node
addressing therefore needs a portable definition identity (including version
for functions) as well as the canonical instance key. Resolving a remote
function means watching a known function instance on the owner, not shipping
Go closures to another process.

Decision: shards may differ in compute capability, but this is acceptable because load is expected to be proportional to the number of customers assigned to each shard.

Open questions:

- How shard membership is represented.
- How the typed Locator obtains partition identity from each generic key type.
- Whether the Locator returns an endpoint, a dialable transport, or an existing
  session, and how local ownership is represented.
- How routing changes are rolled out.
- Whether the library owns shard discovery or receives a complete routing table from the host application.
- What happens to in-flight subscriptions during customer migration.

## Transactions

Current API: `Graph.Update(func(*Tx) error)` serializes updates with a graph
mutex. Returning an error rolls back staged leaf mutations without recomputing
or notifying. A successful update commits all staged mutations in memory,
recomputes affected nodes to a fixed point, and delivers callbacks before
returning. Mutation helpers compose against earlier staged writes in the same
Tx. There is no public transactional read/CAS API or durable commit yet.

Decision: a transaction may update multiple data cells atomically.

Decision: transactions are serialized per graph. Only one transaction runs at a time for a given customer/tailnet graph.

Transaction scope:

- One graph.
- One customer/tailnet.
- One owning shard/process instance.
- Local only.

Guarantees:

- Atomic.
- Durable before commit returns (deferred target, not current behavior).
- Propagation begins after commit.

Non-goals:

- No multi-customer transactions.
- No cross-shard transactions.

Open questions:

- How a durable backend integrates with the existing Graph.Update/Tx API.
- Whether transactions support compare-and-set preconditions.
- Whether to expose public read-your-writes reads inside the transaction.
- Whether transaction commit returns updated versions for changed nodes.
- Whether durability is append-log-only in the prototype.
- How the graph mutex evolves with executor lanes or storage-level sequencing.

## Durability

Deferred: the following is a target design, not an implemented storage layer.

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

Current local behavior: transactions, recomputation, and callbacks are serialized
per graph; callbacks observe the settled result and are not reordered. The
out-of-order delivery and resynchronization requirements below concern future
remote streams. Coalescing/rate control is not yet implemented in the executor.

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

Current public types:

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
	Version  Version
}
```

Current local subscription behavior:

- `Subscribe` installs a callback for subsequent changes; it does not emit an
  initial snapshot. `Read` gets the current `Snapshot[T]` separately.
- `SubscribeMap` atomically obtains the initial snapshot and installs a map
  change callback. `MapEvent` includes previous/current map snapshots, normalized
  keyed changes, and the graph-local version.
- `SubscribeStruct` provides the same atomic setup for typed records and emits
  StructChanges with field replacements and nested map/set mutations. Its
  immutable Then operation supports caller-managed stream coalescing.
- Callbacks run synchronously after propagation, while the graph is locked.
  They must not re-enter the graph or block on network I/O. Unsubscribe is
  synchronous and idempotent when called outside a callback.
- Both option fields exist, but delivery is currently fixed-point-per-transaction
  regardless of options; there is no executor-level coalescing.
- `Snapshot[T]` exposes Value, Version, and Valid. Result error/partial metadata
  is available to computations through `Dep[T]`, not in Snapshot/Event today.

Target remote subscription behavior:

- Updates may arrive out of order.
- Subscribers should receive enough metadata to order, discard, or resync.
- Function output subscribers should have access to both old and new values so they can diff.
- Subscribers cache last-known-good values.
- Subscribers to externally encoded outputs can request fixed-point-only callbacks.
- Output encoders may coalesce multiple fixed-point updates before writing to the wire.

Open questions:

- Whether to add channel/pull delivery alongside the local callback API.
- Remote unsubscribe acknowledgment and cleanup semantics.
- An atomic snapshot-plus-watch API for scalar/set nodes, analogous to SubscribeMap.
- Backpressure and explicit resnapshot behavior for remote subscribers.
- How subscription options interact with an asynchronous executor.
- How snapshot/events expose partial results, errors, and remote provenance.

## Cross-Shard Subscriptions And Caching

Current direction: cross-machine subscriptions are a core mechanism for
resolving nonlocal dependencies, including computed function outputs. This
supersedes the earlier replicated-input-only prototype direction.

Decision: instances speak a bidirectional query/stream-changes protocol through
the Locator-selected transport. Subscribe and unsubscribe remote data/function
instances as they are needed and released. WebSocket is a possible transport;
the library contract remains generic.

Remote collection updates use the same atomic delta semantics as local
updates. A received clear/remove/add-or-put batch must not expose intermediate
states. This does not imply a distributed transaction or global fixed point.

Proposed implementation approach, not yet a settled API: share one upstream
watch when multiple local consumers need the same remote node, retain it while
needed, and unsubscribe when the final consumer releases it. Multiplexing
multiple node watches over a peer session should be considered separately
from node watch identity and lifetime.

Decision: when one shard depends on another shard's value, the subscribing shard caches the results it receives.

Purpose:

- Restart recovery.
- Continued operation when a peer shard is down.
- Potential local fallback computation when possible (still deferred).

Decision: explicit cache invalidation should not be necessary for pure function results. Input changes drive recomputation; function identity includes version.

Previously proposed result precedence (fallback behavior remains deferred):

1. Authoritative remote result.
2. Cached remote result.
3. Local fallback result.

Open questions:

- Whether cached remote values are durable.
- How staleness is exposed to callers.
- Whether cached values have TTLs for observability, even if not for invalidation.
- Whether fallback results can be published downstream or are marked local-only.
- Snapshot-to-delta handoff, stream ordering, base versions, and resynchronizing
  after lost deltas or reconnects.
- Watch/session sharing, unsubscribe races, and slow-subscriber backpressure.
- How ownership changes invalidate Locator results and move active watches.
- How DAG validation and readiness/fixed-point reporting work across remote
  dependency boundaries without implying cross-shard atomicity.

## Value Envelopes

Values propagated through the system need metadata.

Deferred remote/provenance sketch (not a public API):

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
	ClassName       NodeClassName
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
- How ClassName, FunctionVersion, and a generic instance key form a remote address.
- Whether source/provenance metadata should be part of durable cache state.

## Incremental Collections

Decision: maps and sets are first-class incremental collection values.

Snapshot iteration (2026-09-27): keep the callback-based `Range` methods and
also expose `SetSnapshot.All() iter.Seq[K]` and
`MapSnapshot.All() iter.Seq2[K, V]` for native Go range loops. Iterators are lazy,
reusable, stop immediately on early exit, and retain the snapshot on which they
were created. They walk the immutable backing store without materializing a
copy. Order is unspecified; zero/empty snapshots yield nothing.

Reasoning: large collections often change by small deltas, and retransmitting the full collection on every update is too expensive.

Decision: maps are the general primitive.

Decision: sets are a specialization of maps, for example:

```go
map[K]struct{}
map[K]bool
```

Decision: the library must provide a first-class set type for string and integer keys, with upsert and delete operations.

Implemented set shapes include:

- `Node[SetSnapshot[string]]`, constructed by `SetData[string](className)`.
- `Node[SetSnapshot[int]]`, or a defined comparable key type.

The snapshot is an immutable value; its Node is the graph handle. Map data nodes
similarly have type `Node[MapSnapshot[K, V]]`, constructed by
`MapData[K, V](className)`. There are no public `Set[K]` or `Map[K, V]` types.

### Atomic Collection Deltas

Decision (2026-09-26): sets and maps use the same ordered batch semantics for
in-process and cross-machine change notifications. One delta atomically applies
any combination of the following phases, in this order:

1. Clear the entire collection, if requested.
2. Remove the specified items or keys.
3. Add the specified items, or put the specified whole key/value entries.

Subscribers and dependent computations observe the state before the batch and
the state after the batch, never a partially applied batch. In particular,
clear-all followed by additions is one replacement, not an observable empty
collection followed by a second change. If a key appears in both the removal
and addition phases, the addition wins.

Current membership uses Go equality on comparable keys. The local APIs below
are implemented; remote wire field names and any canonical encoding remain
undecided.

### Set Updates

Current public payload:

```go
type SetDelta[K comparable] struct {
	Clear  bool
	Remove []K
	Add    []K
}
```

Examples:

- Clear all: `{Clear: true}`.
- Replace with exactly `{A, B, C}`: `{Clear: true, Add: [A, B, C]}`.
- Add `{A}` and remove `{B, C}`: `{Remove: [B, C], Add: [A]}`.

Repeated additions have set semantics; removing an absent item has no effect.
A batch with no operations leaves the value unchanged. Replacing with the
empty set is expressed explicitly with `Clear: true`.

`SetSnapshot.WithDelta` produces a persistent snapshot. `ApplySetDelta(tx, node,
delta)` batches mutations to a set data node; `SetUpsert` and `SetDelete` are
single-item helpers. Published `SetChange{Key, Present}` events describe net
membership changes, not a compact clear marker; clearing enumerates removals.

### Map Updates

Maps use equivalent phases, with whole values assigned in the final phase:

```go
type MapEntry[K comparable, V any] struct {
	Key   K
	Value V
}

type MapDelta[K comparable, V any] struct {
	Clear  bool
	Remove []K
	Put    []MapEntry[K, V]
}
```

The entry-list notation avoids JSON object-key restrictions, but the current
Go API still requires comparable keys. It does not commit to a wire encoding.
Removing an absent key has no effect; putting an existing key replaces its
whole value. A put of a nil/null value is distinct from removing the key.
`Clear: true` plus `Put` replaces the whole map atomically, including replacement
with an empty map.

Decision (2026-09-27): duplicate keys in a Put batch use the last entry in the
ordered slice. Normalize to one net change per key before publication. The local
`MapDelta`/`WithDelta` API implements this; the eventual wire encoding must
preserve this precedence. Current Go APIs require comparable keys; canonical
JSON identity for non-comparable keys remains future work.

`MapSnapshot.WithDelta` produces a persistent snapshot. `MapPut` and `MapDelete`
mutate data nodes within a Tx. A transaction-level `ApplyMapDelta` helper is not
implemented. `MapChange` records Key, Before/After, and BeforeValid/AfterValid;
the validity flags distinguish absence from a present nil/zero value.

### Snapshots, Versions, And Delivery

Current local `Version` values are graph-local monotonic counters assigned as
changed leaf and derived values are published. One transaction may advance the
counter multiple times; snapshots carry the version at which that node last
changed. Set/map
`Changes()` reports the snapshot's own last delta. `ChangesSince(previous)` is
the safe consumer API: shared roots and adjacent lineage avoid scanning, while
arbitrary replacements or skipped bases may require reconciliation.

Target remote protocol requirements:

- An initial snapshot or resynchronization can use clear-all plus add/put in a
  single atomic replacement payload.
- Steady-state deltas apply only to a known prior version; the message envelope
  still needs base/version and subscription identity semantics.
- A missing base requires resynchronization. Replacement payloads still need
  ordering checks so a stale replacement cannot overwrite newer state.
- Updates may be batched and coalesced, preserving their resulting state and
  atomic visibility. The ordering above is within one batch; separate batches
  must still be composed in delivery/version order.
- Small point updates must not scan or copy the entire collection. Explicit
  replacement may necessarily process every supplied entry; derived operators
  must also account for any affected output memberships.

### Union Of Sets

Decision (2026-09-26): provide a built-in function taking two or more sets of
the same element type and returning their union:

```text
Union(S1, S2, ..., Sn): Set<K>    where n >= 2
```

Required behavior:

- An item is present in the output whenever any input contains it.
- Removing an item from one input must not remove it from the union while
  another input still contains it.
- Apply each input's atomic delta without publishing intermediate states;
  downstream events also respect the existing fixed-point semantics.
- Process small input mutations incrementally. Only output membership changes
  need to propagate, e.g. the first contributing input adds an item or the last
  contributing input removes it.

Implemented as `nodes.Union(className, inputs...)`, requiring 2+ inputs. It uses
per-graph persistent contributor counts, so point updates do not rebuild the
union or scan all input sets. Clear-all removes only that input's contribution,
not the entire union.

### Additional Set Algebra And Map Projections

Implemented (2026-09-27) in `reco/nodes`, using only public reco APIs:

- `nodes.Intersection(className, inputs...)`: keys present in every input; requires 2+ sets.
- `nodes.Xor(className, inputs...)`: symmetric difference, meaning odd membership parity
  across 2+ inputs, not "exactly one" when there are more than two inputs.
- `nodes.Difference(className, first, others...)`: first minus the union of the other sets;
  requires at least one other set. Relative complement is
  `nodes.Difference(className, universe, excluded)`; no implicit universal set exists.
- `nodes.MapKeys(className, mapNode)`: the key set, ignoring value-only changes. Values
  need not be comparable.
- `nodes.MapValues(className, mapNode)`: distinct comparable values with contributor counts.
  Removal of one duplicate does not remove the value until its last contributor
  disappears. Swaps/transfers within a transaction do not flicker memberships.

Repeated input nodes count separately for XOR, so `nodes.Xor("xor", a, a)` is empty, whereas
union/intersection with the same input twice retains its memberships. Difference
with itself is empty. All nodes have per-graph caches, persistent count/output
storage, atomic touched-key deltas, and no notifications for unchanged outputs.
Initial state, clear, and arbitrary replacements can require enumeration;
steady-state changes do not scan collection contents.

Map entry equality preserves comparable identities: use `ValueEqualer` when
provided, otherwise Go equality for comparable values and `reflect.DeepEqual`
for non-comparable values. In particular, distinct pointers with equal pointees
are distinct values; this matters when projecting values into set keys. Scalar
node default equality is unchanged. MapValues requires reflexive values (no
NaNs), dynamically comparable interface values, and custom equality consistent
with Go equality. Nil and zero values are ordinary members, not deletion markers.

Tests cover algebraic membership, duplicate inputs/values, atomic events,
replacements/clear, concurrent graph isolation, and exact mapper invocation
counts. White-box tests count input hashes/comparisons and inspect actual output
HAMT sharing at 1,024 and 16,384 entries. Size-scaling benchmarks include each new
operator at 1,000/100,000 entries and delta sizes 1/16.

### Map Operation Over Sets

Decision (refined 2026-09-26): the dataflow library must support a map operation
over a set in which the pure function returns only the value. The input key
is preserved as the output key.

Conceptually:

```text
S: Set<K>
F: K -> V
map(F, S): Map<K, V>
```

For each input item `k`, the output contains `k -> F(k)`. The output map's
keys are exactly the input set's members. Key remapping is not a requirement,
so this operator needs no output-key collision policy.

Implemented as `nodes.MapSet(className, input, fn)` with `K comparable`, `V any`,
and a pure `fn func(K) V`. Its behavior:

- Adding `k` computes `F(k)` once when the operator evaluates.
- Removing `k` deletes that same key from the output without reevaluating
  unrelated inputs or scanning the map.
- Results are an incremental map-shaped node using the same atomic delta
  semantics as map data nodes.

Deferred extension: reactive per-key dependencies could dirty only affected
keys and reevaluate their values in place. Today's mapper depends only on k;
it cannot declare other reactive inputs or return per-key Result metadata.

This is expected to be a central operation for computing per-peer or per-node parts of `MapResponse`.

Open questions for that extension:

- Whether reactive `F(k)` uses a function-node template, a per-key subgraph, or a specialized operator.
- How dependencies from `F(k)` to other nodes are declared.
- Whether failures for individual keys produce partial map values.

### Sorted Values Of A Map

Deferred proposal: `reco/nodes` may provide an operator producing sorted map
values using a configured sort key. No SortedValues API is implemented.

Example use case:

- `MapResponse.Peers` must be sorted by `tailcfg.Node.ID`.

Conceptually:

```text
SortedValues(className, input: Node<MapSnapshot<K, V>>, less): Node<sorted values>
```

or:

```text
SortedValuesByKey(className, input: Node<MapSnapshot<K, V>>, sortKey): Node<sorted values>
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

Deferred proposal: canonical JSON identity could allow non-comparable keys or
elements. Current map keys and set elements use Go comparability and equality,
not encoded bytes. Changing that contract requires an explicit API decision.

### V1 Constraint

Decision: map entry mutation is `Put(key, wholeValue)` or `Delete(key)`; an
atomic batch may additionally clear the entire collection before these entry
operations. Replacement is clear-all plus puts.

Decision: no recursive nested patch language in the first version.

Open questions:

- Whether map values can be partial values.
- Whether collection deltas are persisted in transaction logs or only used in propagation.

Settled locally: incremental collections are implemented. SetSnapshot and
MapSnapshot are reco-owned value types hiding immutable's storage; constructors
return typed Node handles. Data mutation helpers operate on Tx; snapshot
WithDelta enables external operators to construct immutable derived values.

## Persistent Data Structures And Snapshotting

Decision: large map- and struct-shaped values should use persistent immutable data structures where that makes snapshots and diffs cheap.

Decision: the prototype uses `github.com/benbjohnson/immutable.Map`, wrapped by
Reco-owned snapshot and mutation APIs.

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
- Whether Reco needs a local fork or wrapper that tracks changed paths.
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

Current implementation: Graph.Update performs synchronous propagation through a
sparse, dependency-ordered dirty queue, then invokes subscribers. It creates no
worker goroutines. Eval grants access to declared inputs, not cancellation or
deadlines. The executor/scheduler below is a deferred target.

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

Implemented identity API:

```go
type NodeClassName string
func (n Node[T]) ClassName() NodeClassName
```

Every constructor, including those in `reco/nodes`, takes a className of this
type. Untyped string literals work directly; computed string names need an
explicit `NodeClassName(s)` conversion. Names are unique within a graph,
including dependencies, and can be reused across independent graphs.

Runtime handles use definition-pointer identity. Separate constructor calls
produce separate definitions; equal class names do not intern or merge them,
and registering both in one graph is an error. There is no public NodeIdentity
or NodeDefinition interface. External packages create nodes with Operator and
Input, using library-owned Node handles. Remote class/version registration and
generic instance-key addressing remain open.

Decision: use strongly typed Go generics where possible, with declaration-time
validation of dependency shapes and registration-time validation of names and
DAG structure. JSON compatibility validation remains deferred.

Decision: reflection is acceptable for API ergonomics and declaration-time validation.

Requirements for reflection use:

- Reflection should happen when node definitions are declared, bound, or first instantiated.
- Reflection validates dependency names, field types, and compute function shape.
- Reflection failures should be reported early, preferably before the graph starts processing transactions.
- Target: remove reflection from hot recomputation paths. Currently Func still
  uses reflection to populate dependency structs; Operator/Input avoids that adapter.
- The runtime should cache any adapters, field indexes, and type metadata produced by reflection.
- Reflection-derived metadata should be memoized across declarations and graph instances where the reflected type/function shape is the same.
- Memoization keys should account for dependency struct type, compute function type, output type, and any binding shape needed to preserve safety.

Decision: function nodes should have named dependencies with declared types.

Decision: support both a full typed-struct dependency style and a lightweight inline dependency style if both can compile to the same internal node representation.

Preferred full style: typed struct dependencies. This is the main API direction for serious reusable nodes.

Implemented lightweight style: inline anonymous-struct dependencies with Deps,
for small local nodes that do not need a named dependency struct.

Current dependency model:

- Func binds named dependency fields to other Node handles at declaration time.
- Operator declares a dependency slice and reads typed inputs with Input(eval, node).
- Register recursively instantiates those definitions within each graph.
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

peerView := Func("peerView", struct {
	Node     Node[NodeState]
	Profile  Node[UserProfile]
	Settings Node[TailnetSettings]
}{
	Node:     nodeState,
	Profile:  userProfile,
	Settings: tailnetSettings,
}, func(eval Eval, in PeerInputs) Result[PeerView] {
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
	func(eval Eval, in struct {
		Node     NodeState
		Profile  UserProfile
		Settings TailnetSettings
	}) Result[PeerView] {
		return OK(computePeerView(in.Node, in.Profile, in.Settings))
	},
)
```

These examples use the current Func/Deps binding shapes; the application types
and computePeerView are illustrative. The important design intent is:

- Callers can choose a named dependency struct for clarity and reuse.
- Callers can choose an inline struct for lightweight local nodes.
- Both forms preserve named dependencies.
- Both forms compile to the same runtime representation.
- The runtime validates DAG constraints and dependency binding; it does not yet
  validate JSON compatibility.

Current lower-level extension API:

```go
func Operator[T any](className NodeClassName, deps []Dependency, newCompute func() Compute[T]) Node[T]
type Compute[T any] func(Eval) Result[T]
func Input[T any](eval Eval, node Node[T]) Dep[T]
```

The factory owns per-graph incremental state. Earlier builder/type-based API
sketches are not implemented; versioning and debounce methods remain deferred.

Open questions:

- How class names and function versions compose with generic instance keys for remote addressing.
- How to remove remaining reflection from Func recomputation.
- Whether dynamic graph changes are needed later.
- How generic APIs map to future canonical storage/transport encodings.
- How definition identity is restored across process restarts.

## Node Runtime State

Current runtime state lives in Graph-owned registries keyed by node definition.
Operator caches are created per graph. Local subscriptions also live on Graph;
upstream remote-watch lifetimes in the following target model are deferred.

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

Target: the runtime must track the watches needed by function dependencies;
pure compute functions should not manually manage network subscriptions.

Open questions:

- Whether remote watches need a separate manager beyond Graph's local subscription registry.
- How much previous-value history is retained.
- Whether old/new diff support is universal or only subscription-event metadata.

## Fallback Computation

Current status: fallback computation remains deferred. Normal subscription to
a function on its remote owner is in scope and is distinct from computing a
substitute locally during an outage. The proposals below do not require all
inputs to be pre-replicated or make fallback a prerequisite for remote watches.

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

Current implementation status as of 2026-09-27:

- The module is `github.com/bradfitz/reco`, with package `reco` at the repository
  root and reusable operators in `github.com/bradfitz/reco/nodes`.
- `cmd/recontrol` contains the start of an in-memory Tailscale control server.
- `cmd/webdemo` serves an embedded live graph UI over HTTP/WebSocket.
- `Result[T]`, `Node[T]`, `Dep[T]`, `Graph`, `Tx`, `Snapshot[T]`, `Event[T]`, and subscription option types exist.
- All constructors take `NodeClassName`; `Node.ClassName()` returns it.
  Duplicate names are rejected graph-wide, including recursive dependencies.
- Scalar data nodes can be created with `Data[T](className)`.
- Function nodes can be created with `Func`.
- Stateful incremental function nodes can be created outside the library with
  `Operator`, per-graph compute factories, and declared `Input` reads.
- Both dependency API directions have an initial prototype:
  - typed dependency input structs using `Dep[T]`
  - inline dependency structs using `Deps(...)`
- Reflection validates dependency declarations early and memoizes reflected shape metadata.
- One graph serializes transactions through `Graph.Update`.
- Transactions can set scalar data nodes with package-level `Set(tx, node, value)`.
- The runtime queues only affected functions in dependency order, at most once
  per transaction, skipping unchanged inputs/outputs and unrelated nodes.
- Subscriptions deliver `Event[T]` values containing previous and current snapshots.
- Mutable map and set data nodes use persistent HAMT snapshots:
  - `MapData[K,V]`
  - `SetData[K]`
  - `MapPut`
  - `MapDelete`
  - `SetUpsert`
  - `SetDelete`
  - `SetDelta` / `ApplySetDelta`
  - `MapDelta` / `MapEntry` for snapshot WithDelta batches
- `nodes.Union`, `nodes.Intersection`, `nodes.Xor`, and `nodes.Difference`
  incrementally compute set algebra. `nodes.MapKeys` and `nodes.MapValues`
  project maps; MapValues tracks duplicate contributors. `nodes.MapSet`
  evaluates pure `F(k)` only for newly added keys. All use public
  Operator/snapshot APIs and preserve structural sharing and touched-key changes.
- Set/map snapshots have public `WithDelta` and `ChangesSince` methods. Custom
  immutable values can implement the optional equality/version interfaces.
- Set/map snapshots support both callback Range and native Go All iterators.
- StructSnapshot/StructDelta provide typed records with field-aware equality,
  atomic mutations, nested collection changes, and efficient StructChanges.Then
  composition. nodes.Struct assembles field inputs; SubscribeStruct supplies
  initial-snapshot-plus-watch. Stream encoders can consume only changed entries.
- The webdemo supports atomic transaction staging/commit/rollback, dependency
  highlighting, and a details -> totalRunes -> score -> summary pipeline. Rune
  totals are maintained by a demo-local incremental map reduction, not a public
  nodes reduction API. Initial/reconnect messages are full snapshots; subsequent
  messages carry scalar changes and touched-key collection patches.
- `SubscribeMap` atomically returns an initial snapshot and subscribes to
  transaction-local keyed mutations. Adjacent deltas avoid whole-map diffs;
  arbitrary replacements and skipped delta bases use reconciliation.
- DAG cycle rejection is implemented.

Current prototype simplifications:

- No asynchronous/rate-controlled executor, debounce, or fairness scheduler yet.
- No durable storage yet.
- No canonical JSON validation yet.
- `MapSet` does not yet support reactive per-key dependencies or partial values.
- Collection keys still use Go comparability, not canonical JSON identity.
- `ApplySetDelta` clear currently enumerates removals to preserve the existing
  point-change event API; a first-class compact clear event remains future work.
- No sorted-values node yet.
- The control-server sketch keeps nodes in a reco map data node and exposes an
  O(1) derived response-state node that preserves the persistent snapshot and
  its keyed mutations. It does O(N) work only to build the required complete
  response when a map stream starts.
- A lite map update is a HAMT point update. Streaming polls consume its keyed
  reco mutation directly and encode `Node`, `PeersChanged`, or `PeersRemoved`
  without rebuilding or diffing full `MapResponse` values.
- Per-stream control-server mutations are coalesced by the application's map
  entry key (the peer's public key), not by a reco class name, before wire encoding.
- Subscription options are accepted but not semantically implemented beyond fixed-point-by-transaction behavior.
- Registration builds dependency indexes/topological ranks; mutation-time work
  visits the affected subgraph rather than the full node registry. Each invoked
  generic function still reads its declared dependencies.
- Reflection metadata is memoized, but the compute adapter still uses reflection to populate dependency structs.

Historical first prototype sequence (not the current implementation queue;
the discussion TODOs at the top take precedence):

1. Build canonical JSON identity helpers.
2. Build local node registry and DAG validation.
3. Build first-class incremental map and set nodes. (Implemented as
   MapSnapshot/SetSnapshot values and MapData/SetData constructors.)
4. Build map-over-set operation.
5. Build local data nodes and function nodes.
6. Build local transaction commit with durable storage interface.
7. Build local propagation with explicit executor.
8. Build fixed-point detection for selected output nodes.
9. Build subscriptions with old/new value events and fixed-point-only output callbacks.
10. Add immutable snapshot support for map-shaped nodes. (Implemented for maps
    and sets.)
11. Add sorted-values-of-map node.
12. Add basic `MapResponse` internal representation and delta tracking.
13. Add Locator-based multi-shard resolution and remote data/function watches.
14. Add debouncing and basic fairness across customers.
15. Add Tailscale `MapResponse` initial-complete and subsequent-delta encoder prototype.

Use a single-process multi-shard simulation for deterministic tests, alongside
cross-machine transport tests. Work on the control-server application remains
deferred while the primitives and remote-watch design are discussed.

## Testing Strategy

Current and target coverage (deferred features require future tests):

- Class-name preservation, empty-name rejection, graph-wide name uniqueness,
  repeat registration, and definition/name reuse across independent graphs.
- Canonical JSON equality across non-comparable values (deferred).
- DAG cycle rejection.
- Atomic multi-cell transaction commit.
- Durable commit before propagation.
- Function recomputation after data changes.
- Debouncing that skips intermediate values but converges to final state.
- Old/new subscription events.
- Subscribe and unsubscribe lifecycle.
- Locator routing for generic keys, including customer-containing tuples.
- Cross-machine watches of both data and function instances.
- Remote atomic collection deltas, reconnect/resync, and watch cleanup.
- Replicated-input cache usage during local recomputation.
- Out-of-order update handling.
- Map/set snapshot and delta application.
- Set upsert/delete behavior for string and integer keys.
- Map-over-set incremental behavior.
- Intersection/Xor/Difference algebra, repeated inputs, MapKeys projections,
  and duplicate-value contributor counts for MapValues.
- Exact delta-sized compute/hash/equality work and actual HAMT sharing.
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
- Which additional end-to-end large-graph workloads to add to the existing
  delta-size, collection-size, and unrelated-graph-size benchmarks.

## Current Open Questions

### API

- How do graph-local NodeClassName values, function versions, and generic
  instance keys compose into portable remote addresses?
- How are reactive per-key dependencies declared for a future MapSet extension?
- What is the public API for sorted map values?
- Should Result's existing error/partial metadata gain a portable structured status type?
- What is the transaction-level map batch helper API alongside ApplySetDelta?

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

- Should a channel/pull API complement the current synchronous local callbacks?
- What are remote unsubscribe acknowledgment and watch cleanup semantics?
- How is remote backpressure handled?
- Should scalar/set watchers get atomic snapshot-plus-watch, like SubscribeMap?
- How are out-of-order remote events represented and reconciled?
- How do FixedPointOnly/Coalesce options interact with an asynchronous executor?

### Partial Results And Errors

- What is the structured error type?
- How should partial/error results be stored in durable remote caches?
- How should Snapshot/Event expose result status? Computations already receive
  validity, error, and partial-result metadata through Dep accessors.
- How are errors compared across the wire? Local propagation already includes
  result error/partial status when deciding whether a node changed.

### Incremental Collections

- Are deltas persisted or only propagated?
- How do graph-local versions map to remote stream epochs and base versions?
- How should events represent compact clear/replacement operations?
- Can a richer changed-path representation retain incremental behavior across
  skipped snapshots? Current operators publish WithDelta snapshots and consume
  ChangesSince; arbitrary/skipped bases may need full reconciliation.

### Tailscale MapResponse

- Should the internal representation be `map[string]any`, an immutable map/tree, or a custom typed structure?
- How are `tailcfg.MapResponse` field-level deltas represented internally?
- Which fields need custom zero-value/empty-slice marshal support?
- How should client `CapabilityVersion` select output behavior?
- How should `PeersChangedPatch` be generated and selected?
- How should `PacketFilters` and `DisplayMessages` patch semantics map to Reco map deltas?
- How should non-peer `MapResponse` fields expose keyed/path mutations to the
  subscription watcher?
- What benchmarks define acceptable sub-linear behavior?

### Distribution

- What is the eventual transport abstraction?
- How does a typed Locator expose local ownership versus a remote transport?
- How are portable definition/version identities registered and paired with
  generic instance keys in query/watch messages?
- Who owns shard discovery?
- How are routing changes deployed?
- How do subscriptions survive customer migration?
- How does authentication/authorization fit into peer sessions and node watches?
- How are shared remote watches and sessions owned and released?

## Decisions To Revisit

These decisions are good enough for prototyping but may need revision:

- Canonical JSON as the universal identity mechanism.
- JSON as the only serialization requirement.
- Function values are not durably persisted.
- Maps, sets, and typed fixed-field records are the current incremental primitives.
- No nested patch language in v1.
- Exact division between deterministic in-process and multi-process transport tests.
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

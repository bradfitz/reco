# recotestcontrol

Experimental test code, not a production control server or a stable API.

This is a reco-backed replacement for the public Tailscale
[`tstest/integration/testcontrol`](https://github.com/tailscale/tailscale/blob/d229a06f9a4b2340f0749288df0ac4f6de223acd/tstest/integration/testcontrol/testcontrol.go)
server. Protocol, authentication, Noise, Tailnet Lock, and test-hook code was
adapted from that revision, retaining its BSD-3-Clause notices. No private
control-server implementation is used.

## Data flow

Each server has one fixed graph, independent of the number of HTTP streams:

```text
node map ──────────┬───────────────────┐
                   └─ packet policy ──┤
configuration ─────┬──────────┘        ├─ map-meta struct ─ subscriptions
                   └──────────────────┤                     │
user-profile map ─────────────────────┘              MapResponse encoder
```

Nodes and profiles use persistent reco maps. A small application-local
`Operator` recomputes packet policy only when its relevant inputs change.
`nodes.Struct` assembles the settled record. `SubscribeStruct` supplies an
atomic snapshot/watch, and each stream coalesces unsent changes with
`StructChanges.Then`. Encoding and network I/O happen outside graph callbacks.

The first response is full; subsequent node changes become `PeersChanged`,
`PeersRemoved`, or a self `Node` update. These are whole changed **nodes**, not
yet field-level `PeerChange` patches. A lite update only returns HTTP status;
it does not render a full map. Ordinary updates process touched entries plus
persistent-map path copying, per subscriber, rather than walking all peers.
The allowlist policy also stays unchanged on endpoint/disco updates.

Session replacement and FIFO ping/raw-response injection remain HTTP concerns,
not graph subscriptions. Disconnecting unregisters the subscription. Pending
graph changes are bounded to 10,000 distinct changes; exceeding that closes the
stream so a client can reconnect for a fresh snapshot. This is an entry-count
bound, not a byte limit, and the explicit test-injection queue is not bounded.

## Running the upstream tests

The companion Tailscale checkout has a `!experiment.reco` constraint on its
original `testcontrol.go`. Its `reco-experiment.go` uses `experiment.reco` and
aliases `Server`, `AuthPath`, `MasqueradePair`, `AltMapStreamFunc`, and
`MapStreamWriter` to this package, and forwards `RejectRequestForPath`.

Use a local workspace containing both modules (the workspace files are ignored
here). With those build-tag changes in the Tailscale checkout:

```sh
# From this repository; substitute your actual Tailscale checkout path.
go work init . /path/to/tailscale.com
export GOWORK="$PWD/go.work"
cd /path/to/tailscale.com
GOFLAGS=-tags=experiment.reco go test \
  ./tstest/integration/testcontrol ./control/tsp ./control/controlclient \
  ./tsnet ./tstest/integration ./feature/taildrop ./cmd/sniproxy \
  ./tsconsensus ./tstest/largetailnet ./tstest/membudget
```

If `go.work` already exists, use `go work use` instead of `go work init`.
`GOFLAGS` also propagates the tag to integration tests' subprocess builds.
The workspace avoids committing local replacement paths to either module.
The `control/tsp` peer-update test accepts both `Peers` and `PeersChanged`, as
permitted by the protocol, so it works with either server.

In this repository, `GOWORK=off go test ./...` verifies the public pinned
dependency independently of the workspace. Use `go test -race ./...` for
concurrency checks and `go test ./recotestcontrol -bench=PeerDelta -run='^$'`
for the incremental path benchmark.

## Deliberate limitations

- Set public configuration fields before first use; use setters for live edits.
- Configuration setters, including per-node route/capability overrides, currently
  replace one immutable configuration value and trigger full responses. They
  are not yet fine-grained graph joins.
- A signed-node address or membership change may rebuild the explicit packet
  filter. Endpoint/disco changes do not. Filter rules are whole-value wire fields.
- The shared record is not a production per-recipient authorization graph.
  Peer personalization happens in the encoder for touched entries.
- There is no distributed graph, durable state, session resumption, asynchronous
  graph scheduler, or production authentication/policy enforcement.
- VM/browser-specific upstream packages can be compile-checked locally; running
  those environments needs their own infrastructure.

The existing reco primitives were sufficient for this version; no new core API
was required. Useful next steps are incremental joins for per-node overrides,
field-level peer patches, and byte-bounded backpressure.

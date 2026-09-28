// Package metagraph is an EXPERIMENTAL, unstable prototype of multiplexed
// cross-process reco watches. Do not depend on this API or use it on untrusted
// networks. There is no authentication, consensus, or dynamic ownership yet.
//
// A metagraph is a logical dataflow graph spanning local Graph instances. Each
// addressed value has one configured authority. Export makes a local data or
// function node watchable; Import creates a local mirror leaf and holds demand
// for its remote value. Operators remain pure and need not know about transport.
//
// A Peer is one bidirectional WebSocket connection to a known other process.
// All watches share its bounded reader/writer queues and a constant number of
// goroutines. Multiple Import calls for the same address share a mirror and
// upstream subscription. Closing the last Watch unsubscribes remotely. The
// caller currently owns this demand explicitly, independently of graph demand.
//
// Delivery is ordered, version-checked, and atomic per node, not across nodes or
// processes. Reconnect starts fresh subscriptions and replaces cached values
// with snapshots. While disconnected the last values remain cached; Status
// reports that they are stale. No deltas are silently dropped or replayed.
package metagraph

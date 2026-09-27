// Package nodes provides reusable incremental node definitions for reco graphs.
// Union, Intersection, Xor, and Difference implement set algebra. MapSet
// preserves set keys while computing a value for each added member. MapKeys
// and MapValues project a map into a set of keys or distinct comparable values.
// Point updates do work proportional to changed keys plus persistent storage
// traversal and the number of declared inputs, rather than rebuilding collections.
// Initial snapshots, replacements, and clear operations may require enumeration.
//
// Definitions can be reused across independent graphs; incremental caches are
// graph-local. These nodes use only public reco APIs, the same APIs available
// to other packages implementing their own nodes. See [github.com/bradfitz/reco.Operator].
//
// Struct assembles typed record fields from input nodes, retaining efficient
// collection equality and changes for reco.SubscribeStruct.
package nodes

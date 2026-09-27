package reco_test

import (
	"fmt"
	"slices"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func ExampleStructSnapshot_WithDelta() {
	type settings struct {
		Enabled bool
		Names   reco.SetSnapshot[string]
	}
	enabled := reco.Field[settings, bool]("Enabled")
	names := reco.Field[settings, reco.SetSnapshot[string]]("Names")
	before := reco.NewStruct(settings{Enabled: true})
	after := before.WithDelta(reco.StructDelta[settings]{
		enabled.Set(false), // Explicit false, not an omitted change.
		names.Update(func(s reco.SetSnapshot[string]) reco.SetSnapshot[string] {
			return s.WithDelta(reco.SetDelta[string]{Add: []string{"elm"}})
		}),
	})
	changes := after.ChangesSince(before)
	if old, value, changed := enabled.Change(changes); changed {
		fmt.Println("enabled:", old, "->", value)
	}
	for change := range reco.SetFieldChanges(names, changes) {
		fmt.Println("member:", change.Key, change.Present)
	}
	fmt.Println("old snapshot:", before.Value().Enabled, before.Value().Names.Len())
	// Output:
	// enabled: true -> false
	// member: elm true
	// old snapshot: true 0
}

func ExampleSubscribeStruct() {
	// A miniature control-server meta record. The actual application can use
	// NodeID/PeerInfo and add DNS, filter, policy, and other typed fields.
	type meta struct {
		Domain string
		Peers  reco.MapSnapshot[int, string]
	}
	domain := reco.Data[string]("domain")
	peers := reco.MapData[int, string]("peers")
	mrm := nodes.Struct[meta]("mapResponseMeta", struct {
		Domain reco.Node[string]
		Peers  reco.Node[reco.MapSnapshot[int, string]]
	}{domain, peers})
	domainField := reco.Field[meta, string]("Domain")
	peerField := reco.Field[meta, reco.MapSnapshot[int, string]]("Peers")
	g := reco.NewGraph()
	if err := g.Register(mrm); err != nil {
		panic(err)
	}
	update := func(fn func(*reco.Tx)) {
		if err := g.Update(func(tx *reco.Tx) error { fn(tx); return nil }); err != nil {
			panic(err)
		}
	}
	update(func(tx *reco.Tx) {
		reco.Set(tx, domain, "example.net")
		reco.MapPut(tx, peers, 1, "original")
		reco.MapPut(tx, peers, 2, "leaving")
	})

	// Callbacks only accumulate immutable semantic changes. A real stream
	// writer would synchronize access to pending and bound its retained size.
	// Serialization, compression, and network writes happen outside callbacks.
	var pending reco.StructChanges[meta]
	initial, sub, err := reco.SubscribeStruct(g, mrm, reco.SubscribeOptions{}, func(ev reco.StructEvent[meta]) {
		var err error
		pending, err = pending.Then(ev.Changes)
		if err != nil {
			panic(err)
		} // A production stream would terminate/resync.
	})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()
	fmt.Println("initial:", initial.Value().Value().Domain, initial.Value().Value().Peers.Len(), "peers")

	// Updates can arrive while the initial snapshot is being sent.
	update(func(tx *reco.Tx) { reco.MapPut(tx, peers, 1, "intermediate"); reco.MapPut(tx, peers, 3, "joined") })
	update(func(tx *reco.Tx) {
		reco.MapPut(tx, peers, 1, "final")
		reco.MapDelete(tx, peers, 2)
		reco.Set(tx, domain, "new.example.net")
	})

	if _, current, changed := domainField.Change(pending); changed {
		fmt.Println("domain:", current)
	}
	// Sort only the changed peers, not the full live map. Even though the
	// writer coalesced events, no deep endpoint-snapshot comparison is needed.
	changes := slices.Collect(reco.MapFieldChanges(peerField, pending))
	slices.SortFunc(changes, func(a, b reco.MapChange[int, string]) int { return a.Key - b.Key })
	for _, change := range changes {
		if change.AfterValid {
			fmt.Println("upsert:", change.Key, change.After)
		} else {
			fmt.Println("remove:", change.Key)
		}
	}
	// Output:
	// initial: example.net 2 peers
	// domain: new.example.net
	// upsert: 1 final
	// remove: 2
	// upsert: 3 joined
}

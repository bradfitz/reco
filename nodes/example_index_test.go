package nodes_test

import (
	"fmt"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

func ExampleCountBy() {
	devices := reco.MapData[string, string]("device-owner")
	counts := nodes.CountBy("devices-per-owner", devices, func(_ string, owner string) string { return owner })
	owners := nodes.Distinct("owners-with-devices", counts)
	g := reco.NewGraph()
	if err := g.Register(owners); err != nil {
		panic(err)
	}
	if err := g.Update(func(tx *reco.Tx) error {
		reco.MapPut(tx, devices, "laptop", "alice")
		reco.MapPut(tx, devices, "phone", "alice")
		return nil
	}); err != nil {
		panic(err)
	}
	s, err := reco.Read(g, counts)
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Value().Count("alice"))
	// Removing one device decrements the count without removing its owner.
	if err := g.Update(func(tx *reco.Tx) error { reco.MapDelete(tx, devices, "phone"); return nil }); err != nil {
		panic(err)
	}
	s, err = reco.Read(g, counts)
	if err != nil {
		panic(err)
	}
	u, err := reco.Read(g, owners)
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Value().Count("alice"), u.Value().Contains("alice"))
	// Output:
	// 2
	// 1 true
}

func ExampleGroupCounts() {
	roles := reco.MapData[string, reco.SetSnapshot[string]]("device-roles")
	owners := reco.MapData[string, string]("device-owners")
	counts := nodes.GroupCounts("owner-role-counts", roles, owners)
	g := reco.NewGraph()
	if err := g.Register(counts); err != nil {
		panic(err)
	}
	web := (reco.SetSnapshot[string]{}).WithDelta(reco.SetDelta[string]{Add: []string{"tag:web"}})
	if err := g.Update(func(tx *reco.Tx) error {
		for _, device := range []string{"one", "two"} {
			reco.MapPut(tx, roles, device, web)
			reco.MapPut(tx, owners, device, "alice")
		}
		return nil
	}); err != nil {
		panic(err)
	}
	s, err := reco.Read(g, counts)
	if err != nil {
		panic(err)
	}
	alice, _ := s.Value().Get("alice")
	fmt.Println(alice.Count("tag:web"))
	// An owner transfer touches only this device's memberships, not all of
	// Alice's devices or every observer that might be interested in them.
	if err := g.Update(func(tx *reco.Tx) error { reco.MapPut(tx, owners, "two", "bob"); return nil }); err != nil {
		panic(err)
	}
	s, err = reco.Read(g, counts)
	if err != nil {
		panic(err)
	}
	alice, _ = s.Value().Get("alice")
	bob, _ := s.Value().Get("bob")
	fmt.Println(alice.Count("tag:web"), bob.Count("tag:web"))
	// Output:
	// 2
	// 1 1
}

package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"recontrol/reco"
	"tailscale.com/control/tsp"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

func TestControlServerStreamsRecoUpdates(t *testing.T) {
	s, err := newControlServer()
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s)
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverKey, err := tsp.DiscoverServerKey(ctx, httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	type testNode struct {
		nodeKey    key.NodePrivate
		machineKey key.MachinePrivate
	}
	register := func(hostname string) testNode {
		t.Helper()
		n := testNode{nodeKey: key.NewNode(), machineKey: key.NewMachine()}
		client, err := tsp.NewClient(tsp.ClientOpts{
			ServerURL:  httpServer.URL,
			MachineKey: n.machineKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		client.SetControlPublicKey(serverKey)
		if _, err := client.Register(ctx, tsp.RegisterOpts{
			NodeKey:  n.nodeKey,
			Hostinfo: &tailcfg.Hostinfo{Hostname: hostname},
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}

	a := register("a")
	b := register("b")
	clientB, err := tsp.NewClient(tsp.ClientOpts{
		ServerURL:  httpServer.URL,
		MachineKey: b.machineKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()
	clientB.SetControlPublicKey(serverKey)
	session, err := clientB.Map(ctx, tsp.MapOpts{
		NodeKey:  b.nodeKey,
		Hostinfo: &tailcfg.Hostinfo{Hostname: "b"},
		Stream:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	initial, err := session.Next()
	if err != nil {
		t.Fatal(err)
	}
	if initial.Node == nil || initial.Node.Key != b.nodeKey.Public() {
		t.Fatalf("initial self node = %v; want %v", initial.Node, b.nodeKey.Public())
	}
	if peerByKey(initial.Peers, a.nodeKey.Public()) == nil {
		t.Fatalf("initial response omitted peer A")
	}

	clientA, err := tsp.NewClient(tsp.ClientOpts{
		ServerURL:  httpServer.URL,
		MachineKey: a.machineKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()
	clientA.SetControlPublicKey(serverKey)
	wantDisco := key.NewDisco().Public()
	if err := clientA.SendMapUpdate(ctx, tsp.SendMapUpdateOpts{
		NodeKey:  a.nodeKey,
		DiscoKey: wantDisco,
		Hostinfo: &tailcfg.Hostinfo{Hostname: "a"},
	}); err != nil {
		t.Fatal(err)
	}

	delta, err := session.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Peers) != 0 {
		t.Fatalf("delta contains %d full peers", len(delta.Peers))
	}
	changed := peerByKey(delta.PeersChanged, a.nodeKey.Public())
	if changed == nil || changed.DiscoKey != wantDisco {
		t.Fatalf("peer delta = %v; want DiscoKey %v", changed, wantDisco)
	}
}

func peerByKey(nodes []*tailcfg.Node, want key.NodePublic) *tailcfg.Node {
	for _, n := range nodes {
		if n.Key == want {
			return n
		}
	}
	return nil
}

func TestNodeUpdateQueueCoalescesByKey(t *testing.T) {
	q := newNodeUpdateQueue()
	nk := key.NewNode().Public()
	before := &tailcfg.Node{ID: 1, Key: nk, Name: "before"}
	middle := &tailcfg.Node{ID: 1, Key: nk, Name: "middle"}
	after := &tailcfg.Node{ID: 1, Key: nk, Name: "after"}
	q.add([]reco.MapChange[key.NodePublic, *tailcfg.Node]{
		{Key: nk, Before: before, BeforeValid: true, After: middle, AfterValid: true},
		{Key: nk, Before: middle, BeforeValid: true, After: after, AfterValid: true},
	})
	changes := q.take()
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	if changes[0].Before != before || changes[0].After != after {
		t.Fatalf("coalesced change = %+v", changes[0])
	}
}

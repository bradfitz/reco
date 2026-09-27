package recotestcontrol

import (
	"fmt"
	"net/netip"
	"slices"
	"testing"

	"github.com/bradfitz/reco"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/util/must"
)

func testNodes(tb testing.TB, count int) (*Server, []*tailcfg.Node) {
	tb.Helper()
	s := &Server{Logf: tb.Logf}
	nodes := make([]*tailcfg.Node, count)
	for i := range nodes {
		n := &tailcfg.Node{
			ID: tailcfg.NodeID(i + 1), StableID: tailcfg.StableNodeID(fmt.Sprint(i + 1)),
			Key: key.NewNode().Public(), Machine: key.NewMachine().Public(),
			Hostinfo:  (&tailcfg.Hostinfo{Hostname: fmt.Sprint("node-", i)}).View(),
			Addresses: []netip.Prefix{netip.MustParsePrefix(fmt.Sprintf("100.64.%d.%d/32", (i+1)>>8, (i+1)&255))},
		}
		n.AllowedIPs = slices.Clone(n.Addresses)
		s.UpdateNode(n)
		nodes[i] = n
	}
	return s, nodes
}

func watchGraph(tb testing.TB, s *Server) (mapMeta, *mapWatch) {
	tb.Helper()
	w := newMapWatch()
	s.mu.Lock()
	s.ensureGraphLocked()
	initial, sub, err := reco.SubscribeStruct(s.graph, metaNode, reco.SubscribeOptions{}, w.onChange)
	s.mu.Unlock()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { w.close(); sub.Unsubscribe() })
	return initial.Value().Value(), w
}

func TestCoalescedPeerDelta(t *testing.T) {
	s, ns := testNodes(t, 1000)
	initial, w := watchGraph(t, s)
	for range 10 {
		ns[1].DiscoKey = key.NewDisco().Public()
		s.UpdateNode(ns[1])
	}
	c := w.take()
	if c.ChangeCount() != 1 {
		t.Fatalf("changed entries = %d, want 1", c.ChangeCount())
	}
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, c, true)
	if len(r.Peers) != 0 || len(r.PeersChangedPatch) != 1 || len(r.PeersChanged) != 0 || r.Node != nil || r.PacketFilter != nil {
		t.Fatalf("not a one-peer delta: %+v", r)
	}
	if r.PeersChangedPatch[0].DiscoKey == nil || *r.PeersChangedPatch[0].DiscoKey != ns[1].DiscoKey {
		t.Fatal("lost final disco key")
	}
	old, _ := initial.Nodes.Get(ns[1].Key)
	if !old.DiscoKey.IsZero() {
		t.Fatal("published snapshot mutated")
	}
	if initial.Policy != c.After().Value().Policy {
		t.Fatal("irrelevant node update recomputed policy")
	}
}

func TestPolicyDependency(t *testing.T) {
	s, ns := testNodes(t, 3)
	s.SetUnsignedPeerAPIOnly(ns[2].Key, true)
	initial, w := watchGraph(t, s)
	ns[1].Endpoints = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:123")}
	s.UpdateNode(ns[1])
	c := w.take()
	if c.After().Value().Policy != initial.Policy {
		t.Fatal("endpoint change rebuilt signed-address policy")
	}
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, c, true)
	if r.PacketFilter != nil || len(r.PeersChangedPatch) != 1 || len(r.PeersChanged) != 0 || len(r.Peers) != 0 {
		t.Fatal("endpoint update expanded to full response")
	}
	ns[1].Addresses = []netip.Prefix{netip.MustParsePrefix("100.90.0.1/32")}
	s.UpdateNode(ns[1])
	c = w.take()
	r = deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, c, true)
	if r.PacketFilter == nil || c.After().Value().Policy == initial.Policy {
		t.Fatal("address change did not rebuild policy")
	}
	if !slices.Contains(r.PacketFilter[0].SrcIPs, "100.90.0.1/32") {
		t.Fatal("new address missing from policy")
	}
}

func TestLastPeerRemoval(t *testing.T) {
	s, ns := testNodes(t, 2)
	_, w := watchGraph(t, s)
	s.mu.Lock()
	s.deleteNodeLocked(ns[1].Key)
	s.mu.Unlock()
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if !slices.Equal(r.PeersRemoved, []tailcfg.NodeID{ns[1].ID}) {
		t.Fatalf("removals = %v", r.PeersRemoved)
	}
}

func TestRotatedKeyRetirement(t *testing.T) {
	s, ns := testNodes(t, 2)
	replacement := ns[1].Clone()
	replacement.Key = key.NewNode().Public()
	s.UpdateNode(replacement)
	_, w := watchGraph(t, s)
	s.mu.Lock()
	s.retireNodeKeyLocked(ns[1].Key, replacement.Key)
	s.mu.Unlock()
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if len(r.PeersRemoved) != 0 || len(r.PeersChanged) != 1 || r.PeersChanged[0].Key != replacement.Key {
		t.Fatalf("key retirement removed the rotated peer: %+v", r)
	}
}

func TestConfigRefreshWithLastPeerRemoval(t *testing.T) {
	s, ns := testNodes(t, 2)
	_, w := watchGraph(t, s)
	s.mu.Lock()
	s.deleteNodeLocked(ns[1].Key)
	s.mu.Unlock()
	s.AddDNSRecords(tailcfg.DNSRecord{Name: "example.test", Type: "A", Value: "100.64.0.1"})
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if len(r.Peers) != 0 || !slices.Equal(r.PeersRemoved, []tailcfg.NodeID{ns[1].ID}) || r.DNSConfig == nil {
		t.Fatalf("empty full peer list must retain explicit removal: %+v", r)
	}
}

func TestMailboxBound(t *testing.T) {
	s, _ := testNodes(t, 1)
	initial, w := watchGraph(t, s)
	var delta reco.MapDelta[key.NodePublic, *tailcfg.Node]
	for i := range 10001 {
		k := key.NewNode().Public()
		delta.Put = append(delta.Put, reco.MapEntry[key.NodePublic, *tailcfg.Node]{Key: k, Value: &tailcfg.Node{Key: k, ID: tailcfg.NodeID(i + 2)}})
	}
	// A single large settled batch is enough to exceed the mailbox limit.
	before := reco.NewStruct(initial)
	after := before.WithDelta(reco.StructDelta[mapMeta]{nodesField.Update(func(m reco.MapSnapshot[key.NodePublic, *tailcfg.Node]) reco.MapSnapshot[key.NodePublic, *tailcfg.Node] {
		return m.WithDelta(delta)
	})})
	w.onChange(reco.StructEvent[mapMeta]{Changes: after.ChangesSince(before)})
	select {
	case <-w.done:
	default:
		t.Fatal("unbounded slow-consumer mailbox")
	}
	if w.take().ChangeCount() != 0 {
		t.Fatal("closed mailbox retained pending batch")
	}
}

func TestSelfAndConfigChanges(t *testing.T) {
	s, ns := testNodes(t, 3)
	_, w := watchGraph(t, s)
	ns[0].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[0])
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if r.Node == nil || r.Node.DiscoKey != ns[0].DiscoKey || len(r.PeersChanged) != 0 || r.PacketFilter != nil {
		t.Fatal("not a self-only delta")
	}
	s.SetExpireAllNodes(true)
	r = deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if r.Node.KeyExpiry.IsZero() {
		t.Fatal("expiry change not delivered")
	}
}

func TestHostnameDNSDependency(t *testing.T) {
	s, ns := testNodes(t, 2)
	s.mu.Lock()
	s.MagicDNSDomain = "demo.example"
	s.DNSConfig = &tailcfg.DNSConfig{}
	s.publishConfigLocked()
	s.mu.Unlock()
	_, w := watchGraph(t, s)
	hi := ns[0].Hostinfo.AsStruct()
	hi.Hostname = "renamed"
	ns[0].Hostinfo = hi.View()
	s.UpdateNode(ns[0])
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if r.DNSConfig == nil || !slices.Contains(r.DNSConfig.CertDomains, "renamed.demo.example") {
		t.Fatal("hostname delta did not update DNS certificate name")
	}
	if len(r.Peers) != 0 || r.PacketFilter != nil {
		t.Fatal("hostname change caused full refresh")
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s, ns := testNodes(t, 2)
	route := netip.MustParsePrefix("10.0.0.0/8")
	s.SetSubnetRoutes(ns[1].Key, []netip.Prefix{route})
	req := &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}
	r := must.Get(s.MapResponse(req))
	r.Peers[0].PrimaryRoutes[0] = netip.Prefix{}
	r.Peers[0].Addresses[0] = netip.Prefix{}
	r.PacketFilter[0].DstPorts[0].IP = "broken"
	r = must.Get(s.MapResponse(req))
	if r.Peers[0].PrimaryRoutes[0] != route || !r.Peers[0].Addresses[0].IsValid() || r.PacketFilter[0].DstPorts[0].IP != "*" {
		t.Fatal("response mutated graph or configuration")
	}
	ns[1].Addresses[0] = netip.Prefix{}
	if !s.Node(ns[1].Key).Addresses[0].IsValid() {
		t.Fatal("UpdateNode retained caller-owned mutable memory")
	}
}

func TestSinglePeerUpdateAllocations(t *testing.T) {
	// HAMT depth can grow, but neither graph delivery nor encoding should
	// allocate a new object for every unchanged peer. Cover the allowlist too.
	for _, unsigned := range []bool{false, true} {
		t.Run(fmt.Sprint("unsigned=", unsigned), func(t *testing.T) {
			measure := func(count int) float64 {
				s, ns := testNodes(t, count)
				if unsigned {
					s.SetUnsignedPeerAPIOnly(ns[count-1].Key, true)
				}
				_, w := watchGraph(t, s)
				req := &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}
				return testing.AllocsPerRun(30, func() {
					ns[1].HomeDERP++
					s.UpdateNode(ns[1])
					r := deltaResponse(req, w.take(), true)
					if len(r.PeersChangedPatch) != 1 || len(r.PeersChanged) != 0 || r.PacketFilter != nil {
						t.Fatal("not a point delta")
					}
				})
			}
			small, large := measure(100), measure(10000)
			t.Logf("allocations at 100/10000 nodes: %.0f / %.0f", small, large)
			if large > 3*small {
				t.Fatalf("point update allocations scale with full map: %.0f -> %.0f", small, large)
			}
		})
	}
}

func BenchmarkPeerDelta(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s, ns := testNodes(b, count)
			_, w := watchGraph(b, s)
			req := &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ns[1].HomeDERP++
				s.UpdateNode(ns[1])
				deltaResponse(req, w.take(), true)
			}
		})
	}
}

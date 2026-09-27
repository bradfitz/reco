package recotestcontrol

import (
	"encoding/json"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"

	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/nodecap"
	"tailscale.com/types/dnstype"
	"tailscale.com/types/key"
	"tailscale.com/util/must"
)

// Apply the public protocol's patch semantics after an actual JSON round trip.
// In particular, nil slice/map/pointer fields retain the previous value.
func applyPeerPatch(n *tailcfg.Node, p *tailcfg.PeerChange) {
	if n.ID != p.NodeID {
		panic("unknown peer")
	}
	if p.DERPRegion != 0 {
		n.HomeDERP = p.DERPRegion
	}
	if p.Cap != 0 {
		n.Cap = p.Cap
	}
	if p.Endpoints != nil {
		n.Endpoints = p.Endpoints
	}
	if p.CapMap != nil {
		n.CapMap = p.CapMap
	}
	if p.Key != nil {
		n.Key = *p.Key
	}
	if p.DiscoKey != nil {
		n.DiscoKey = *p.DiscoKey
	}
	if p.KeySignature != nil {
		n.KeySignature = p.KeySignature
	}
	if p.KeyExpiry != nil {
		n.KeyExpiry = *p.KeyExpiry
	}
	if p.Online != nil {
		n.Online = p.Online
	}
	if p.LastSeen != nil {
		n.LastSeen = p.LastSeen
	}
}

func canonicalPeer(n *tailcfg.Node) *tailcfg.Node {
	n = n.Clone()
	if len(n.Endpoints) == 0 {
		n.Endpoints = nil
	}
	if len(n.CapMap) == 0 {
		n.CapMap = nil
	}
	if len(n.KeySignature) == 0 {
		n.KeySignature = nil
	}
	return n
}

func TestPeerPatch(t *testing.T) {
	base := &tailcfg.Node{
		ID: 7, StableID: "peer-7", Name: "peer.example.",
		Key: key.NewNode().Public(), DiscoKey: key.NewDisco().Public(),
		Endpoints: []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:123")},
		Addresses: []netip.Prefix{netip.MustParsePrefix("100.64.0.7/32")},
		HomeDERP:  1, Cap: 100, KeySignature: []byte{1, 2, 3},
		KeyExpiry: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		CapMap:    tailcfg.NodeCapMap{nodecap.HTTPS: {"true"}},
		Online:    new(true), LastSeen: new(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	tests := []struct {
		name       string
		edit       func(*tailcfg.Node)
		field      string // empty means a whole-node replacement is required
		minVersion tailcfg.CapabilityVersion
	}{
		{"derp", func(n *tailcfg.Node) { n.HomeDERP = 2 }, "DERPRegion", 33},
		{"endpoints", func(n *tailcfg.Node) { n.Endpoints[0] = netip.MustParseAddrPort("192.0.2.2:456") }, "Endpoints", 33},
		{"endpoints-nil", func(n *tailcfg.Node) { n.Endpoints = nil }, "Endpoints", 33},
		{"endpoints-empty", func(n *tailcfg.Node) { n.Endpoints = []netip.AddrPort{} }, "Endpoints", 33},
		{"key", func(n *tailcfg.Node) { n.Key = key.NewNode().Public() }, "Key", 36},
		{"key-zero", func(n *tailcfg.Node) { n.Key = key.NodePublic{} }, "Key", 36},
		{"disco", func(n *tailcfg.Node) { n.DiscoKey = key.NewDisco().Public() }, "DiscoKey", 36},
		{"disco-zero", func(n *tailcfg.Node) { n.DiscoKey = key.DiscoPublic{} }, "DiscoKey", 36},
		{"expiry-zero", func(n *tailcfg.Node) { n.KeyExpiry = time.Time{} }, "KeyExpiry", 36},
		{"online-false", func(n *tailcfg.Node) { n.Online = new(false) }, "Online", 36},
		{"last-seen", func(n *tailcfg.Node) { n.LastSeen = new(n.LastSeen.Add(time.Hour)) }, "LastSeen", 36},
		{"last-seen-zero", func(n *tailcfg.Node) { n.LastSeen = new(time.Time{}) }, "LastSeen", 36},
		{"signature", func(n *tailcfg.Node) { n.KeySignature = []byte{4, 5} }, "KeySignature", 40},
		{"signature-clear", func(n *tailcfg.Node) { n.KeySignature = nil }, "KeySignature", 40},
		{"cap-version", func(n *tailcfg.Node) { n.Cap++ }, "Cap", 54},
		{"cap-map", func(n *tailcfg.Node) { n.CapMap[nodecap.HTTPS] = []tailcfg.RawMessage{"false"} }, "CapMap", 74},
		{"cap-map-nil", func(n *tailcfg.Node) { n.CapMap = nil }, "CapMap", 74},
		{"cap-map-empty", func(n *tailcfg.Node) { n.CapMap = tailcfg.NodeCapMap{} }, "CapMap", 74},
		{"derp-zero", func(n *tailcfg.Node) { n.HomeDERP = 0 }, "", 0},
		{"cap-zero", func(n *tailcfg.Node) { n.Cap = 0 }, "", 0},
		{"online-nil", func(n *tailcfg.Node) { n.Online = nil }, "", 0},
		{"last-seen-nil", func(n *tailcfg.Node) { n.LastSeen = nil }, "", 0},
		{"name", func(n *tailcfg.Node) { n.Name = "renamed.example." }, "", 0},
		{"address", func(n *tailcfg.Node) { n.Addresses[0] = netip.MustParsePrefix("100.64.0.8/32") }, "", 0},
		{"tags", func(n *tailcfg.Node) { n.Tags = []string{"tag:test"} }, "", 0},
		{"hostinfo", func(n *tailcfg.Node) { n.Hostinfo = (&tailcfg.Hostinfo{Hostname: "new"}).View() }, "", 0},
		{"routes", func(n *tailcfg.Node) { n.PrimaryRoutes = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")} }, "", 0},
		{"exit-dns", func(n *tailcfg.Node) { n.ExitNodeDNSResolvers = []*dnstype.Resolver{{Addr: "1.1.1.1"}} }, "", 0},
		{"mixed", func(n *tailcfg.Node) { n.DiscoKey = key.NewDisco().Public(); n.Name = "new" }, "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			after := base.Clone()
			tt.edit(after)
			originalBefore, originalAfter := base.Clone(), after.Clone()
			p, ok := peerPatch(base, after, tailcfg.CurrentCapabilityVersion)
			if tt.field == "" {
				if ok {
					t.Fatalf("unsupported update became a patch: %+v", p)
				}
				return
			}
			if !ok || p == nil {
				t.Fatal("expected a patch")
			}
			r := &tailcfg.MapResponse{PeersChangedPatch: []*tailcfg.PeerChange{p}}
			wire := must.Get(new(Server).encode(false, r))
			var raw struct{ PeersChangedPatch []map[string]json.RawMessage }
			if err := json.Unmarshal(wire, &raw); err != nil {
				t.Fatal(err)
			}
			fields := raw.PeersChangedPatch[0]
			if len(fields) != 2 || fields["NodeID"] == nil || fields[tt.field] == nil {
				t.Fatalf("unexpected patch fields: %s", wire)
			}
			var decoded tailcfg.MapResponse
			if err := json.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			got := base.Clone()
			applyPeerPatch(got, decoded.PeersChangedPatch[0])
			if !reflect.DeepEqual(canonicalPeer(got), canonicalPeer(after)) {
				t.Fatalf("wire patch didn't reconstruct peer:\n got %+v\nwant %+v\nwire %s", got, after, wire)
			}
			if !reflect.DeepEqual(base, originalBefore) || !reflect.DeepEqual(after, originalAfter) {
				t.Fatal("diff mutated input")
			}
			if _, ok := peerPatch(base, after, tt.minVersion-1); ok {
				t.Fatal("sent a patch to an unsupported client")
			}
			if _, ok := peerPatch(base, after, tt.minVersion); !ok {
				t.Fatal("patch rejected at first supported version")
			}
		})
	}
}

func TestPeerPatchNilToValueAndNoop(t *testing.T) {
	before := &tailcfg.Node{ID: 1}
	after := before.Clone()
	after.Online = new(false)
	after.LastSeen = new(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	p, ok := peerPatch(before, after, 36)
	if !ok || p == nil || p.Online == nil || *p.Online || p.LastSeen == nil {
		t.Fatalf("nil-to-value patch: %+v, %v", p, ok)
	}
	if p, ok := peerPatch(after, after.Clone(), tailcfg.CurrentCapabilityVersion); !ok || p != nil {
		t.Fatalf("no-op = %+v, %v", p, ok)
	}
}

func TestDeltaPatchSelection(t *testing.T) {
	s, ns := testNodes(t, 4)
	_, w := watchGraph(t, s)
	ns[1].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[1])
	ns[2].Name = "renamed.example."
	s.UpdateNode(ns[2])
	s.mu.Lock()
	s.deleteNodeLocked(ns[3].Key)
	s.mu.Unlock()
	fresh := ns[3].Clone()
	fresh.ID, fresh.StableID, fresh.Key = 5, "new-peer", key.NewNode().Public()
	s.UpdateNode(fresh)
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if len(r.Peers) != 0 || len(r.PeersChangedPatch) != 1 || r.PeersChangedPatch[0].NodeID != ns[1].ID {
		t.Fatalf("expected one existing-peer patch: %+v", r)
	}
	if len(r.PeersChanged) != 2 || r.PeersChanged[0].ID != ns[2].ID || r.PeersChanged[1].ID != fresh.ID {
		t.Fatalf("new/unsupported peers must be full sorted replacements: %+v", r.PeersChanged)
	}
	if !slices.Equal(r.PeersRemoved, []tailcfg.NodeID{ns[3].ID}) {
		t.Fatalf("removals = %v", r.PeersRemoved)
	}
}

func TestCoalescedPatchAndNoop(t *testing.T) {
	s, ns := testNodes(t, 2)
	_, w := watchGraph(t, s)
	req := &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}
	original := ns[1].Clone()
	ns[1].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[1])
	s.UpdateNode(original)
	if r := deltaResponse(req, w.take(), true); !emptyDelta(r) {
		t.Fatalf("A -> B -> A emitted a change: %+v", r)
	}
	ns[1].HomeDERP = 2
	s.UpdateNode(ns[1])
	ns[1].Endpoints = []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:123")}
	s.UpdateNode(ns[1])
	r := deltaResponse(req, w.take(), true)
	if len(r.PeersChangedPatch) != 1 {
		t.Fatalf("expected one composed patch: %+v", r)
	}
	p := r.PeersChangedPatch[0]
	if p.DERPRegion != 2 || p.DiscoKey == nil || *p.DiscoKey != ns[1].DiscoKey || !slices.Equal(p.Endpoints, ns[1].Endpoints) {
		t.Fatalf("lost a coalesced field change: %+v", p)
	}
}

func TestPatchAfterPersonalization(t *testing.T) {
	s, ns := testNodes(t, 2)
	s.mu.Lock()
	s.AllOnline = true
	s.publishConfigLocked()
	s.mu.Unlock()
	_, w := watchGraph(t, s)
	ns[1].Online = new(false) // Hidden by AllOnline.
	ns[1].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[1])
	r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
	if len(r.PeersChangedPatch) != 1 || r.PeersChangedPatch[0].Online != nil || r.PeersChangedPatch[0].DiscoKey == nil {
		t.Fatalf("patch ignored recipient-specific rendering: %+v", r)
	}
}

func TestPeerRefreshForUnrepresentableClears(t *testing.T) {
	for _, field := range []string{"derp", "cap", "online", "last-seen"} {
		t.Run(field, func(t *testing.T) {
			s, ns := testNodes(t, 3)
			n := ns[1]
			n.HomeDERP, n.Cap, n.Online, n.LastSeen = 1, 100, new(true), new(time.Now())
			s.UpdateNode(n)
			_, w := watchGraph(t, s)
			switch field {
			case "derp":
				n.HomeDERP = 0
			case "cap":
				n.Cap = 0
			case "online":
				n.Online = nil
			case "last-seen":
				n.LastSeen = nil
			}
			s.UpdateNode(n)
			r := deltaResponse(&tailcfg.MapRequest{NodeKey: ns[0].Key, Version: tailcfg.CurrentCapabilityVersion}, w.take(), true)
			if len(r.Peers) != 2 || len(r.PeersChanged) != 0 || len(r.PeersChangedPatch) != 0 {
				t.Fatalf("lossy client patchification was not bypassed: %+v", r)
			}
		})
	}
}

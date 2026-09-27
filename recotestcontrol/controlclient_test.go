package recotestcontrol

import (
	"context"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"tailscale.com/control/controlclient"
	"tailscale.com/health"
	"tailscale.com/net/netmon"
	"tailscale.com/net/tsdial"
	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/nodecap"
	"tailscale.com/types/key"
	"tailscale.com/types/netmap"
	"tailscale.com/util/eventbus/eventbustest"
	"tailscale.com/util/must"
)

type netmapCollector struct {
	ctx     context.Context
	updates chan *netmap.NetworkMap
}

func (c *netmapCollector) UpdateFullNetmap(nm *netmap.NetworkMap) {
	select {
	case c.updates <- nm:
	case <-c.ctx.Done():
	}
}

// Exercise the real client, including its normalization and patchification,
// through Noise, compression, and streaming. Protocol-only patch application
// tests would miss the client's lossy replacement-to-patch conversion.
func TestControlClientPeerPatches(t *testing.T) {
	s, ns := testNodes(t, 1)
	n := ns[0]
	n.HomeDERP, n.Cap = 1, 100
	n.Online = new(true)
	n.LastSeen = new(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	n.KeyExpiry = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	n.KeySignature = []byte{1, 2, 3}
	n.DiscoKey = key.NewDisco().Public()
	s.UpdateNode(n)
	s.HTTPTestServer = httptest.NewServer(s)
	t.Cleanup(s.HTTPTestServer.Close)
	bus := eventbustest.NewBus(t)
	dialer := tsdial.NewDialer(netmon.NewStatic())
	dialer.SetBus(bus)
	t.Cleanup(func() { dialer.Close() })
	machineKey := key.NewMachine()
	c := must.Get(controlclient.NewDirect(controlclient.Options{
		ServerURL: s.BaseURL(), Hostinfo: &tailcfg.Hostinfo{Hostname: "patch-reader", BackendLogID: "test"},
		GetMachinePrivateKey: func() (key.MachinePrivate, error) { return machineKey, nil },
		Dialer:               dialer, Bus: bus, HealthTracker: new(health.Tracker), Logf: t.Logf,
	}))
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	if url, err := c.TryLogin(ctx, controlclient.LoginDefault); err != nil || url != "" {
		t.Fatalf("login: %q, %v", url, err)
	}
	collector := &netmapCollector{ctx: ctx, updates: make(chan *netmap.NetworkMap, 1)}
	done := make(chan error, 1)
	go func() { done <- c.PollNetMap(ctx, collector) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("map poll did not stop")
		}
	})
	check := func() {
		t.Helper()
		select {
		case nm := <-collector.updates:
			for _, p := range nm.Peers {
				if p.ID() != n.ID {
					continue
				}
				got := p.AsStruct()
				// Restrict comparison to the fields managed by peer patches;
				// the client also computes names and other local-only fields.
				state := func(p *tailcfg.Node) *tailcfg.Node {
					return canonicalPeer(&tailcfg.Node{ID: p.ID, HomeDERP: p.HomeDERP, Cap: p.Cap, CapMap: p.CapMap,
						Key: p.Key, DiscoKey: p.DiscoKey, KeyExpiry: p.KeyExpiry, KeySignature: p.KeySignature,
						Endpoints: p.Endpoints, Online: p.Online, LastSeen: p.LastSeen})
				}
				if !reflect.DeepEqual(state(got), state(n)) {
					t.Fatalf("client retained stale peer fields:\n got %+v\nwant %+v", state(got), state(n))
				}
				return
			}
			t.Fatal("peer disappeared")
		case <-ctx.Done():
			t.Fatal("waiting for map update:", ctx.Err())
		}
	}
	check()
	for _, tt := range []struct {
		name string
		edit func(*tailcfg.Node)
	}{
		{"endpoints", func(n *tailcfg.Node) { n.Endpoints = []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:123")} }},
		{"endpoints-clear", func(n *tailcfg.Node) { n.Endpoints = nil }},
		{"caps", func(n *tailcfg.Node) { n.CapMap = tailcfg.NodeCapMap{nodecap.HTTPS: nil} }},
		{"caps-clear", func(n *tailcfg.Node) { n.CapMap = nil }},
		{"signature-clear", func(n *tailcfg.Node) { n.KeySignature = nil }},
		{"expiry-clear", func(n *tailcfg.Node) { n.KeyExpiry = time.Time{} }},
		{"disco-clear", func(n *tailcfg.Node) { n.DiscoKey = key.DiscoPublic{} }},
		{"offline", func(n *tailcfg.Node) { n.Online = new(false) }},
		{"online-unknown", func(n *tailcfg.Node) { n.Online = nil }},
		{"online-again", func(n *tailcfg.Node) { n.Online = new(true) }},
		{"last-seen-clear", func(n *tailcfg.Node) { n.LastSeen = nil }},
		{"derp-clear", func(n *tailcfg.Node) { n.HomeDERP = 0 }},
		{"cap-version-clear", func(n *tailcfg.Node) { n.Cap = 0 }},
	} {
		t.Log(tt.name)
		tt.edit(n)
		s.UpdateNode(n)
		check()
	}
}

// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package recotestcontrol

import (
	"cmp"
	"net/netip"
	"slices"
	"sort"
	"time"

	"github.com/bradfitz/reco"
	"tailscale.com/net/netaddr"
	"tailscale.com/net/tsaddr"
	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/nodecap"
	"tailscale.com/types/key"
	"tailscale.com/types/opt"
	"tailscale.com/util/mak"
	"tailscale.com/util/must"
)

// MapResponse returns a full response from the current immutable graph value.
func (s *Server) MapResponse(req *tailcfg.MapRequest) (*tailcfg.MapResponse, error) {
	s.mu.Lock()
	s.ensureGraphLocked()
	meta := must.Get(reco.Read(s.graph, metaNode)).Value().Value()
	s.mu.Unlock()
	return fullResponse(req, meta), nil
}

// fullResponse renders an immutable settled graph snapshot.
func fullResponse(req *tailcfg.MapRequest, meta mapMeta) (res *tailcfg.MapResponse) {
	s := meta.Config
	nk := req.NodeKey
	n, _ := meta.Nodes.Get(nk)
	node := n
	if node == nil {
		// The requested key was retired.
		return nil
	}

	node = selfFor(s, node)
	sshPolicy := s.SSHPolicy.Clone()
	t := time.Date(2020, 8, 3, 0, 0, 0, 1, time.UTC)

	res = &tailcfg.MapResponse{
		Node:            node,
		DERPMap:         s.DERPMap.Clone(),
		Domain:          domain,
		CollectServices: cmp.Or(s.CollectServices, opt.True),
		PacketFilter:    meta.Policy.cloneRules(),
		DNSConfig:       dnsFor(s, node),
		SSHPolicy:       sshPolicy,
		ControlTime:     &t,
	}

	for _, p := range meta.Nodes.All() {
		if p.StableID != node.StableID {
			res.Peers = append(res.Peers, peerFor(s, node.Key, p))
		}
	}

	sort.Slice(res.Peers, func(i, j int) bool {
		return res.Peers[i].ID < res.Peers[j].ID
	})
	for _, p := range meta.Profiles.All() {
		res.UserProfiles = append(res.UserProfiles, p)
	}

	if s.tkaInfo != nil {
		info := *s.tkaInfo
		res.TKAInfo = &info
	}
	return res
}

func peerFor(s *renderConfig, nk key.NodePublic, n *tailcfg.Node) *tailcfg.Node {
	p := n.Clone()
	nodeMasqs := s.masquerades[nk]
	jailed := s.peerIsJailed[nk]
	unsignedNodes := s.nodeUnsignedPeerAPIOnly
	if masqIP := nodeMasqs[p.Key]; masqIP.IsValid() {
		if masqIP.Is6() {
			p.SelfNodeV6MasqAddrForThisPeer = new(masqIP)
		} else {
			p.SelfNodeV4MasqAddrForThisPeer = new(masqIP)
		}
	}
	p.IsJailed = jailed[p.Key]
	p.UnsignedPeerAPIOnly = unsignedNodes[p.Key]

	peerAddress := s.masquerades[p.Key][nk]
	routes := s.nodeSubnetRoutes[p.Key]
	peerCapMap := cloneMap(s.nodeCapMaps[p.Key], slices.Clone[[]tailcfg.RawMessage])
	if peerCapMap != nil {
		p.CapMap = peerCapMap
	}
	if peerAddress.IsValid() {
		if peerAddress.Is6() {
			p.Addresses[1] = netip.PrefixFrom(peerAddress, peerAddress.BitLen())
			p.AllowedIPs[1] = netip.PrefixFrom(peerAddress, peerAddress.BitLen())
		} else {
			p.Addresses[0] = netip.PrefixFrom(peerAddress, peerAddress.BitLen())
			p.AllowedIPs[0] = netip.PrefixFrom(peerAddress, peerAddress.BitLen())
		}
	}
	if len(routes) > 0 {
		p.PrimaryRoutes = slices.Clone(routes)
		p.AllowedIPs = append(p.AllowedIPs, routes...)
	}
	if s.AllOnline {
		p.Online = new(true)
	}
	return p
}

// deltaResponse translates only touched graph entries. Global policy changes,
// but not endpoint updates, can need a full refresh.
func deltaResponse(req *tailcfg.MapRequest, changes reco.StructChanges[mapMeta]) *tailcfg.MapResponse {
	meta := changes.After().Value()
	if _, _, changed := configField.Change(changes); changed {
		res := fullResponse(req, meta)
		if res != nil {
			addRemovals(res, req, changes)
		}
		return res
	}
	self, ok := meta.Nodes.Get(req.NodeKey)
	if !ok {
		return nil
	}
	res := new(tailcfg.MapResponse)
	if _, policy, changed := policyField.Change(changes); changed {
		res.PacketFilter = policy.cloneRules()
	}
	for c := range reco.MapFieldChanges(nodesField, changes) {
		if c.Key == req.NodeKey && c.AfterValid {
			res.Node = selfFor(meta.Config, c.After)
			if meta.Config.MagicDNSDomain != "" && (!c.BeforeValid || hostname(c.Before) != hostname(c.After)) {
				res.DNSConfig = dnsFor(meta.Config, c.After)
			}
		} else if c.AfterValid && c.After.StableID != self.StableID {
			res.PeersChanged = append(res.PeersChanged, peerFor(meta.Config, req.NodeKey, c.After))
		}
	}
	addRemovals(res, req, changes)
	for c := range reco.MapFieldChanges(profilesField, changes) {
		if c.AfterValid {
			res.UserProfiles = append(res.UserProfiles, c.After)
		}
	}
	sort.Slice(res.PeersChanged, func(i, j int) bool { return res.PeersChanged[i].ID < res.PeersChanged[j].ID })
	return res
}

func hostname(n *tailcfg.Node) string {
	if n.Hostinfo.Valid() {
		return n.Hostinfo.Hostname()
	}
	return ""
}

func dnsFor(c *renderConfig, n *tailcfg.Node) *tailcfg.DNSConfig {
	dns := c.DNSConfig.Clone()
	if dns != nil && c.MagicDNSDomain != "" {
		dns.CertDomains = append(dns.CertDomains, hostname(n)+"."+c.MagicDNSDomain)
	}
	return dns
}

func addRemovals(res *tailcfg.MapResponse, req *tailcfg.MapRequest, changes reco.StructChanges[mapMeta]) {
	replacements := make(map[tailcfg.NodeID]bool)
	for c := range reco.MapFieldChanges(nodesField, changes) {
		if c.AfterValid {
			replacements[c.After.ID] = true
		}
	}
	self, _ := changes.Before().Value().Nodes.Get(req.NodeKey)
	for c := range reco.MapFieldChanges(nodesField, changes) {
		if !c.AfterValid && c.BeforeValid && !replacements[c.Before.ID] && (self == nil || c.Before.StableID != self.StableID) {
			res.PeersRemoved = append(res.PeersRemoved, c.Before.ID)
		}
	}
}

// selfFor renders just the requesting node; it never walks the peer map.
func selfFor(s *renderConfig, n *tailcfg.Node) *tailcfg.Node {
	node := n.Clone()
	nk := node.Key
	nodeCapMap := cloneMap(s.nodeCapMaps[nk], slices.Clone[[]tailcfg.RawMessage])

	node.CapMap = nodeCapMap
	node.Capabilities = append(node.Capabilities, nodecap.DisableUPnP)
	if s.SSHPolicy != nil {
		mak.Set(&node.CapMap, nodecap.SSH, nil)
	}

	v4Prefix := netip.PrefixFrom(netaddr.IPv4(100, 64, uint8(node.ID>>8), uint8(node.ID)), 32)
	v6Prefix := netip.PrefixFrom(tsaddr.Tailscale4To6(v4Prefix.Addr()), 128)

	node.Addresses = []netip.Prefix{
		v4Prefix,
		v6Prefix,
	}

	node.PrimaryRoutes = slices.Clone(s.nodeSubnetRoutes[nk])
	node.AllowedIPs = append(node.Addresses, s.nodeSubnetRoutes[nk]...)

	if s.allExpired {
		node.KeyExpiry = time.Now().Add(-time.Minute)
	}

	return node
}

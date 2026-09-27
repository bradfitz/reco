package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/bradfitz/reco"
	"golang.org/x/net/http2"
	"tailscale.com/control/controlhttp/controlhttpserver"
	"tailscale.com/net/netaddr"
	"tailscale.com/net/tsaddr"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/opt"
	"tailscale.com/util/zstdframe"
)

const (
	domain        = "recontrol.example.net"
	requestLimit  = 1 << 20
	maxMapMessage = 256 << 20
)

var controlTime = time.Date(2020, 8, 3, 0, 0, 0, 1, time.UTC)

// controlServer is an intentionally small in-memory control plane. Node state
// is authoritative reco data. Full MapResponses are built only when a stream
// starts; steady-state responses come directly from reco map mutations.
type controlServer struct {
	graph     *reco.Graph
	nodes     reco.Node[reco.MapSnapshot[key.NodePublic, *tailcfg.Node]]
	responses reco.Node[reco.MapSnapshot[key.NodePublic, *tailcfg.Node]]

	muxOnce sync.Once
	mux     *http.ServeMux

	mu       sync.Mutex
	noiseKey key.MachinePrivate
	nextID   tailcfg.NodeID
}

func newControlServer() (*controlServer, error) {
	nodes := reco.MapData[key.NodePublic, *tailcfg.Node]("nodes")
	type responseDeps struct {
		Nodes reco.Dep[reco.MapSnapshot[key.NodePublic, *tailcfg.Node]]
	}
	// The response node preserves the persistent node snapshot and its exact
	// point mutations. Computing it is O(1); materializing a full wire response
	// is deferred until a client starts a stream.
	responses := reco.Func("map-response-state", struct {
		Nodes reco.Node[reco.MapSnapshot[key.NodePublic, *tailcfg.Node]]
	}{Nodes: nodes}, func(_ reco.Eval, in responseDeps) reco.Result[reco.MapSnapshot[key.NodePublic, *tailcfg.Node]] {
		return reco.OK(in.Nodes.Value())
	})
	g := reco.NewGraph()
	if err := g.Register(responses); err != nil {
		return nil, err
	}
	return &controlServer{
		graph:     g,
		nodes:     nodes,
		responses: responses,
		noiseKey:  key.NewMachine(),
	}, nil
}

func buildInitialMapResponse(nodeKey key.NodePublic, nodes reco.MapSnapshot[key.NodePublic, *tailcfg.Node]) (*tailcfg.MapResponse, error) {
	self, ok := nodes.Get(nodeKey)
	if !ok {
		return nil, errors.New("node not found")
	}
	res := &tailcfg.MapResponse{
		Node:            mapResponseNode(self),
		DERPMap:         &tailcfg.DERPMap{},
		Domain:          domain,
		CollectServices: opt.True,
		PacketFilter:    slices.Clone(tailcfg.FilterAllowAll),
		ControlTime:     new(controlTime),
	}
	var all []*tailcfg.Node
	nodes.Range(func(k key.NodePublic, n *tailcfg.Node) bool {
		all = append(all, n)
		if k != nodeKey {
			res.Peers = append(res.Peers, mapResponseNode(n))
		}
		return true
	})
	res.UserProfiles = userProfiles(all)
	sort.Slice(res.Peers, func(i, j int) bool { return res.Peers[i].ID < res.Peers[j].ID })
	return res, nil
}

func mapResponseNode(n *tailcfg.Node) *tailcfg.Node {
	n = n.Clone()
	if !slices.Contains(n.Capabilities, tailcfg.NodeAttrDisableUPnP) {
		n.Capabilities = append(n.Capabilities, tailcfg.NodeAttrDisableUPnP)
	}
	return n
}

func userProfiles(nodes []*tailcfg.Node) []tailcfg.UserProfile {
	seen := map[tailcfg.UserID]bool{}
	var ret []tailcfg.UserProfile
	for _, n := range nodes {
		if seen[n.User] {
			continue
		}
		seen[n.User] = true
		ret = append(ret, userProfile(n.User))
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].ID < ret[j].ID })
	return ret
}

func userProfile(id tailcfg.UserID) tailcfg.UserProfile {
	return tailcfg.UserProfile{
		ID:          id,
		LoginName:   fmt.Sprintf("user-%d@%s", id, domain),
		DisplayName: fmt.Sprintf("User %d", id),
	}
}

func (s *controlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.muxOnce.Do(s.initMux)
	s.mux.ServeHTTP(w, r)
}

func (s *controlServer) initMux() {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/key", s.serveKey)
	s.mux.HandleFunc("/ts2021", s.serveNoiseUpgrade)
	s.mux.HandleFunc("/machine/", s.serveMachine)
	s.mux.HandleFunc("/generate_204", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *controlServer) serveKey(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&tailcfg.OverTLSPublicKeyResponse{PublicKey: s.noiseKey.Public()}); err != nil {
		log.Printf("encoding key response: %v", err)
	}
}

type peerMachineKey struct{}

func (s *controlServer) serveNoiseUpgrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "POST or GET required", http.StatusMethodNotAllowed)
		return
	}
	conn, err := controlhttpserver.AcceptHTTP(r.Context(), w, r, s.noiseKey, nil)
	if err != nil {
		log.Printf("accepting Noise connection: %v", err)
		return
	}
	defer conn.Close()

	var h2 http2.Server
	h2.ServeConn(conn, &http2.ServeConnOpts{
		Context: context.WithValue(r.Context(), peerMachineKey{}, conn.Peer()),
		BaseConfig: &http.Server{
			Handler: s.mux,
		},
	})
}

func (s *controlServer) serveMachine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	machine, ok := r.Context().Value(peerMachineKey{}).(key.MachinePublic)
	if !ok {
		http.Error(w, "missing machine identity", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/machine/register":
		s.serveRegister(w, r, machine)
	case "/machine/map":
		s.serveMap(w, r, machine)
	case "/machine/update-health":
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (s *controlServer) serveRegister(w http.ResponseWriter, r *http.Request, machine key.MachinePublic) {
	var req tailcfg.RegisterRequest
	if err := decodeRequest(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Version == 0 || req.NodeKey.IsZero() {
		http.Error(w, "register request missing version or node key", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot, err := reco.Read(s.graph, s.nodes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var node *tailcfg.Node
	if snapshot.Valid() {
		node, _ = snapshot.Value().Get(req.NodeKey)
	}
	if node == nil {
		s.nextID++
		node = newNode(s.nextID, machine, &req)
	} else {
		node = node.Clone()
		node.Machine = machine
		node.Cap = req.Version
		if req.Hostinfo != nil {
			node.Hostinfo = req.Hostinfo.View()
			node.Name = req.Hostinfo.Hostname
		}
	}
	if err := s.graph.Update(func(tx *reco.Tx) error {
		reco.MapPut(tx, s.nodes, req.NodeKey, node)
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := tailcfg.RegisterResponse{
		User: tailcfg.User{
			ID:          node.User,
			DisplayName: fmt.Sprintf("User %d", node.User),
		},
		Login: tailcfg.Login{
			ID:          tailcfg.LoginID(node.User),
			Provider:    "recontrol",
			LoginName:   fmt.Sprintf("user-%d@%s", node.User, domain),
			DisplayName: fmt.Sprintf("User %d", node.User),
		},
		MachineAuthorized: true,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("encoding register response: %v", err)
	}
}

func newNode(id tailcfg.NodeID, machine key.MachinePublic, req *tailcfg.RegisterRequest) *tailcfg.Node {
	v4 := netip.PrefixFrom(netaddr.IPv4(100, 64, uint8(id>>8), uint8(id)), 32)
	v6 := netip.PrefixFrom(tsaddr.Tailscale4To6(v4.Addr()), 128)
	hi := req.Hostinfo.View()
	name := hi.Hostname()
	capMap := tailcfg.NodeCapMap{
		tailcfg.CapabilityHTTPS:       []tailcfg.RawMessage{},
		tailcfg.CapabilityFileSharing: []tailcfg.RawMessage{},
	}
	return &tailcfg.Node{
		ID:                id,
		StableID:          tailcfg.StableNodeID(fmt.Sprintf("RECONTROL%08x", id)),
		User:              tailcfg.UserID(id),
		Machine:           machine,
		Key:               req.NodeKey,
		MachineAuthorized: true,
		Addresses:         []netip.Prefix{v4, v6},
		AllowedIPs:        []netip.Prefix{v4, v6},
		Hostinfo:          hi,
		Name:              name,
		Cap:               req.Version,
		CapMap:            capMap,
		Capabilities:      []tailcfg.NodeCapability{tailcfg.CapabilityHTTPS, tailcfg.CapabilityFileSharing},
	}
}

func (s *controlServer) serveMap(w http.ResponseWriter, r *http.Request, machine key.MachinePublic) {
	var req tailcfg.MapRequest
	if err := decodeRequest(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Keep the read-modify-write of node state atomic with registration and
	// other map updates. Graph.Update serializes reco transactions; this mutex
	// also covers choosing the input value for those transactions.
	s.mu.Lock()
	node, err := s.node(req.NodeKey)
	if err != nil {
		s.mu.Unlock()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if node.Machine != machine {
		s.mu.Unlock()
		http.Error(w, "node does not belong to machine", http.StatusForbidden)
		return
	}

	streamingReadOnly := req.Stream && req.Version >= 68
	if !req.ReadOnly && !streamingReadOnly {
		node.Endpoints = filterEndpoints(req.Endpoints)
		node.DiscoKey = req.DiscoKey
		node.Cap = req.Version
		if req.Hostinfo != nil {
			node.Hostinfo = req.Hostinfo.View()
			node.Name = req.Hostinfo.Hostname
		}
		if err := s.graph.Update(func(tx *reco.Tx) error {
			reco.MapPut(tx, s.nodes, req.NodeKey, node)
			return nil
		}); err != nil {
			s.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.mu.Unlock()

	// Lite endpoint updates only persist one point mutation. Existing streaming
	// polls consume that mutation directly from their reco map subscriptions.
	if !req.Stream && req.OmitPeers && !req.ReadOnly {
		w.WriteHeader(http.StatusOK)
		return
	}

	if !req.Stream {
		snapshot, err := reco.Read(s.graph, s.nodes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		res, err := buildInitialMapResponse(req.NodeKey, snapshot.Value())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := sendMapMessage(w, req.Compress != "", res); err != nil {
			log.Printf("sending map response: %v", err)
		}
		return
	}

	updates := newNodeUpdateQueue()
	initial, sub, err := reco.SubscribeMap(s.graph, s.responses, reco.SubscribeOptions{
		FixedPointOnly: true,
		Coalesce:       true,
	}, func(ev reco.MapEvent[key.NodePublic, *tailcfg.Node]) {
		updates.add(ev.Changes)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer sub.Unsubscribe()

	current, err := buildInitialMapResponse(req.NodeKey, initial.Value())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if err := sendMapMessage(w, req.Compress != "", current); err != nil {
		return
	}
	keepAlive := time.NewTicker(50 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-updates.wake:
			delta, selfRemoved := deltaMapResponse(req.NodeKey, updates.take())
			if selfRemoved {
				return
			}
			if mapResponseEmpty(delta) {
				continue
			}
			if err := sendMapMessage(w, req.Compress != "", delta); err != nil {
				return
			}
		case <-keepAlive.C:
			if err := sendMapMessage(w, req.Compress != "", &tailcfg.MapResponse{KeepAlive: true}); err != nil {
				return
			}
		}
	}
}

func (s *controlServer) node(nk key.NodePublic) (*tailcfg.Node, error) {
	snap, err := reco.Read(s.graph, s.nodes)
	if err != nil {
		return nil, err
	}
	if !snap.Valid() {
		return nil, errors.New("node not found")
	}
	n, ok := snap.Value().Get(nk)
	if !ok {
		return nil, errors.New("node not found")
	}
	return n.Clone(), nil
}

type nodeUpdateQueue struct {
	mu      sync.Mutex
	pending map[key.NodePublic]reco.MapChange[key.NodePublic, *tailcfg.Node]
	wake    chan struct{}
}

func newNodeUpdateQueue() *nodeUpdateQueue {
	return &nodeUpdateQueue{
		pending: make(map[key.NodePublic]reco.MapChange[key.NodePublic, *tailcfg.Node]),
		wake:    make(chan struct{}, 1),
	}
}

func (q *nodeUpdateQueue) add(changes []reco.MapChange[key.NodePublic, *tailcfg.Node]) {
	q.mu.Lock()
	for _, change := range changes {
		if old, ok := q.pending[change.Key]; ok {
			old.After = change.After
			old.AfterValid = change.AfterValid
			q.pending[change.Key] = old
		} else {
			q.pending[change.Key] = change
		}
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *nodeUpdateQueue) take() []reco.MapChange[key.NodePublic, *tailcfg.Node] {
	q.mu.Lock()
	defer q.mu.Unlock()
	changes := make([]reco.MapChange[key.NodePublic, *tailcfg.Node], 0, len(q.pending))
	for _, change := range q.pending {
		if change.BeforeValid || change.AfterValid {
			changes = append(changes, change)
		}
	}
	clear(q.pending)
	return changes
}

func deltaMapResponse(self key.NodePublic, changes []reco.MapChange[key.NodePublic, *tailcfg.Node]) (_ *tailcfg.MapResponse, selfRemoved bool) {
	delta := new(tailcfg.MapResponse)
	for _, change := range changes {
		if change.Key == self {
			if !change.AfterValid {
				return nil, true
			}
			delta.Node = mapResponseNode(change.After)
			continue
		}
		if change.AfterValid {
			delta.PeersChanged = append(delta.PeersChanged, mapResponseNode(change.After))
			if !change.BeforeValid {
				profile := userProfile(change.After.User)
				delta.UserProfiles = append(delta.UserProfiles, profile)
			}
		} else if change.BeforeValid {
			delta.PeersRemoved = append(delta.PeersRemoved, change.Before.ID)
		}
	}
	sort.Slice(delta.PeersChanged, func(i, j int) bool { return delta.PeersChanged[i].ID < delta.PeersChanged[j].ID })
	slices.Sort(delta.PeersRemoved)
	return delta, false
}

func mapResponseEmpty(r *tailcfg.MapResponse) bool {
	return r.Node == nil && r.DERPMap == nil && r.Domain == "" && r.CollectServices == "" &&
		r.PacketFilter == nil && r.UserProfiles == nil && r.PeersChanged == nil && r.PeersRemoved == nil
}

func sendMapMessage(w http.ResponseWriter, compress bool, msg *tailcfg.MapResponse) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if compress {
		b = zstdframe.AppendEncode(nil, b, zstdframe.FastestCompression)
	}
	if len(b) > maxMapMessage {
		return fmt.Errorf("map message too large: %d", len(b))
	}
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(b)))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func decodeRequest(r *http.Request, dst any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, requestLimit+1))
	if err != nil {
		return err
	}
	if len(b) > requestLimit {
		return errors.New("request too large")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func filterEndpoints(in []netip.AddrPort) []netip.AddrPort {
	out := in[:0]
	for _, ep := range in {
		ip := ep.Addr()
		if ip.Zone() != "" || ip.Is6() && ip.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ep)
	}
	return out
}

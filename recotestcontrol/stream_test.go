package recotestcontrol

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/util/must"
)

func readMapFrame(t *testing.T, r io.Reader) *tailcfg.MapResponse {
	t.Helper()
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		t.Fatal(err)
	}
	if size > 1<<20 {
		t.Fatalf("oversized frame: %d", size)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		t.Fatal(err)
	}
	var res tailcfg.MapResponse
	if err := json.Unmarshal(b, &res); err != nil {
		t.Fatal(err)
	}
	return &res
}

func openMapStream(t *testing.T, ctx context.Context, url string, nk key.NodePublic) *http.Response {
	t.Helper()
	body := must.Get(json.Marshal(&tailcfg.MapRequest{NodeKey: nk, Version: MinCapabilityVersion, Stream: true}))
	r := must.Get(http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body)))
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	if res.StatusCode != 200 {
		t.Fatalf("status = %v", res.Status)
	}
	return res
}

func TestStreamDeltasCommandsAndCleanup(t *testing.T) {
	s, ns := testNodes(t, 2)
	done := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		s.serveMap(w, r, ns[0].Machine)
	}))
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	res := openMapStream(t, ctx, httpServer.URL, ns[0].Key)
	if first := readMapFrame(t, res.Body); len(first.Peers) != 1 || first.Node == nil {
		t.Fatal("missing initial snapshot")
	}
	if err := s.AwaitNodeInMapRequest(ctx, ns[0].Key); err != nil {
		t.Fatal(err)
	}
	ns[1].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[1])
	if delta := readMapFrame(t, res.Body); len(delta.PeersChangedPatch) != 1 || len(delta.PeersChanged) != 0 || len(delta.Peers) != 0 || delta.PeersChangedPatch[0].DiscoKey == nil || *delta.PeersChangedPatch[0].DiscoKey != ns[1].DiscoKey {
		t.Fatalf("not a peer delta: %+v", delta)
	}
	// A lite map update receives only a status acknowledgement, while the
	// standing stream gets the self-node delta. It never builds a full map.
	lite := &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: MinCapabilityVersion, OmitPeers: true, DiscoKey: key.NewDisco().Public()}
	w := httptest.NewRecorder()
	s.serveMap(w, httptest.NewRequest("POST", "/map", bytes.NewReader(must.Get(json.Marshal(lite)))), ns[0].Machine)
	if w.Code != 200 || w.Body.Len() != 0 {
		t.Fatalf("lite response: %d, %d bytes", w.Code, w.Body.Len())
	}
	if delta := readMapFrame(t, res.Body); delta.Node == nil || delta.Node.DiscoKey != lite.DiscoKey || len(delta.Peers) != 0 {
		t.Fatal("missing self delta")
	}
	// A repeated peer value should not create an empty map response.
	s.UpdateNode(ns[1])
	if !s.AddPingRequest(ns[0].Key, &tailcfg.PingRequest{URL: "https://noop.example/"}) {
		t.Fatal("ping rejected")
	}
	if r := readMapFrame(t, res.Body); r.PingRequest == nil {
		t.Fatalf("no-op emitted a map response: %+v", r)
	}
	ns[1].HomeDERP = 2
	s.UpdateNode(ns[1])
	if r := readMapFrame(t, res.Body); len(r.PeersChangedPatch) != 1 || r.PeersChangedPatch[0].DERPRegion != 2 {
		t.Fatalf("expected DERP patch after no-op: %+v", r)
	}
	if !s.AddRawMapResponse(ns[0].Key, &tailcfg.MapResponse{Domain: "first"}) ||
		!s.AddPingRequest(ns[0].Key, &tailcfg.PingRequest{URL: "https://ping.example/"}) ||
		!s.AddRawMapResponse(ns[0].Key, &tailcfg.MapResponse{Domain: "last"}) {
		t.Fatal("connected stream rejected commands")
	}
	if r := readMapFrame(t, res.Body); r.Domain != "first" {
		t.Fatal("raw FIFO: missing first")
	}
	if r := readMapFrame(t, res.Body); r.PingRequest == nil {
		t.Fatal("raw FIFO: missing ping")
	}
	if r := readMapFrame(t, res.Body); r.Domain != "last" {
		t.Fatal("raw FIFO: missing last")
	}
	if s.canGenerateAutomaticMapResponseFor(ns[0].Key) {
		t.Fatal("raw injection did not suppress automatic responses")
	}
	s.UpdateNode(ns[1])
	if !s.AddPingRequest(ns[0].Key, &tailcfg.PingRequest{URL: "https://after-suppression.example/"}) {
		t.Fatal("ping rejected after suppression")
	}
	if r := readMapFrame(t, res.Body); r.PingRequest == nil || r.Node != nil || len(r.PeersChanged) != 0 {
		t.Fatal("automatic response escaped suppression")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not exit")
	}
	s.mu.Lock()
	left := len(s.sessions)
	s.mu.Unlock()
	if left != 0 || s.AddPingRequest(ns[0].Key, &tailcfg.PingRequest{}) {
		t.Fatal("stream not removed on disconnect")
	}
}

func TestStreamReplacement(t *testing.T) {
	s, ns := testNodes(t, 1)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serveMap(w, r, ns[0].Machine) }))
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	a := openMapStream(t, ctx, httpServer.URL, ns[0].Key)
	readMapFrame(t, a.Body)
	b := openMapStream(t, ctx, httpServer.URL, ns[0].Key)
	readMapFrame(t, b.Body)
	if _, err := a.Body.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("old stream not closed: %v", err)
	}
	if !s.AddPingRequest(ns[0].Key, &tailcfg.PingRequest{URL: "https://ping.example/"}) {
		t.Fatal("old cleanup removed replacement stream")
	}
	if r := readMapFrame(t, b.Body); r.PingRequest == nil {
		t.Fatal("replacement did not get ping")
	}
}

func TestModifiedInitialPeersDisablePatches(t *testing.T) {
	s, ns := testNodes(t, 2)
	s.ModifyFirstMapResponse = func(r *tailcfg.MapResponse, _ *tailcfg.MapRequest) { r.Peers[0].Name = "custom-initial-name" }
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serveMap(w, r, ns[0].Machine) }))
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream := openMapStream(t, ctx, httpServer.URL, ns[0].Key)
	if first := readMapFrame(t, stream.Body); first.Peers[0].Name != "custom-initial-name" {
		t.Fatal("initial hook not applied")
	}
	ns[1].DiscoKey = key.NewDisco().Public()
	s.UpdateNode(ns[1])
	r := readMapFrame(t, stream.Body)
	if len(r.PeersChanged) != 1 || len(r.PeersChangedPatch) != 0 || r.PeersChanged[0].Name != ns[1].Name {
		t.Fatalf("patch assumed a graph baseline after a custom initial response: %+v", r)
	}
}

func TestAwaitNodeCanceled(t *testing.T) {
	s, ns := testNodes(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.AwaitNodeInMapRequest(ctx, ns[0].Key); err != context.Canceled {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

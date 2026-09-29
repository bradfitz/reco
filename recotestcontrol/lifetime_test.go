package recotestcontrol

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/reco"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/util/must"
)

type blockedMapWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *blockedMapWriter) Header() http.Header { return make(http.Header) }
func (w *blockedMapWriter) WriteHeader(int)     {}
func (w *blockedMapWriter) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func assertDormantGraph(t *testing.T, g *reco.Graph) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := g.Stats()
		if s.Subscriptions == 0 && s.ActiveFunctions == 0 && s.CachedFunctions == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("graph still demanded: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMapCancellationReleasesBlockedWriter(t *testing.T) {
	for _, reason := range []string{"disconnect", "replacement", "overflow"} {
		t.Run(reason, func(t *testing.T) {
			s, ns := testNodes(t, 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w := &blockedMapWriter{entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.serveMapStream(w, httptest.NewRequestWithContext(ctx, "POST", "/machine/map", nil), &tailcfg.MapRequest{NodeKey: ns[0].Key, Version: MinCapabilityVersion, Stream: true}, ns[0].ID)
			}()
			t.Cleanup(func() {
				close(w.release)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("map handler stuck")
				}
			})
			select {
			case <-w.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("map did not start writing")
			}
			switch reason {
			case "disconnect":
				cancel()
			case "replacement":
				s.mu.Lock()
				s.sessions[ns[0].ID].close()
				s.mu.Unlock()
			case "overflow":
				var d reco.MapDelta[key.NodePublic, *tailcfg.Node]
				for i := range 10001 {
					n := ns[0].Clone()
					n.Key = key.NewNode().Public()
					n.ID = tailcfg.NodeID(i + 2)
					d.Put = append(d.Put, reco.MapEntry[key.NodePublic, *tailcfg.Node]{Key: n.Key, Value: n})
				}
				must.Do(s.graph.Update(func(tx *reco.Tx) error { reco.Set(tx, nodeData, s.nodes.WithDelta(d)); return nil }))
			}
			assertDormantGraph(t, s.graph) // Writer remains blocked.
		})
	}
}

func TestDemandLifetime(t *testing.T) {
	s, ns := testNodes(t, 2)
	assertDormantGraph(t, s.graph)
	for i := range 3 {
		before := s.graph.Stats().Evaluations
		ns[1].HomeDERP = tailcfg.DERPRegionID(i + 1)
		s.UpdateNode(ns[1])
		if s.graph.Stats().Evaluations != before {
			t.Fatal("unwatched input edit ran computations")
		}
		initial, sub, err := reco.SubscribeStruct(s.graph, metaNode, reco.SubscribeOptions{}, func(reco.StructEvent[mapMeta]) {})
		if err != nil {
			t.Fatal(err)
		}
		n, _ := initial.Value().Value().Nodes.Get(ns[1].Key)
		if n.HomeDERP != ns[1].HomeDERP {
			t.Fatal("reconnect missed offline edit")
		}
		must.Do(sub.Unsubscribe())
		assertDormantGraph(t, s.graph)
	}
}

func TestRejectedRequests(t *testing.T) {
	s := &Server{}
	for _, handler := range []func(http.ResponseWriter, *http.Request, key.MachinePublic){s.serveRegister, s.serveMap} {
		for _, body := range []string{`not json`, `{"Version":108}`} {
			w := httptest.NewRecorder()
			handler(w, httptest.NewRequest("POST", "/machine", bytes.NewBufferString(body)), key.NewMachine().Public())
			if w.Code != http.StatusBadRequest {
				t.Fatalf("invalid request status = %d", w.Code)
			}
		}
	}
}

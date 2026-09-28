package metagraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/reco"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
func peer(t *testing.T, local, remote string, custom func(*Options)) *Peer {
	t.Helper()
	o := Options{Graph: reco.NewGraphWithOptions(reco.GraphOptions{DemandDriven: true}), Metagraph: "test", Local: local, Remote: remote}
	if custom != nil {
		custom(&o)
	}
	p, err := New(o)
	must(t, err)
	t.Cleanup(p.Close)
	return p
}
func link(t *testing.T, a, b *Peer) func() {
	t.Helper()
	s := httptest.NewServer(b)
	ctx, cancel := context.WithCancel(t.Context())
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), nil)
	must(t, err)
	done := make(chan struct{})
	go func() { defer close(done); _ = a.Serve(ctx, c) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			c.CloseNow()
			<-done
			b.Disconnect()
			eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.session == nil })
			s.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}
func read[T any](t *testing.T, p *Peer, n reco.Node[T]) reco.Snapshot[T] {
	t.Helper()
	s, err := reco.Read(p.opts.Graph, n)
	must(t, err)
	return s
}

func TestBidirectionalComputedWatchesAndLifetime(t *testing.T) {
	var mu sync.Mutex
	var wire []frame
	a := peer(t, "a", "b", func(o *Options) {
		o.OnWire = func(dir string, raw []byte) {
			if dir != "sent" {
				return
			}
			var f frame
			_ = json.Unmarshal(raw, &f)
			mu.Lock()
			wire = append(wire, f)
			mu.Unlock()
		}
	})
	b := peer(t, "b", "a", nil)
	addr := Address{Namespace: "test", Class: "words", Key: `["partition",42]`}
	n := reco.SetData[string]("words")
	must(t, Export(a, addr, n, Set[string]("v1")))
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, n, reco.SetDelta[string]{Add: []string{"birch", "cedar"}})
		return nil
	}))
	mirror, w, err := Import(b, addr, Set[string]("v1"))
	must(t, err)
	again, w2, err := Import(b, addr, Set[string]("v1"))
	must(t, err)
	if mirror != again {
		t.Fatal("same address did not share its mirror")
	}
	computed := reco.Func("count", reco.Deps(struct {
		Words reco.Node[reco.SetSnapshot[string]]
	}{mirror}), func(_ reco.Eval, in struct{ Words reco.SetSnapshot[string] }) reco.Result[int] {
		return reco.OK(in.Words.Len())
	})
	countAddr := Address{Namespace: "test", Class: "count"}
	must(t, Export(b, countAddr, computed, JSON[int]("v1")))
	count, countWatch, err := Import(a, countAddr, JSON[int]("v1"))
	must(t, err)
	if read(t, a, count).Valid() {
		t.Fatal("mirror valid before snapshot")
	}
	stop := link(t, a, b)
	eventually(t, func() bool { return a.Status().Ready == 1 && b.Status().Ready == 1 && read(t, a, count).Value() == 2 })
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, n, reco.SetDelta[string]{Add: []string{"elm"}, Remove: []string{"birch", "cedar"}})
		return nil
	}))
	eventually(t, func() bool { return read(t, a, count).Value() == 1 && read(t, b, mirror).Value().Contains("elm") })
	mu.Lock()
	var delta frame
	for _, f := range wire {
		if f.Type == "value" && !f.Full {
			delta = f
		}
	}
	mu.Unlock()
	var patch setDelta[string]
	must(t, json.Unmarshal(delta.Value, &patch))
	if patch.Clear || len(patch.Add) != 1 || len(patch.Remove) != 2 || delta.Version <= delta.Base {
		t.Fatalf("not a minimal atomic delta: %+v", delta)
	}
	w.Close()
	w.Close()
	if b.Status().Watching != 1 {
		t.Fatal("one shared close canceled another watcher")
	}
	countWatch.Close()
	eventually(t, func() bool { return b.opts.Graph.Stats().ActiveFunctions == 0 && b.Status().Serving == 0 })
	w2.Close()
	eventually(t, func() bool { return a.Status().Serving == 0 && a.opts.Graph.Stats().Subscriptions == 0 })
	before := b.opts.Graph.Stats().Nodes
	reused, w3, err := Import(b, addr, Set[string]("v1"))
	must(t, err)
	if reused != mirror || b.opts.Graph.Stats().Nodes != before {
		t.Fatal("reimport leaked a new registered node")
	}
	eventually(t, func() bool { return b.Status().Ready == 1 })
	w3.Close()
	stop()
	if a.Status().Connected || b.Status().Connected {
		t.Fatal("stale connection")
	}
}

func TestReconnectAndNewEpochReplaceCachedSet(t *testing.T) {
	a, b := peer(t, "a", "b", nil), peer(t, "b", "a", nil)
	addr := Address{Namespace: "test", Class: "words"}
	n := reco.SetData[string]("words")
	setup := func(p *Peer, items []string) {
		must(t, Export(p, addr, n, Set[string]("v1")))
		must(t, p.opts.Graph.Update(func(tx *reco.Tx) error { reco.ApplySetDelta(tx, n, reco.SetDelta[string]{Add: items}); return nil }))
	}
	setup(a, []string{"old", "kept"})
	m, w, err := Import(b, addr, Set[string]("v1"))
	must(t, err)
	defer w.Close()
	stop := link(t, a, b)
	eventually(t, func() bool { return b.Status().Ready == 1 })
	epoch := b.Status().Epoch
	stop()
	if b.Status().Ready != 0 || !read(t, b, m).Value().Contains("old") {
		t.Fatal("cache/freshness incorrect after disconnect")
	}
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error {
		reco.ApplySetDelta(tx, n, reco.SetDelta[string]{Clear: true, Add: []string{"offline"}})
		return nil
	}))
	stop = link(t, a, b)
	eventually(t, func() bool { return b.Status().Ready == 1 && read(t, b, m).Value().Contains("offline") })
	if read(t, b, m).Value().Len() != 1 {
		t.Fatal("reconnect retained deleted members")
	}
	stop()
	a2 := peer(t, "a", "b", nil)
	setup(a2, []string{"restarted"})
	link(t, a2, b)
	eventually(t, func() bool {
		return b.Status().Ready == 1 && b.Status().Epoch != epoch && read(t, b, m).Value().Contains("restarted")
	})
	if read(t, b, m).Value().Len() != 1 {
		t.Fatal("new authority epoch did not replace snapshot")
	}
}

func TestThousandSubscriptionsUseConstantGoroutines(t *testing.T) {
	a := peer(t, "a", "b", func(o *Options) { o.QueueMessages = 8 })
	b := peer(t, "b", "a", func(o *Options) { o.QueueMessages = 8 })
	for i := range 1000 {
		n := reco.In(new(reco.Scope), reco.Data[int]("number"))
		addr := Address{Namespace: "test", Class: "number", Key: fmt.Sprint(i)}
		must(t, Export(a, addr, n, JSON[int]("v1")))
		must(t, a.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, n, i); return nil }))
		_, _, err := Import(b, addr, JSON[int]("v1"))
		must(t, err)
	}
	before := runtime.NumGoroutine()
	stop := link(t, a, b)
	eventually(t, func() bool { return b.Status().Ready == 1000 })
	if added := runtime.NumGoroutine() - before; added > 25 {
		t.Fatalf("%d goroutines for 1000 watches", added)
	}
	if a.Status().Serving != 1000 {
		t.Fatalf("serving: %+v", a.Status())
	}
	stop()
	if a.opts.Graph.Stats().Subscriptions != 0 {
		t.Fatal("disconnect leaked graph subscriptions")
	}
}

func TestBadHelloAndDeltaAreRejected(t *testing.T) {
	for _, test := range []string{"hello", "base", "schema", "authority"} {
		t.Run(test, func(t *testing.T) {
			p := peer(t, "local", "remote", nil)
			addr := Address{Namespace: "test", Class: "n"}
			if test == "base" {
				_, _, err := Import(p, addr, JSON[int]("v1"))
				must(t, err)
			}
			if test == "schema" {
				must(t, Export(p, addr, reco.Data[int]("n"), JSON[int]("v1")))
			}
			s := httptest.NewServer(p)
			defer s.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), nil)
			must(t, err)
			defer c.CloseNow()
			var f frame
			must(t, wsjson.Read(ctx, c, &f))
			hello := frame{Type: "hello", Protocol: 1, Metagraph: "test", Peer: "remote", Epoch: "boot"}
			if test == "hello" {
				hello.Metagraph = "wrong"
			}
			must(t, wsjson.Write(ctx, c, hello))
			if test == "base" {
				must(t, wsjson.Read(ctx, c, &f))
				must(t, wsjson.Write(ctx, c, frame{Type: "value", ID: f.ID, Full: true, Version: 1, Value: json.RawMessage(`1`)}))
				must(t, wsjson.Write(ctx, c, frame{Type: "value", ID: f.ID, Base: 42, Version: 43, Value: json.RawMessage(`2`)}))
			}
			if test == "schema" || test == "authority" {
				must(t, wsjson.Write(ctx, c, frame{Type: "subscribe", ID: 1, Address: &addr, Codec: "json/wrong"}))
			}
			if _, _, err := c.Read(ctx); err == nil || ctx.Err() != nil {
				t.Fatalf("peer didn't reject %s promptly: %v", test, err)
			}
		})
	}
}

func TestBoundedQueueCancelsRatherThanDropping(t *testing.T) {
	p := peer(t, "a", "b", nil)
	for _, limit := range []string{"count", "bytes", "message"} {
		t.Run(limit, func(t *testing.T) {
			opts := p.opts
			opts.QueueMessages = 1
			if limit == "bytes" {
				opts.QueueBytes = 1
			}
			if limit == "message" {
				opts.MaxMessageBytes = 1
			}
			q, err := New(opts)
			must(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := &session{p: q, ctx: ctx, cancel: cancel, queue: make(chan []byte, 1)}
			s.send(frame{Type: "subscribe", ID: 1})
			if limit == "count" {
				if ctx.Err() != nil {
					t.Fatal("first message should fit")
				}
				s.send(frame{Type: "subscribe", ID: 2})
			}
			if ctx.Err() == nil {
				t.Fatal("overflow silently dropped a frame")
			}
		})
	}
}

func TestSnapshotCannotBeOvertakenByConcurrentDelta(t *testing.T) {
	p := peer(t, "a", "b", nil)
	n := reco.Data[int]("n")
	addr := Address{Namespace: "test", Class: "n"}
	encoding, release := make(chan struct{}), make(chan struct{})
	c := JSON[int]("v1")
	encode := c.Encode
	c.Encode = func(prev, cur int, full bool) (json.RawMessage, error) {
		if full {
			close(encoding)
			<-release
		}
		return encode(prev, cur, full)
	}
	must(t, Export(p, addr, n, c))
	must(t, p.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, n, 1); return nil }))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := &session{p: p, ctx: ctx, cancel: cancel, queue: make(chan []byte, 10)}
	started := make(chan struct{})
	done := make(chan struct{})
	var sub reco.SubscriptionHandle
	go func() {
		defer close(done)
		var err error
		sub, err = p.exports[addr].start(s, 1)
		if err != nil {
			t.Error(err)
		}
	}()
	<-encoding
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		err := p.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, n, 2); close(started); return nil })
		if err != nil {
			t.Error(err)
		}
	}()
	<-started
	close(release)
	<-done
	<-updated
	defer sub.Unsubscribe()
	var first, second frame
	must(t, json.Unmarshal(<-s.queue, &first))
	must(t, json.Unmarshal(<-s.queue, &second))
	if !first.Full || second.Full || second.Base != first.Version || string(first.Value) != "1" || string(second.Value) != "2" {
		t.Fatalf("incorrect snapshot/delta ordering: %+v / %+v", first, second)
	}
}

func TestUninitializedExportDoesNotBlockOtherWatches(t *testing.T) {
	a, b := peer(t, "a", "b", nil), peer(t, "b", "a", nil)
	unready, ready := reco.Data[int]("unready"), reco.Data[int]("ready")
	for _, n := range []reco.Node[int]{unready, ready} {
		must(t, Export(a, Address{Namespace: "test", Class: n.ClassName()}, n, JSON[int]("v1")))
	}
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, ready, 1); return nil }))
	x, _, err := Import(b, Address{Namespace: "test", Class: "unready"}, JSON[int]("v1"))
	must(t, err)
	y, _, err := Import(b, Address{Namespace: "test", Class: "ready"}, JSON[int]("v1"))
	must(t, err)
	link(t, a, b)
	eventually(t, func() bool { return a.Status().Serving == 2 && b.Status().Ready == 1 && read(t, b, y).Valid() })
	if read(t, b, x).Valid() {
		t.Fatal("invented a zero for a missing authoritative value")
	}
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, unready, 0); return nil }))
	eventually(t, func() bool { return b.Status().Ready == 2 && read(t, b, x).Valid() })
}

func TestConcurrentWatchChurnDoesNotLeakRegistrations(t *testing.T) {
	a, b := peer(t, "a", "b", nil), peer(t, "b", "a", nil)
	n := reco.Data[int]("n")
	addr := Address{Namespace: "test", Class: "n"}
	must(t, Export(a, addr, n, JSON[int]("v1")))
	must(t, a.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, n, 0); return nil }))
	stop := link(t, a, b)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				_, w, err := Import(b, addr, JSON[int]("v1"))
				if err != nil {
					t.Error(err)
					return
				}
				runtime.Gosched()
				w.Close()
			}
		})
	}
	wg.Go(func() {
		for i := range 100 {
			if err := a.opts.Graph.Update(func(tx *reco.Tx) error { reco.Set(tx, n, i); return nil }); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	stop()
	if b.Status().Watching != 0 || b.opts.Graph.Stats().Nodes != 1 || a.opts.Graph.Stats().Subscriptions != 0 {
		t.Fatal("churn leaked mirror registrations or upstream demand")
	}
}

func TestPeerEndpointRejectsBrowserOrigin(t *testing.T) {
	p := peer(t, "a", "b", nil)
	r := httptest.NewRequest("GET", "http://example/peer", nil)
	r.Header.Set("Origin", "http://example")
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("status = %d, want 403", w.Code)
	}
}

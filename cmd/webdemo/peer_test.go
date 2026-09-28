package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func awaitDemo(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("demo did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func demoValue(d *demo, id string) any {
	d.mu.Lock()
	defer d.mu.Unlock()
	if f := d.latest[id]; f != nil {
		return f().Value
	}
	return nil
}
func editDemo(t *testing.T, d *demo, cmd command) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.apply(cmd); err != nil {
		t.Fatal(err)
	}
}

func TestTwoProcessDemoConvergenceAndRecovery(t *testing.T) {
	a, err := newDemoWithOptions(demoOptions{PeerID: "a", PeerURL: "http://localhost:8032"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := newDemoWithOptions(demoOptions{PeerID: "b", PeerURL: "http://localhost:8031"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.peer.Close)
	t.Cleanup(b.peer.Close)
	if demoValue(a, "summary") != nil || demoValue(b, "summary") != nil {
		t.Fatal("computed summary before remote leaves initialized")
	}
	// An owner can edit before its peer has ever connected.
	editDemo(t, a, command{Op: "set", Node: "label", Value: json.RawMessage(`"Distributed garden"`)})
	s := httptest.NewServer(b)
	defer s.Close()
	connect := func() func() {
		ctx, cancel := context.WithCancel(t.Context())
		c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"/peer", nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); _ = a.peer.Serve(ctx, c) }()
		return func() {
			cancel()
			c.CloseNow()
			<-done
			awaitDemo(t, func() bool { return !b.peer.Status().Connected })
		}
	}
	stop := connect()
	awaitDemo(t, func() bool {
		return demoValue(a, "summary") == "Distributed garden · 19 runes · 57 points" && demoValue(a, "summary") == demoValue(b, "summary")
	})
	editDemo(t, a, command{Op: "add", Node: "a", Item: "willow"})
	editDemo(t, b, command{Op: "batch", Commands: []command{{Op: "set", Node: "weight", Value: json.RawMessage(`5`)}, {Op: "remove", Node: "b", Item: "dune"}}})
	awaitDemo(t, func() bool {
		return demoValue(a, "summary") == "Distributed garden · 21 runes · 105 points" && demoValue(a, "summary") == demoValue(b, "summary")
	})
	// A forged browser batch must roll back local edits if any target is remote.
	a.mu.Lock()
	err = a.apply(command{Op: "batch", Commands: []command{{Op: "add", Node: "a", Item: "illegal"}, {Op: "clear", Node: "b"}}})
	a.mu.Unlock()
	if err == nil {
		t.Fatal("foreign write accepted")
	}
	if demoValue(a, "totalRunes") != 21 {
		t.Fatal("foreign batch partially committed")
	}
	stop()
	if a.peer.Status().Ready != 0 || b.peer.Status().Ready != 0 {
		t.Fatal("disconnected mirrors still reported ready")
	}
	editDemo(t, a, command{Op: "replace", Node: "a", Items: []string{"oak"}})
	editDemo(t, b, command{Op: "clear", Node: "b"})
	stop = connect()
	defer stop()
	awaitDemo(t, func() bool {
		return demoValue(a, "summary") == "Distributed garden · 3 runes · 15 points" && demoValue(a, "summary") == demoValue(b, "summary")
	})
	editDemo(t, a, command{Op: "reset"})
	awaitDemo(t, func() bool {
		return demoValue(a, "summary") == "Word garden · 15 runes · 75 points" && demoValue(a, "summary") == demoValue(b, "summary")
	})
	if demoValue(b, "weight") != 5 {
		t.Fatal("reset changed another owner's leaf")
	}
}

func TestDemoPeerConfiguration(t *testing.T) {
	for _, opts := range []demoOptions{{PeerID: "x", PeerURL: "http://localhost"}, {PeerID: "a"}, {PeerURL: "http://localhost"}, {PeerID: "b", PeerURL: "javascript:alert(1)"}, {PeerID: "a", PeerURL: "http://user:pass@host"}} {
		if _, err := newDemoWithOptions(opts); err == nil {
			t.Fatalf("accepted invalid options %+v", opts)
		}
	}
}

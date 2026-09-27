package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func testDemo(t *testing.T) (*demo, *httptest.Server) {
	t.Helper()
	d, err := newDemo()
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(d)
	t.Cleanup(s.Close)
	return d, s
}

func dial(t *testing.T, s *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func readMessage(t *testing.T, c *websocket.Conn) message {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var m message
	if err := wsjson.Read(ctx, c, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func sendCommand(t *testing.T, c *websocket.Conn, cmd command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, c, cmd); err != nil {
		t.Fatal(err)
	}
}

func nodesByID(m message) map[string]nodeUpdate {
	nodes := make(map[string]nodeUpdate)
	for _, n := range m.Nodes {
		nodes[n.ID] = n
	}
	return nodes
}

func TestWebSocketSnapshotDeltasAndReconnect(t *testing.T) {
	_, s := testDemo(t)
	c1, c2 := dial(t, s), dial(t, s)
	initial := readMessage(t, c1)
	if other := readMessage(t, c2); !reflect.DeepEqual(initial, other) {
		t.Fatal("clients got different initial snapshots")
	}
	if initial.Type != "snapshot" || initial.Revision != 1 || len(initial.Nodes) != 9 {
		t.Fatalf("snapshot: %+v", initial)
	}
	nodes := nodesByID(initial)
	if nodes["totalRunes"].Value != float64(19) || nodes["score"].Value != float64(57) {
		t.Fatal("incorrect initial DAG output")
	}

	sendCommand(t, c1, command{Op: "remove", Node: "a", Item: "cedar"})
	update := readMessage(t, c1)
	if other := readMessage(t, c2); !reflect.DeepEqual(update, other) {
		t.Fatal("broadcast differed")
	}
	if update.Type != "update" || update.Revision != 2 || len(update.Nodes) != 1 || update.Nodes[0].ID != "a" {
		t.Fatalf("overlap should not change union: %+v", update)
	}
	if n := update.Nodes[0]; n.Value != nil || n.Set == nil || !slices.Equal(n.Set.Remove, []string{"cedar"}) {
		t.Fatalf("not a point delta: %+v", n)
	}

	sendCommand(t, c2, command{Op: "remove", Node: "b", Item: "cedar"})
	update = readMessage(t, c1)
	if other := readMessage(t, c2); !reflect.DeepEqual(update, other) {
		t.Fatal("broadcast differed")
	}
	nodes = nodesByID(update)
	if nodes["totalRunes"].Value != float64(14) || nodes["score"].Value != float64(42) {
		t.Fatalf("wrong derived outputs: %+v", update)
	}
	if n := nodes["details"]; n.Value != nil || n.Map == nil || !slices.Equal(n.Map.Remove, []string{"cedar"}) || len(n.Map.Put) != 0 {
		t.Fatalf("map not a one-key removal: %+v", n)
	}
	if n := nodes["union"]; n.Value != nil || n.Set == nil || !slices.Equal(n.Set.Remove, []string{"cedar"}) {
		t.Fatalf("union not a one-key removal: %+v", n)
	}

	c1.CloseNow()
	sendCommand(t, c2, command{Op: "add", Node: "b", Item: "moss"})
	update = readMessage(t, c2)
	nodes = nodesByID(update)
	if n := nodes["details"]; n.Map == nil || len(n.Map.Put) != 1 || n.Map.Put["moss"].Upper != "MOSS" {
		t.Fatalf("wrong map put: %+v", n)
	}
	c3 := dial(t, s)
	resync := readMessage(t, c3)
	if resync.Type != "snapshot" || resync.Revision != update.Revision {
		t.Fatal("reconnect did not resync")
	}
	nodes = nodesByID(resync)
	if nodes["totalRunes"].Value != float64(18) || len(nodes["union"].Value.([]any)) != 4 {
		t.Fatalf("stale reconnect snapshot: %+v", resync)
	}
}

func TestWebSocketAtomicReplaceScalarsAndValidation(t *testing.T) {
	_, s := testDemo(t)
	c := dial(t, s)
	readMessage(t, c)
	sendCommand(t, c, command{Op: "replace", Node: "a", Items: []string{"fern", "moss", "fern"}})
	m := readMessage(t, c)
	n := nodesByID(m)
	if p := n["a"].Set; p == nil || len(p.Remove) != 3 || len(p.Add) != 2 {
		t.Fatalf("bad atomic replacement: %+v", m)
	}
	// The number of unique words stays four, but rune counts affect the score.
	if n["totalRunes"].Value != float64(17) || n["score"].Value != float64(51) {
		t.Fatalf("replacement did not update the rune total and score: %+v", m)
	}
	if p := n["details"].Map; p == nil || len(p.Put) != 2 || len(p.Remove) != 2 {
		t.Fatalf("bad map replacement delta: %+v", p)
	}
	sendCommand(t, c, command{Op: "set", Node: "weight", Value: json.RawMessage("7")})
	m = readMessage(t, c)
	n = nodesByID(m)
	if len(n) != 3 || n["score"].Value != float64(119) {
		t.Fatalf("scalar propagation: %+v", m)
	}
	for _, cmd := range []command{
		{Op: "set", Node: "score", Value: json.RawMessage("9")},
		{Op: "set", Node: "weight", Value: json.RawMessage("101")},
		{Op: "set", Node: "weight", Value: json.RawMessage("null")},
		{Op: "replace", Node: "a", Items: []string{"valid", " "}},
		{Op: "add", Node: "union", Item: "x"},
		{Op: "surprise"},
	} {
		sendCommand(t, c, cmd)
		if reply := readMessage(t, c); reply.Type != "error" || reply.Revision != m.Revision {
			t.Fatalf("invalid edit mutated state: %+v", reply)
		}
	}
	sendCommand(t, c, command{Op: "reset"})
	m = readMessage(t, c)
	n = nodesByID(m)
	if n["score"].Value != float64(57) || len(n["a"].Set.Add) != 3 {
		t.Fatalf("reset: %+v", m)
	}
}

func TestSlowClientDoesNotBlockAndCanResync(t *testing.T) {
	d, _ := testDemo(t)
	cancelled := false
	c := &client{send: make(chan []byte, 1), cancel: func() { cancelled = true }}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.clients[c] = true
	for _, item := range []string{"x", "y"} {
		if err := d.apply(command{Op: "add", Node: "a", Item: item}); err != nil {
			t.Fatal(err)
		}
	}
	if !cancelled {
		t.Fatal("slow client was not disconnected")
	}
	nodes := nodesByID(d.snapshot())
	if nodes["totalRunes"].Value != 21 {
		t.Fatal("slow client blocked propagation")
	}
}

func TestRuneTotalsUnicodeEqualTotalAndClear(t *testing.T) {
	_, s := testDemo(t)
	c := dial(t, s)
	readMessage(t, c)
	sendCommand(t, c, command{Op: "add", Node: "a", Item: "🌿é"})
	m := readMessage(t, c)
	n := nodesByID(m)
	if n["totalRunes"].Value != float64(21) || n["score"].Value != float64(63) || n["details"].Map.Put["🌿é"].Runes != 2 {
		t.Fatalf("expected code points, not bytes or UTF-16 units: %+v", m)
	}
	// Replacing a word with another of equal rune count changes the map, but
	// the total and all downstream outputs must stay quiet.
	sendCommand(t, c, command{Op: "batch", Request: 11, Commands: []command{
		{Op: "remove", Node: "a", Item: "🌿é"},
		{Op: "add", Node: "a", Item: "ab"},
	}})
	m = readMessage(t, c)
	n = nodesByID(m)
	for _, id := range []string{"totalRunes", "score", "summary"} {
		if _, ok := n[id]; ok {
			t.Fatalf("unchanged %s was sent: %+v", id, m)
		}
	}
	if len(n) != 3 || len(n["details"].Map.Put) != 1 || len(n["details"].Map.Remove) != 1 {
		t.Fatalf("expected only leaf, union, and map deltas: %+v", m)
	}
	if ack := readMessage(t, c); ack.Type != "committed" || ack.Request != 11 {
		t.Fatalf("bad commit ack: %+v", ack)
	}
	sendCommand(t, c, command{Op: "batch", Request: 12, Commands: []command{{Op: "clear", Node: "a"}, {Op: "clear", Node: "b"}}})
	m = readMessage(t, c)
	n = nodesByID(m)
	if n["totalRunes"].Value != float64(0) || n["score"].Value != float64(0) || n["summary"].Value != "Word garden · 0 runes · 0 points" {
		t.Fatalf("clear did not zero the whole branch: %+v", m)
	}
}

func TestSocketCleanupOriginAndStaticFiles(t *testing.T) {
	d, s := testDemo(t)
	c := dial(t, s)
	readMessage(t, c)
	c.CloseNow()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		d.mu.Lock()
		n := len(d.clients)
		d.mu.Unlock()
		if n == 0 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("disconnected client leaked")
		case <-tick.C:
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	bad, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example"}}})
	if bad != nil {
		bad.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatal("cross-origin websocket was accepted")
	}
	for _, path := range []string{"/", "/app.js", "/style.css"} {
		rr := httptest.NewRecorder()
		d.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
		if rr.Code != 200 || rr.Body.Len() == 0 || rr.Header().Get("Content-Security-Policy") == "" {
			t.Fatalf("static asset %s: %d", path, rr.Code)
		}
	}
}

func TestSetLimitAndNoOpAck(t *testing.T) {
	d, s := testDemo(t)
	c := dial(t, s)
	readMessage(t, c)
	sendCommand(t, c, command{Op: "add", Node: "a", Item: "amber"})
	if m := readMessage(t, c); m.Type != "update" || m.Revision != 2 || len(m.Nodes) != 0 {
		t.Fatalf("no-op ack: %+v", m)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	items := make([]string, maxSetSize+1)
	for i := range items {
		items[i] = "word"
	}
	if err := d.apply(command{Op: "replace", Node: "a", Items: items}); err == nil {
		t.Fatal("oversized input accepted")
	}
	if d.revision != 2 {
		t.Fatal("invalid mutation changed revision")
	}
}

func TestBatchCommitAndRollback(t *testing.T) {
	d, s := testDemo(t)
	a, b := dial(t, s), dial(t, s)
	readMessage(t, a)
	readMessage(t, b)
	sendCommand(t, a, command{Op: "batch", Request: 7, Commands: []command{
		{Op: "add", Node: "a", Item: "fern"},
		{Op: "add", Node: "b", Item: "moss"},
		{Op: "set", Node: "weight", Value: json.RawMessage("7")},
		{Op: "set", Node: "label", Value: json.RawMessage(`"Batch"`)},
	}})
	m := readMessage(t, a)
	if other := readMessage(t, b); !reflect.DeepEqual(m, other) {
		t.Fatal("clients saw different commits")
	}
	n := nodesByID(m)
	if m.Type != "update" || m.Revision != 2 || n["totalRunes"].Value != float64(27) || n["score"].Value != float64(189) || n["summary"].Value != "Batch · 27 runes · 189 points" {
		t.Fatalf("batch was not one settled result: %+v", m)
	}
	if len(n["details"].Map.Put) != 2 {
		t.Fatal("map delta did not contain exactly the two additions")
	}
	if ack := readMessage(t, a); ack.Type != "committed" || ack.Request != 7 || ack.Revision != 2 {
		t.Fatalf("commit ack: %+v", ack)
	}
	d.mu.Lock()
	before := d.snapshot()
	d.mu.Unlock()
	sendCommand(t, a, command{Op: "batch", Request: 8, Commands: []command{
		{Op: "clear", Node: "a"},
		{Op: "set", Node: "label", Value: json.RawMessage(`"MUST NOT COMMIT"`)},
		{Op: "set", Node: "weight", Value: json.RawMessage("101")},
	}})
	if err := readMessage(t, a); err.Type != "error" || err.Request != 8 || err.Revision != 2 {
		t.Fatalf("batch rejection: %+v", err)
	}
	d.mu.Lock()
	after := d.snapshot()
	d.mu.Unlock()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed batch leaked writes or versions")
	}
	// The other client must have no intermediate/failed-batch frames queued.
	sendCommand(t, b, command{Op: "set", Node: "weight", Value: json.RawMessage("8")})
	for _, c := range []*websocket.Conn{a, b} {
		if m := readMessage(t, c); m.Type != "update" || m.Revision != 3 || nodesByID(m)["score"].Value != float64(216) {
			t.Fatalf("extra or partial frame: %+v", m)
		}
	}
}

func TestBatchValidatesStagedSetSize(t *testing.T) {
	d, _ := testDemo(t)
	d.mu.Lock()
	defer d.mu.Unlock()
	items := make([]string, maxSetSize)
	for i := range items {
		items[i] = fmt.Sprintf("word-%d", i)
	}
	if err := d.apply(command{Op: "replace", Node: "a", Items: items}); err != nil {
		t.Fatal(err)
	}
	if err := d.apply(command{Op: "batch", Commands: []command{{Op: "remove", Node: "a", Item: items[0]}, {Op: "add", Node: "a", Item: "new"}}}); err != nil {
		t.Fatalf("remove must make room for later add: %v", err)
	}
	before := d.snapshot()
	if err := d.apply(command{Op: "batch", Commands: []command{{Op: "remove", Node: "a", Item: items[1]}, {Op: "add", Node: "a", Item: "first"}, {Op: "add", Node: "a", Item: "overflow"}}}); err == nil {
		t.Fatal("batch exceeded set limit")
	}
	if !reflect.DeepEqual(before, d.snapshot()) {
		t.Fatal("oversized batch partially applied")
	}
	for _, cmd := range []command{
		{Op: "batch"},
		{Op: "batch", Commands: make([]command, maxBatchSize+1)},
		{Op: "batch", Commands: []command{{Op: "clear", Node: "a"}, {Op: "batch"}}},
	} {
		if err := d.apply(cmd); err == nil {
			t.Fatal("invalid batch accepted")
		}
		if !reflect.DeepEqual(before, d.snapshot()) {
			t.Fatal("invalid batch changed state")
		}
	}
}

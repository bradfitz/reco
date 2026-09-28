package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/metagraph"
	"github.com/bradfitz/reco/nodes"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

//go:embed static/*
var assets embed.FS

const maxSetSize = 64
const maxBatchSize = 128

type word struct {
	Upper string `json:"upper"`
	Runes int    `json:"runes"`
}

// These messages belong only to this demo, not to reco's eventual peer protocol.
type nodeUpdate struct {
	ID      string       `json:"id"`
	Version reco.Version `json:"version"`
	Value   any          `json:"value,omitempty"`
	Set     *setPatch    `json:"set,omitempty"`
	Map     *mapPatch    `json:"map,omitempty"`
}

type setPatch struct {
	Remove []string `json:"remove"`
	Add    []string `json:"add"`
}

type mapPatch struct {
	Remove []string        `json:"remove"`
	Put    map[string]word `json:"put"`
}

type message struct {
	Type     string            `json:"type"`
	Revision uint64            `json:"revision"`
	Nodes    []nodeUpdate      `json:"nodes,omitempty"`
	Error    string            `json:"error,omitempty"`
	Request  uint64            `json:"request,omitempty"`
	Info     *demoInfo         `json:"info,omitempty"`
	Peer     *metagraph.Status `json:"peer,omitempty"`
	Wire     *wireEvent        `json:"wire,omitempty"`
}

type command struct {
	Op       string          `json:"op"`
	Node     string          `json:"node"`
	Value    json.RawMessage `json:"value"`
	Item     string          `json:"item"`
	Items    []string        `json:"items"`
	Commands []command       `json:"commands,omitempty"`
	Request  uint64          `json:"request,omitempty"`
}

type client struct {
	send   chan []byte
	cancel context.CancelFunc
}

type demo struct {
	// All graph reads/writes and snapshot/watch setup pass through mu. reco
	// callbacks run synchronously inside Update and never perform network I/O.
	mu       sync.Mutex
	g        *reco.Graph
	a, b     reco.Node[reco.SetSnapshot[string]]
	label    reco.Node[string]
	weight   reco.Node[int]
	latest   map[string]func() nodeUpdate
	pending  []nodeUpdate
	revision uint64
	clients  map[*client]bool
	handler  http.Handler
	peer     *metagraph.Peer
	info     *demoInfo
	watches  []*metagraph.Watch
}

func newDemo() (*demo, error) {
	return newDemoWithOptions(demoOptions{})
}

func newDemoWithOptions(opts demoOptions) (*demo, error) {
	d := &demo{
		g: reco.NewGraph(), a: reco.SetData[string]("a"), b: reco.SetData[string]("b"),
		label: reco.Data[string]("label"), weight: reco.Data[int]("weight"),
		latest: make(map[string]func() nodeUpdate), clients: make(map[*client]bool),
	}
	if err := d.configurePeer(opts); err != nil {
		return nil, err
	}
	union := nodes.Union("union", d.a, d.b)
	details := nodes.MapSet("details", union, func(k string) word {
		return word{Upper: strings.ToUpper(k), Runes: utf8.RuneCountInString(k)}
	})
	totalRunes := sumMap("totalRunes", details, func(v word) int { return v.Runes })
	score := reco.Func("score", reco.Deps(struct{ TotalRunes, Weight reco.Node[int] }{totalRunes, d.weight}),
		func(_ reco.Eval, in struct{ TotalRunes, Weight int }) reco.Result[int] {
			return reco.OK(in.TotalRunes * in.Weight)
		})
	summary := reco.Func("summary", reco.Deps(struct {
		Label             reco.Node[string]
		TotalRunes, Score reco.Node[int]
	}{d.label, totalRunes, score}), func(_ reco.Eval, in struct {
		Label             string
		TotalRunes, Score int
	}) reco.Result[string] {
		return reco.OK(fmt.Sprintf("%s · %d runes · %d points", in.Label, in.TotalRunes, in.Score))
	})
	if err := d.g.Register(summary); err != nil {
		return nil, err
	}
	for _, n := range []reco.Node[reco.SetSnapshot[string]]{d.a, d.b, union} {
		if err := watch(d, n, fullSet, func(prev, cur reco.SetSnapshot[string]) nodeUpdate {
			p := &setPatch{Remove: []string{}, Add: []string{}}
			// Normalize multiple edits of the same key to its final membership.
			keys := make(map[string]bool)
			for _, c := range cur.Changes() {
				keys[c.Key] = true
			}
			for k := range keys {
				if prev.Contains(k) == cur.Contains(k) {
					continue
				}
				if cur.Contains(k) {
					p.Add = append(p.Add, k)
				} else {
					p.Remove = append(p.Remove, k)
				}
			}
			slices.Sort(p.Add)
			slices.Sort(p.Remove)
			return nodeUpdate{Set: p}
		}); err != nil {
			return nil, err
		}
	}
	if err := watch(d, details, func(v reco.MapSnapshot[string, word]) any {
		out := make(map[string]word, v.Len())
		v.Range(func(k string, v word) bool { out[k] = v; return true })
		return out
	}, func(_, cur reco.MapSnapshot[string, word]) nodeUpdate {
		p := &mapPatch{Remove: []string{}, Put: map[string]word{}}
		for _, c := range cur.Changes() {
			if c.AfterValid {
				p.Put[c.Key] = c.After
			} else {
				p.Remove = append(p.Remove, c.Key)
			}
		}
		slices.Sort(p.Remove)
		return nodeUpdate{Map: p}
	}); err != nil {
		return nil, err
	}
	for _, n := range []reco.Node[int]{d.weight, totalRunes, score} {
		if err := watchScalar(d, n); err != nil {
			return nil, err
		}
	}
	for _, n := range []reco.Node[string]{d.label, summary} {
		if err := watchScalar(d, n); err != nil {
			return nil, err
		}
	}
	if err := d.g.Update(d.seed); err != nil {
		return nil, err
	}
	d.revision, d.pending = 1, nil
	static, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", d.socket)
	if d.peer != nil {
		mux.Handle("GET /peer", d.peer)
	}
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	d.handler = mux
	return d, nil
}

func watchScalar[T any](d *demo, n reco.Node[T]) error {
	return watch(d, n, func(v T) any { return v }, func(_, v T) nodeUpdate { return nodeUpdate{Value: v} })
}

func watch[T any](d *demo, n reco.Node[T], full func(T) any, patch func(T, T) nodeUpdate) error {
	_, err := reco.Subscribe(d.g, n, reco.SubscribeOptions{FixedPointOnly: true}, func(ev reco.Event[T]) {
		cur := ev.Current.Value()
		d.latest[string(n.ClassName())] = func() nodeUpdate {
			return nodeUpdate{ID: string(n.ClassName()), Version: ev.Version, Value: full(cur)}
		}
		var u nodeUpdate
		if !ev.Previous.Valid() {
			u.Value = full(cur)
		} else {
			u = patch(ev.Previous.Value(), cur)
		}
		u.ID, u.Version = string(n.ClassName()), ev.Version
		d.pending = append(d.pending, u)
	})
	return err
}

func fullSet(s reco.SetSnapshot[string]) any {
	items := make([]string, 0, s.Len())
	s.Range(func(k string) bool { items = append(items, k); return true })
	slices.Sort(items)
	return items
}

func (d *demo) seed(tx *reco.Tx) error {
	// Initialize empty collections explicitly; a node with no value is not
	// the same as an initialized empty set.
	for _, n := range []reco.Node[reco.SetSnapshot[string]]{d.a, d.b} {
		if d.owns(string(n.ClassName())) {
			reco.Set(tx, n, reco.SetSnapshot[string]{})
		}
	}
	if d.owns("a") {
		reco.ApplySetDelta(tx, d.a, reco.SetDelta[string]{Add: []string{"amber", "birch", "cedar"}})
	}
	if d.owns("b") {
		reco.ApplySetDelta(tx, d.b, reco.SetDelta[string]{Add: []string{"cedar", "dune"}})
	}
	if d.owns("label") {
		reco.Set(tx, d.label, "Word garden")
	}
	if d.owns("weight") {
		reco.Set(tx, d.weight, 3)
	}
	return nil
}

func (d *demo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
	d.handler.ServeHTTP(w, r)
}

func (d *demo) snapshot() message {
	m := message{Type: "snapshot", Revision: d.revision}
	m.Info = d.info
	if d.peer != nil {
		st := d.peer.Status()
		m.Peer = &st
	}
	for _, get := range d.latest {
		m.Nodes = append(m.Nodes, get())
	}
	slices.SortFunc(m.Nodes, func(a, b nodeUpdate) int { return strings.Compare(a.ID, b.ID) })
	return m
}

func (d *demo) socket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil) // Same-origin only; no wildcard CORS.
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 10)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	c := &client{send: make(chan []byte, 32), cancel: cancel}
	d.mu.Lock()
	d.clients[c] = true
	d.send(c, d.snapshot()) // Atomic snapshot + watch registration.
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.clients, c)
		d.mu.Unlock()
	}()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer cancel()
		for {
			var cmd command
			if err := wsjson.Read(ctx, conn, &cmd); err != nil {
				return
			}
			d.mu.Lock()
			err := d.apply(cmd)
			if err != nil {
				d.send(c, message{Type: "error", Revision: d.revision, Error: err.Error(), Request: cmd.Request})
			} else if cmd.Op == "batch" {
				d.send(c, message{Type: "committed", Revision: d.revision, Request: cmd.Request})
			}
			d.mu.Unlock()
		}
	}()
	defer func() { cancel(); conn.CloseNow(); <-readDone }()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-c.send:
			writeCtx, done := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(writeCtx, websocket.MessageText, data)
			done()
			if err != nil {
				return
			}
		}
	}
}

// send never blocks propagation. An overloaded client is disconnected and can
// reconnect for a complete snapshot; dropping individual deltas is unsafe.
func (d *demo) send(c *client, m message) {
	data, err := json.Marshal(m)
	if err != nil {
		c.cancel()
		return
	}
	d.enqueue(c, data)
}

func (d *demo) enqueue(c *client, data []byte) {
	select {
	case c.send <- data:
	default:
		c.cancel()
	}
}

// apply is called with mu held, after decoding a bounded WebSocket message.
func (d *demo) apply(cmd command) error {
	commands := []command{cmd}
	if cmd.Op == "batch" {
		commands = cmd.Commands
		if len(commands) == 0 || len(commands) > maxBatchSize {
			return fmt.Errorf("a transaction must have 1–%d edits", maxBatchSize)
		}
	}
	// Snapshot leaf membership before taking the graph lock. This sparse overlay
	// validates size limits against earlier edits in the SAME transaction.
	drafts := make(map[string]*setDraft)
	for _, n := range []reco.Node[reco.SetSnapshot[string]]{d.a, d.b} {
		cur, err := reco.Read(d.g, n)
		if err != nil {
			return err
		}
		drafts[string(n.ClassName())] = &setDraft{base: cur.Value(), size: cur.Value().Len(), edits: make(map[string]bool)}
	}
	d.pending = nil
	if err := d.g.Update(func(tx *reco.Tx) error {
		for i, edit := range commands {
			if err := d.mutate(tx, edit, drafts); err != nil {
				return fmt.Errorf("edit %d: %w", i+1, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return d.publish()
}

// publish runs under mu after one local graph transaction (including a remote
// leaf delivery). Browser revisions are local; they are not metagraph versions.
func (d *demo) publish() error {
	d.revision++
	slices.SortFunc(d.pending, func(a, b nodeUpdate) int { return strings.Compare(a.ID, b.ID) })
	data, err := json.Marshal(message{Type: "update", Revision: d.revision, Nodes: d.pending})
	if err != nil {
		return err
	}
	for c := range d.clients {
		d.enqueue(c, data)
	}
	d.pending = nil
	return nil
}

// setDraft tracks only touched keys; ordinary edits never copy a whole set.
type setDraft struct {
	base    reco.SetSnapshot[string]
	edits   map[string]bool
	cleared bool
	size    int
}

func (s *setDraft) contains(k string) bool {
	if present, ok := s.edits[k]; ok {
		return present
	}
	return !s.cleared && s.base.Contains(k)
}

func (s *setDraft) apply(d reco.SetDelta[string]) error {
	if d.Clear {
		s.cleared, s.size, s.edits = true, 0, make(map[string]bool)
	}
	for _, k := range d.Remove {
		if s.contains(k) {
			s.size--
		}
		s.edits[k] = false
	}
	for _, k := range d.Add {
		if !s.contains(k) {
			s.size++
		}
		s.edits[k] = true
	}
	if s.size > maxSetSize {
		return fmt.Errorf("sets are limited to %d words in this demo", maxSetSize)
	}
	return nil
}

func (d *demo) mutate(tx *reco.Tx, cmd command, drafts map[string]*setDraft) error {
	if cmd.Op != "reset" && !d.owns(cmd.Node) {
		return fmt.Errorf("%s is owned by process %s; edit it there", cmd.Node, d.info.Owners[cmd.Node])
	}
	var mutate func(*reco.Tx) error
	switch cmd.Op {
	case "set":
		switch cmd.Node {
		case "label":
			var value string
			if err := json.Unmarshal(cmd.Value, &value); err != nil || len(value) > 80 || string(cmd.Value) == "null" {
				return fmt.Errorf("label must be a string of at most 80 bytes")
			}
			mutate = func(tx *reco.Tx) error { reco.Set(tx, d.label, value); return nil }
		case "weight":
			var value int
			if err := json.Unmarshal(cmd.Value, &value); err != nil || value < 0 || value > 100 || string(cmd.Value) == "null" {
				return fmt.Errorf("weight must be an integer from 0 to 100")
			}
			mutate = func(tx *reco.Tx) error { reco.Set(tx, d.weight, value); return nil }
		default:
			return fmt.Errorf("only label and weight are editable scalar leaves")
		}
	case "add", "remove", "clear", "replace":
		var n reco.Node[reco.SetSnapshot[string]]
		switch cmd.Node {
		case "a":
			n = d.a
		case "b":
			n = d.b
		default:
			return fmt.Errorf("only sets a and b are editable")
		}
		delta := reco.SetDelta[string]{Clear: cmd.Op == "clear" || cmd.Op == "replace"}
		switch cmd.Op {
		case "add", "remove":
			item, err := validItem(cmd.Item)
			if err != nil {
				return err
			}
			if cmd.Op == "add" {
				delta.Add = []string{item}
			} else {
				delta.Remove = []string{item}
			}
		case "replace":
			if len(cmd.Items) > maxSetSize {
				return fmt.Errorf("sets are limited to %d words in this demo", maxSetSize)
			}
			for _, raw := range cmd.Items {
				item, err := validItem(raw)
				if err != nil {
					return err
				}
				delta.Add = append(delta.Add, item)
			}
		}
		if err := drafts[cmd.Node].apply(delta); err != nil {
			return err
		}
		mutate = func(tx *reco.Tx) error { reco.ApplySetDelta(tx, n, delta); return nil }
	case "reset":
		if d.peer != nil {
			// Reset only this process's authority, with normal delta semantics.
			for _, id := range []string{"a", "b"} {
				if !d.owns(id) {
					continue
				}
				items := []string{"amber", "birch", "cedar"}
				n := d.a
				if id == "b" {
					items = []string{"cedar", "dune"}
					n = d.b
				}
				delta := reco.SetDelta[string]{Clear: true, Add: items}
				if err := drafts[id].apply(delta); err != nil {
					return err
				}
				reco.ApplySetDelta(tx, n, delta)
			}
			if d.owns("label") {
				reco.Set(tx, d.label, "Word garden")
			}
			if d.owns("weight") {
				reco.Set(tx, d.weight, 3)
			}
			return nil
		}
		if err := drafts["a"].apply(reco.SetDelta[string]{Clear: true, Add: []string{"amber", "birch", "cedar"}}); err != nil {
			return err
		}
		if err := drafts["b"].apply(reco.SetDelta[string]{Clear: true, Add: []string{"cedar", "dune"}}); err != nil {
			return err
		}
		// Use deltas on initialized sets so existing subscribers see removals.
		mutate = func(tx *reco.Tx) error {
			reco.ApplySetDelta(tx, d.a, reco.SetDelta[string]{Clear: true, Add: []string{"amber", "birch", "cedar"}})
			reco.ApplySetDelta(tx, d.b, reco.SetDelta[string]{Clear: true, Add: []string{"cedar", "dune"}})
			reco.Set(tx, d.label, "Word garden")
			reco.Set(tx, d.weight, 3)
			return nil
		}
	default:
		return fmt.Errorf("unknown operation %q", cmd.Op)
	}
	return mutate(tx)
}

func validItem(raw string) (string, error) {
	item := strings.TrimSpace(raw)
	if item == "" || len(item) > 48 {
		return "", fmt.Errorf("words must contain 1–48 bytes after trimming spaces")
	}
	return item, nil
}

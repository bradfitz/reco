package metagraph

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bradfitz/reco"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Address identifies a node instance in the agreed metagraph. Namespace and Key
// are application-defined, exact strings, not routing instructions. Applications
// must encode composite keys canonically. Class identifies the shared definition.
type Address struct {
	Namespace string             `json:"namespace"`
	Class     reco.NodeClassName `json:"class"`
	Key       string             `json:"key,omitempty"`
}

func (a Address) valid() bool {
	return a.Namespace != "" && a.Class != "" && len(a.Namespace)+len(a.Class)+len(a.Key) <= 4096
}

// Options configures one known peer and its bounded connection resources.
type Options struct {
	Graph                    *reco.Graph
	Metagraph, Local, Remote string
	// Apply defaults to Graph.Update. Applications may wrap it to serialize
	// publication with other observers, but must call Graph.Update exactly once.
	Apply func(func(*reco.Tx) error) error
	// OnStatus and OnWire run outside graph and peer locks, on session goroutines.
	// Keep them brief. They may inspect Status, but must not call Close.
	OnStatus         func(Status)
	OnWire           func(direction string, data []byte)
	MaxSubscriptions int // active exports / lifetime distinct imports; default 10,000
	QueueMessages    int // default 256
	QueueBytes       int // default 4 MiB
	MaxMessageBytes  int // default 1 MiB; snapshots must fit in one message
}

// Status describes transport freshness, not a distributed fixed point. Ready
// counts requested nodes whose initial snapshot arrived on the current session.
type Status struct {
	Local     string `json:"local"`
	Remote    string `json:"remote"`
	Connected bool   `json:"connected"`
	Epoch     string `json:"epoch,omitempty"`
	Watching  int    `json:"watching"`
	Serving   int    `json:"serving"`
	Ready     int    `json:"ready"`
	LastError string `json:"error,omitempty"`
}

// Peer binds local exports and remote mirrors to one other process. Configure
// exports before serving or dialing; Import and Watch.Close may run concurrently
// with a session. Do not call graph-locking methods from a graph callback.
type Peer struct {
	mu       sync.Mutex
	delivery sync.Mutex // serializes incoming edits with last-watch cancellation
	opts     Options
	epoch    string
	exports  map[Address]exporter
	imports  map[Address]*imported
	byID     map[uint64]*imported
	nextID   uint64
	session  *session
	status   Status
	closed   bool
}

type imported struct {
	id      uint64
	address Address
	codec   string
	node    any
	refs    int
	ready   bool
	sent    bool
	pending *list.Element // current session's not-yet-sent subscription
	version reco.Version
	apply   func(*reco.Tx, json.RawMessage, bool) error
}

type exporter struct {
	codec string
	start func(*session, uint64) (reco.SubscriptionHandle, error)
}

func New(opts Options) (*Peer, error) {
	if opts.Graph == nil || opts.Metagraph == "" || opts.Local == "" || opts.Remote == "" || opts.Local == opts.Remote {
		return nil, errors.New("metagraph: graph, metagraph name, and distinct peer identities are required")
	}
	if opts.Apply == nil {
		opts.Apply = opts.Graph.Update
	}
	if opts.MaxSubscriptions == 0 {
		opts.MaxSubscriptions = 10000
	}
	if opts.QueueMessages == 0 {
		opts.QueueMessages = 256
	}
	if opts.QueueBytes == 0 {
		opts.QueueBytes = 4 << 20
	}
	if opts.MaxMessageBytes == 0 {
		opts.MaxMessageBytes = 1 << 20
	}
	if opts.MaxSubscriptions < 1 || opts.QueueMessages < 1 || opts.QueueBytes < 1 || opts.MaxMessageBytes < 1 {
		return nil, errors.New("metagraph: invalid limits")
	}
	return &Peer{opts: opts, epoch: rand.Text(), exports: make(map[Address]exporter), imports: make(map[Address]*imported), byID: make(map[uint64]*imported), status: Status{Local: opts.Local, Remote: opts.Remote}}, nil
}

// Export permits this peer to watch a local authority's data OR computed output.
// Register exports before starting the connection. Imported values cannot be
// exported at the same address: mirrors never advertise themselves as authority.
func Export[T any](p *Peer, address Address, node reco.Node[T], codec Codec[T]) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.session != nil {
		return errors.New("metagraph: configure exports before connecting")
	}
	if !address.valid() || address.Class != node.ClassName() || !codec.valid() {
		return errors.New("metagraph: invalid export address or codec")
	}
	if _, ok := p.exports[address]; ok || p.imports[address] != nil {
		return errors.New("metagraph: duplicate authority")
	}
	if err := p.opts.Graph.Register(node); err != nil {
		return err
	}
	p.exports[address] = exporter{codec: codec.Name, start: func(s *session, id uint64) (reco.SubscriptionHandle, error) {
		// The gate prevents a change callback from overtaking its initial
		// snapshot. It is never held across network I/O or Unsubscribe.
		var gate sync.Mutex
		gate.Lock()
		snap, sub, err := reco.SubscribeSnapshot(p.opts.Graph, node, reco.SubscribeOptions{FixedPointOnly: true}, func(ev reco.Event[T]) {
			gate.Lock()
			defer gate.Unlock()
			full := !ev.Previous.Valid()
			payload, err := codec.Encode(ev.Previous.Value(), ev.Current.Value(), full)
			if err != nil {
				s.cancel()
				return
			}
			s.send(frame{Type: "value", ID: id, Full: full, Base: ev.Previous.Version(), Version: ev.Version, Value: payload})
		})
		if err != nil {
			gate.Unlock()
			return nil, err
		}
		if !snap.Valid() {
			s.send(frame{Type: "waiting", ID: id})
			gate.Unlock()
			return sub, nil
		}
		payload, err := codec.Encode(*new(T), snap.Value(), true)
		if err != nil {
			gate.Unlock()
			sub.Unsubscribe()
			return nil, err
		}
		s.send(frame{Type: "value", ID: id, Full: true, Version: snap.Version(), Value: payload})
		gate.Unlock()
		return sub, nil
	}}
	return nil
}

// Watch is one explicit reference to a shared remote mirror. Close is idempotent.
type Watch struct {
	once  sync.Once
	p     *Peer
	value *imported
}

// Import obtains a shared, scoped mirror leaf and keeps an upstream watch alive.
// Its value is initially invalid, not an invented zero. Multiple callers using
// the same address/codec get the same node. Close the watch when demand ends.
// Mirrors retain the last received value on disconnect or cancellation; consult
// Status for freshness. Automatic integration with graph demand is future work.
// Treat mirrors as read-only. Inactive bindings are reused, not unregistered;
// MaxSubscriptions bounds the number of distinct imported addresses over this
// Peer's lifetime, including inactive bindings. Codec implementations must agree
// when sharing a name; the peer checks the name and Go value type, not functions.
func Import[T any](p *Peer, address Address, codec Codec[T]) (reco.Node[T], *Watch, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var zero reco.Node[T]
	if p.closed || !address.valid() || !codec.valid() {
		return zero, nil, errors.New("metagraph: invalid import or closed peer")
	}
	if _, ok := p.exports[address]; ok {
		return zero, nil, errors.New("metagraph: address is locally authoritative")
	}
	if old := p.imports[address]; old != nil {
		n, ok := old.node.(reco.Node[T])
		if !ok || old.codec != codec.Name {
			return zero, nil, errors.New("metagraph: incompatible shared import")
		}
		if old.refs == 0 {
			p.activate(old)
		}
		old.refs++
		return n, &Watch{p: p, value: old}, nil
	}
	if len(p.imports) >= p.opts.MaxSubscriptions {
		return zero, nil, errors.New("metagraph: subscription limit")
	}
	n := reco.In(new(reco.Scope), reco.Data[T](address.Class))
	if err := p.opts.Graph.Register(n); err != nil {
		return zero, nil, err
	}
	b := &imported{address: address, codec: codec.Name, node: n, refs: 1, apply: func(tx *reco.Tx, raw json.RawMessage, full bool) error { return codec.Apply(tx, n, raw, full) }}
	p.imports[address] = b
	p.activate(b)
	return n, &Watch{p: p, value: b}, nil
}

// activate and nextSubscribe require p.mu. Pace snapshot requests so a large
// initial watch set cannot flood either peer's bounded output queue. One setup
// is in flight; established watches stream concurrently without this restriction.
func (p *Peer) activate(b *imported) {
	p.nextID++
	b.id = p.nextID
	p.byID[b.id] = b
	if s := p.session; s != nil && s.ready {
		b.pending = s.pending.PushBack(b)
		s.nextSubscribe()
	}
}

func (s *session) nextSubscribe() {
	if s.inFlight != 0 || s.pending.Len() == 0 {
		return
	}
	e := s.pending.Front()
	b := e.Value.(*imported)
	s.pending.Remove(e)
	b.pending = nil
	b.sent = true
	s.inFlight = b.id
	s.send(frame{Type: "subscribe", ID: b.id, Address: &b.address, Codec: b.codec})
}

func (s *session) subscribed(id uint64) {
	if s.inFlight == id {
		s.inFlight = 0
		s.nextSubscribe()
	}
}

// Close releases this reference and waits for any in-flight mirror application.
// Do not hold locks needed by Options.Apply, or call it from a graph callback.
func (w *Watch) Close() {
	w.once.Do(func() {
		p := w.p
		p.delivery.Lock()
		defer p.delivery.Unlock()
		p.mu.Lock()
		defer p.mu.Unlock()
		b := w.value
		w.p, w.value = nil, nil
		b.refs--
		if b.refs != 0 {
			return
		}
		delete(p.byID, b.id)
		if b.ready {
			p.status.Ready--
			b.ready = false
		}
		if s := p.session; s != nil && s.ready {
			if b.pending != nil {
				s.pending.Remove(b.pending)
				b.pending = nil
			}
			if b.sent {
				s.send(frame{Type: "unsubscribe", ID: b.id})
				b.sent = false
			}
			s.subscribed(b.id)
		}
	})
}

func (p *Peer) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.status
	st.Watching = len(p.byID)
	return st
}

func (p *Peer) notify() {
	if p.opts.OnStatus != nil {
		p.opts.OnStatus(p.Status())
	}
}

// Close disconnects this peer. Callers must also cancel the context passed to
// Run and wait for their serving goroutine before discarding their Graph.
func (p *Peer) Close() {
	p.mu.Lock()
	p.closed = true
	if p.session != nil {
		p.session.cancel()
	}
	p.mu.Unlock()
}

// Disconnect drops only the current session; Run will reconnect and resnapshot.
func (p *Peer) Disconnect() {
	p.mu.Lock()
	if p.session != nil {
		p.session.cancel()
	}
	p.mu.Unlock()
}

// ServeHTTP accepts server-to-server connections only, not browser-originated
// WebSockets. This is not authentication: use only on trusted networks.
func (p *Peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "" {
		http.Error(w, "peer connections are not a browser API", http.StatusForbidden)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	_ = p.Serve(r.Context(), c)
}

// Run dials and reconnects until ctx is canceled. Configure exactly one dialer
// per peer pair; the same socket carries subscriptions in BOTH directions.
func (p *Peer) Run(ctx context.Context, url string) error {
	delay := 200 * time.Millisecond
	for {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed || ctx.Err() != nil {
			return ctx.Err()
		}
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		c, _, err := websocket.Dial(dialCtx, url, nil)
		cancel()
		if err == nil {
			delay = 200 * time.Millisecond
			err = p.Serve(ctx, c)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			p.mu.Lock()
			p.status.LastError = err.Error()
			p.mu.Unlock()
			p.notify()
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 5*time.Second)
	}
}

type frame struct {
	Type      string          `json:"type"`
	Protocol  int             `json:"protocol,omitempty"`
	Metagraph string          `json:"metagraph,omitempty"`
	Peer      string          `json:"peer,omitempty"`
	Epoch     string          `json:"epoch,omitempty"`
	ID        uint64          `json:"id,omitempty"`
	Address   *Address        `json:"address,omitempty"`
	Codec     string          `json:"codec,omitempty"`
	Full      bool            `json:"full,omitempty"`
	Base      reco.Version    `json:"base,omitempty"`
	Version   reco.Version    `json:"version,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
}

type session struct {
	p           *Peer
	ctx         context.Context
	cancel      context.CancelFunc
	conn        *websocket.Conn
	ready       bool // guarded by Peer.mu
	queueMu     sync.Mutex
	queue       chan []byte
	queuedBytes int
	exports     map[uint64]served // reader owns
	addresses   map[Address]uint64
	pending     list.List // guarded by Peer.mu
	inFlight    uint64    // subscription awaiting snapshot or waiting acknowledgment
}

type served struct {
	address Address
	sub     reco.SubscriptionHandle
}

// send is safe in a graph callback: bounded encoding/queue work, no network I/O.
func (s *session) send(f frame) {
	data, err := json.Marshal(f)
	if err != nil || len(data) > s.p.opts.MaxMessageBytes {
		s.cancel()
		return
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	if s.queuedBytes+len(data) > s.p.opts.QueueBytes {
		s.cancel()
		return
	}
	select {
	case s.queue <- data:
		s.queuedBytes += len(data)
	default:
		s.cancel()
	}
}

// Serve owns c until disconnect. Only one session may run per Peer. Old readers
// and subscriptions are gone before a replacement session can be installed.
func (p *Peer) Serve(ctx context.Context, c *websocket.Conn) (retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &session{p: p, ctx: ctx, cancel: cancel, conn: c, queue: make(chan []byte, p.opts.QueueMessages), exports: make(map[uint64]served), addresses: make(map[Address]uint64)}
	defer c.CloseNow()
	defer cancel()
	c.SetReadLimit(int64(p.opts.MaxMessageBytes))
	p.mu.Lock()
	if p.closed || p.session != nil {
		p.mu.Unlock()
		return errors.New("metagraph: peer already connected or closed")
	}
	p.session = s
	p.mu.Unlock()
	defer func() {
		cancel()
		c.CloseNow()
		for _, e := range s.exports {
			e.sub.Unsubscribe()
		}
		p.delivery.Lock()
		p.mu.Lock()
		p.session = nil
		p.status.Connected = false
		p.status.Serving = 0
		p.status.Ready = 0
		if retErr != nil {
			p.status.LastError = retErr.Error()
		}
		for _, b := range p.byID {
			b.ready = false
			b.sent = false
			b.pending = nil
		}
		p.mu.Unlock()
		p.delivery.Unlock()
		p.notify()
	}()
	helloCtx, done := context.WithTimeout(ctx, 5*time.Second)
	err := wsjson.Write(helloCtx, c, frame{Type: "hello", Protocol: 1, Metagraph: p.opts.Metagraph, Peer: p.opts.Local, Epoch: p.epoch})
	var hello frame
	if err == nil {
		err = wsjson.Read(helloCtx, c, &hello)
	}
	done()
	if err != nil {
		return err
	}
	if hello.Type != "hello" || hello.Protocol != 1 || hello.Metagraph != p.opts.Metagraph || hello.Peer != p.opts.Remote || hello.Epoch == "" {
		return errors.New("metagraph: incompatible hello or unexpected peer")
	}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-s.queue:
				s.queueMu.Lock()
				s.queuedBytes -= len(data)
				s.queueMu.Unlock()
				wc, done := context.WithTimeout(ctx, 5*time.Second)
				err := c.Write(wc, websocket.MessageText, data)
				done()
				if err != nil {
					return
				}
				if p.opts.OnWire != nil {
					p.opts.OnWire("sent", data)
				}
			}
		}
	}()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				pc, done := context.WithTimeout(ctx, 5*time.Second)
				err := c.Ping(pc)
				done()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); c.CloseNow(); <-writerDone; <-heartbeatDone }()
	p.mu.Lock()
	s.ready = true
	p.status.Connected = true
	p.status.Epoch = hello.Epoch
	p.status.LastError = ""
	for _, b := range p.byID {
		b.ready = false
		b.pending = s.pending.PushBack(b)
	}
	s.nextSubscribe()
	p.mu.Unlock()
	p.notify()
	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			return err
		}
		var f frame
		if err := json.Unmarshal(raw, &f); err != nil {
			return err
		}
		if p.opts.OnWire != nil {
			p.opts.OnWire("received", raw)
		}
		if err := s.receive(f); err != nil {
			return err
		}
	}
}

func (s *session) receive(f frame) error {
	p := s.p
	if f.ID == 0 {
		return errors.New("metagraph: missing subscription ID")
	}
	switch f.Type {
	case "subscribe":
		if f.Address == nil || !f.Address.valid() || s.exports[f.ID].sub != nil || s.addresses[*f.Address] != 0 || len(s.exports) >= p.opts.MaxSubscriptions {
			return errors.New("metagraph: invalid/duplicate subscription")
		}
		p.mu.Lock()
		e, ok := p.exports[*f.Address]
		p.mu.Unlock()
		if !ok || e.codec != f.Codec {
			return errors.New("metagraph: not authoritative for requested address/schema")
		}
		sub, err := e.start(s, f.ID)
		if err != nil {
			return err
		}
		s.exports[f.ID] = served{*f.Address, sub}
		s.addresses[*f.Address] = f.ID
		p.mu.Lock()
		p.status.Serving = len(s.exports)
		p.mu.Unlock()
		p.notify()
	case "unsubscribe":
		if e := s.exports[f.ID]; e.sub != nil {
			e.sub.Unsubscribe()
			delete(s.exports, f.ID)
			delete(s.addresses, e.address)
			p.mu.Lock()
			p.status.Serving = len(s.exports)
			p.mu.Unlock()
			p.notify()
		}
	case "value":
		return s.apply(f)
	case "waiting":
		p.mu.Lock()
		b := p.byID[f.ID]
		if b != nil && (!b.sent || b.ready || s.inFlight != f.ID) {
			p.mu.Unlock()
			return errors.New("metagraph: unexpected waiting acknowledgment")
		}
		s.subscribed(f.ID)
		p.mu.Unlock()
	default:
		return fmt.Errorf("metagraph: unknown message type %q", f.Type)
	}
	return nil
}

func (s *session) apply(f frame) error {
	p := s.p
	p.delivery.Lock()
	p.mu.Lock()
	b := p.byID[f.ID]
	if b == nil {
		p.mu.Unlock()
		p.delivery.Unlock()
		return nil
	} // queued before unsubscribe
	if !b.sent || (f.Full && b.ready) || (!f.Full && (!b.ready || f.Base != b.version || f.Version <= f.Base)) || len(f.Value) == 0 {
		p.mu.Unlock()
		p.delivery.Unlock()
		return errors.New("metagraph: delta base mismatch; reconnect for snapshot")
	}
	p.mu.Unlock()
	err := p.opts.Apply(func(tx *reco.Tx) error { return b.apply(tx, f.Value, f.Full) })
	if err == nil {
		p.mu.Lock()
		b.version = f.Version
		if !b.ready {
			b.ready = true
			p.status.Ready++
		}
		s.subscribed(f.ID)
		p.mu.Unlock()
	}
	p.delivery.Unlock()
	if err == nil {
		p.notify()
	}
	return err
}

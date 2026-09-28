package main

import (
	"fmt"
	"net/url"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/metagraph"
)

type demoOptions struct {
	PeerID, PeerURL, Metagraph string
}

type demoInfo struct {
	Local     string            `json:"local"`
	Remote    string            `json:"remote"`
	PeerURL   string            `json:"peerURL"`
	Metagraph string            `json:"metagraph"`
	Owners    map[string]string `json:"owners"`
}

type wireEvent struct {
	Direction string `json:"direction"`
	Data      string `json:"data"`
}

func (d *demo) owns(id string) bool {
	return d.info == nil || d.info.Owners[id] == d.info.Local
}

func (d *demo) configurePeer(opts demoOptions) error {
	if opts.PeerID == "" && opts.PeerURL == "" {
		return nil
	}
	if opts.PeerID != "a" && opts.PeerID != "b" {
		return fmt.Errorf("peer-id must be a or b")
	}
	u, err := url.Parse(opts.PeerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("peer-url must be an http(s) origin, such as http://localhost:8032")
	}
	if opts.Metagraph == "" {
		opts.Metagraph = "word-garden"
	}
	remote := "b"
	if opts.PeerID == "b" {
		remote = "a"
	}
	d.info = &demoInfo{Local: opts.PeerID, Remote: remote, PeerURL: u.String(), Metagraph: opts.Metagraph, Owners: map[string]string{"a": "a", "label": "a", "b": "b", "weight": "b"}}
	d.peer, err = metagraph.New(metagraph.Options{
		Graph: d.g, Metagraph: opts.Metagraph, Local: opts.PeerID, Remote: remote,
		Apply: func(fn func(*reco.Tx) error) error {
			d.mu.Lock()
			defer d.mu.Unlock()
			d.pending = nil
			if err := d.g.Update(fn); err != nil {
				return err
			}
			return d.publish()
		},
		OnStatus: func(st metagraph.Status) { d.broadcast(message{Type: "peer", Peer: &st}) },
		OnWire: func(direction string, data []byte) {
			// Wire inspection is an intentionally bounded demo convenience.
			if len(data) > 8192 {
				data = append(append([]byte{}, data[:8192]...), []byte("… [truncated]")...)
			}
			d.broadcast(message{Type: "peer-wire", Wire: &wireEvent{Direction: direction, Data: string(data)}})
		},
	})
	if err != nil {
		return err
	}
	if err = bindLeaf(d, &d.a, metagraph.Set[string]("words-v1")); err != nil {
		return err
	}
	if err = bindLeaf(d, &d.b, metagraph.Set[string]("words-v1")); err != nil {
		return err
	}
	if err = bindLeaf(d, &d.label, metagraph.JSON[string]("label-v1")); err != nil {
		return err
	}
	return bindLeaf(d, &d.weight, metagraph.JSON[int]("weight-v1"))
}

func bindLeaf[T any](d *demo, n *reco.Node[T], codec metagraph.Codec[T]) error {
	a := metagraph.Address{Namespace: "garden", Class: n.ClassName()}
	if d.owns(string(n.ClassName())) {
		return metagraph.Export(d.peer, a, *n, codec)
	}
	mirror, watch, err := metagraph.Import(d.peer, a, codec)
	if err != nil {
		return err
	}
	*n = mirror
	d.watches = append(d.watches, watch)
	return nil
}

func (d *demo) broadcast(m message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for c := range d.clients {
		d.send(c, m)
	}
}

func (d *demo) peerSocketURL() string {
	u, _ := url.Parse(d.info.PeerURL)
	u.Path = "/peer"
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	return u.String()
}

// Command webdemo runs an in-memory reco playground with optional peer watches.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address (no authentication; use a trusted network)")
	peerID := flag.String("peer-id", "", "optional metagraph owner: a (set A + label), or b (set B + weight)")
	peerURL := flag.String("peer-url", "", "other process's http(s) origin; only a dials, both watch over that socket")
	group := flag.String("metagraph", "word-garden", "logical graph identity; must match on both peers")
	flag.Parse()
	demo, err := newDemoWithOptions(demoOptions{PeerID: *peerID, PeerURL: *peerURL, Metagraph: *group})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("reco playground: http://%s (shared in-memory graph; restart resets it)", *listen)
	server := &http.Server{Addr: *listen, Handler: demo, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if demo.peer != nil {
		defer demo.peer.Close()
		log.Printf("metagraph %q: process %s, peer %s at %s (trusted networks only)", *group, *peerID, demo.info.Remote, *peerURL)
		if *peerID == "a" {
			go func() { _ = demo.peer.Run(ctx, demo.peerSocketURL()) }()
		}
	}
	go func() {
		<-ctx.Done()
		if demo.peer != nil {
			demo.peer.Close()
		}
		_ = server.Close()
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// Command webdemo runs a single-process, in-memory reco playground.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address (no authentication; use a trusted network)")
	flag.Parse()
	demo, err := newDemo()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("reco playground: http://%s (shared in-memory graph; restart resets it)", *listen)
	server := &http.Server{Addr: *listen, Handler: demo, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

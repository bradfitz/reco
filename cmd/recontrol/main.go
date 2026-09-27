// Command recontrol runs the prototype reco-backed Tailscale control server.
package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	flag.Parse()

	s, err := newControlServer()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recontrol listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, s))
}

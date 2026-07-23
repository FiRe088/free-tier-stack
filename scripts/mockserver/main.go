package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// mockserver serves many endpoints on one port for load-testing Pulse's
// concurrent checker against realistic latency/failure variance, without
// hammering real external services.
//
// Routes:
//   /ok/N       -> always 200, small random latency (0-50ms)
//   /slow/N     -> 200, but latency 200-800ms (tests CHECK_TIMEOUT_MS handling)
//   /fail/N     -> always 500
//   /flaky/N    -> ~30% chance of 500, else 200 (tests DOWN_THRESHOLD flap resistance)
func main() {
	port := flag.Int("port", 9100, "port to listen on")
	flag.Parse()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	mux := http.NewServeMux()

	mux.HandleFunc("/ok/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(rng.Intn(50)) * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/slow/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(200+rng.Intn(600)) * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/fail/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	mux.HandleFunc("/flaky/", func(w http.ResponseWriter, r *http.Request) {
		if rng.Float64() < 0.3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("mockserver: listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

package base

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/openark/golib/log"
)

// HttpConfig contains the configuration of the http server
type HttpConfig struct {
	ListenAddr string
}

// handleHttpPing handles a ping request
func handleHttpPing(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}

// NewHttpServer runs the http server
func NewHttpServer(config HttpConfig) {
	http.HandleFunc("/_ping", handleHttpPing)
	log.Errore(http.ListenAndServe(config.ListenAddr, nil))
}

package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"github.com/aricart/nks/kvserver"
	"net/http"
	"runtime"
)

var srv *kvserver.KvServerOptions

func main() {
	opts := kvserver.DefaultKvServerOptions()

	flag.BoolVar(&opts.Embed, "e", false, "Embed gnatsd")
	flag.StringVar(&opts.Host, "h", "localhost", "server host")
	flag.IntVar(&opts.Port, "p", 4222, "NATS Server Port")
	flag.StringVar(&opts.DataDir, "d", "/private/tmp", "datadir")
	flag.StringVar(&opts.Prefix, "-prefix", kvserver.DefaultPrefix, "keystore prefix")
	flag.Parse()

	go func() {
		http.ListenAndServe(fmt.Sprintf(":%d", opts.MonPort), http.DefaultServeMux)
	}()
	fmt.Printf("Monitoring http://localhost:%d/debug/vars\n", opts.MonPort)

	server := kvserver.NewKvServer(opts)
	server.Start()

	runtime.Goexit()
}

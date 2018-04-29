package main

import (
	"flag"
	"github.com/aricart/nks/kvserver"
	"runtime"
)

var srv *kvserver.KvServerOptions

func main() {
	opts := &kvserver.KvServerOptions{}

	flag.BoolVar(&opts.Embed, "e", false, "Embed gnatsd")
	flag.StringVar(&opts.Host, "h", "localhost", "server host")
	flag.IntVar(&opts.Port, "p", 4222, "NATS Server Port")
	flag.StringVar(&opts.DataDir, "d", "/tmp", "datadir")
	flag.StringVar(&opts.Prefix, "-prefix", kvserver.DefaultPrefix, "keystore prefix")
	flag.Parse()

	server := kvserver.NewKvServer(opts)
	server.Start()

	runtime.Goexit()
}

package main

import (
	"flag"
	"github.com/aricart/nks/kvserver"
	"runtime"
)

var srv *kvserver.KvOpts

func main() {
	s := &kvserver.KvOpts{}

	flag.BoolVar(&s.Embed, "e", false, "Embed gnatsd")
	flag.StringVar(&s.Host, "h", "localhost", "server host")
	flag.IntVar(&s.Port, "p", 4222, "NATS Server Port")
	flag.StringVar(&s.DataDir, "d", "/tmp", "datadir")
	flag.StringVar(&s.Prefix, "-prefix", kvserver.DefaultPrefix, "keystore prefix")
	flag.Parse()

	s.Start()
	runtime.Goexit()
}

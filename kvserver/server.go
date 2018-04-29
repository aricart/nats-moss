package kvserver

import (
	"fmt"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"net"
)

const DefaultPrefix = "keystore"

type KvServerOptions struct {
	DataDir string
	Embed   bool
	Host    string
	Port    int
	Prefix  string
}

func DefaultKvServerOptions() *KvServerOptions {
	kvopts := KvServerOptions{}
	kvopts.Embed = true
	kvopts.Host = "localhost"
	kvopts.Port = -1
	kvopts.DataDir = "/tmp"
	kvopts.Prefix = DefaultPrefix

	return &kvopts
}

type KvServer struct {
	KvServerOptions
	subject string
	gnatsd  *server.Server
	nc      *nats.Conn
	kvs     *Kvs
}

func NewKvServer(options *KvServerOptions) *KvServer {
	if options == nil {
		options = DefaultKvServerOptions()
	}
	v := KvServer{}
	v.KvServerOptions = *options
	return &v
}

func (s *KvServerOptions) GetOptions() *KvServerOptions {
	v := DefaultKvServerOptions()
	v.Embed = s.Embed
	v.Host = s.Host
	v.Port = s.Port
	v.DataDir = s.DataDir
	v.Prefix = s.Prefix

	return v
}

func (s *KvServer) isEmbedded() bool {
	return s.Embed
}

func (s *KvServer) Start() {
	s.handleSignals()
	s.kvs = NewKvs(s.DataDir)
	s.kvs.Start()
	s.maybeStartServer()
	s.startClient()
}

func (s *KvServer) GetEmbeddedPort() int {
	return s.gnatsd.Addr().(*net.TCPAddr).Port
}

func (s *KvServer) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGINT:
				s.Stop()
				os.Exit(0)
			}
		}
	}()
}

func (s *KvServer) Stop() {
	if s.gnatsd != nil {
		fmt.Println("Stopping embedded gnatsd")
		s.gnatsd.Shutdown()
	}

	s.kvs.Stop()
}

func (s *KvServer) maybeStartServer() {
	if s.isEmbedded() {
		fmt.Println("Starting gnatsd")
		opts := server.Options{
			Host:           "localhost",
			Port:           s.Port,
			NoLog:          false,
			NoSigs:         true,
			MaxControlLine: 1024,
		}
		s.gnatsd = server.New(&opts)
		if s.gnatsd == nil {
			panic("unable to create gnatsd")
		}

		go s.gnatsd.Start()

		if s.isEmbedded() && !s.gnatsd.ReadyForConnections(5 * time.Second) {
			panic("unable to start embedded server")
		}

		if s.Port == -1 {
			s.Port = s.GetEmbeddedPort()
		}
	}
}

func (s *KvServer) startClient() {
	var err error
	url := fmt.Sprintf("nats://%s:%d", s.Host, s.Port)
	s.nc, err = nats.Connect(url)
	if err != nil {
		panic(fmt.Sprintf("unable to connect to server [%s]: %v", url, err))
	}
	fmt.Printf("Connected [%s]\n", url)

	s.subject = ""
	if s.isEmbedded() && s.Prefix != DefaultPrefix {
		s.subject = s.Prefix
	}

	if s.subject == "" {
		s.nc.Subscribe(">", s.handler)
		fmt.Println("Listening for keystore requests on >", s.subject)

	} else {
		var sub = fmt.Sprintf("%s.>", s.Prefix)
		s.nc.Subscribe(sub, s.handler)
		fmt.Printf("Listening for keystore requests on [%s]\n", sub)
	}
}

func (s *KvServer) handler(msg *nats.Msg) {
	// don't deal with _INBOX messages
	if strings.HasPrefix(msg.Subject, "_INBOX.") {
		return
	}

	key := []byte(msg.Subject)
	if s.subject != "" {
		key = key[len(s.Prefix):]
	}

	var err error
	var op = ""
	if len(msg.Data) > 0 {
		op = "[P]"
		err = s.kvs.Put(key, msg.Data)
	} else {
		if msg.Reply == "" {
			op = "[D]"
			err = s.kvs.Delete(key)
		} else {
			var data []byte
			op = "[G]"
			data, err = s.kvs.Get(key)
			if err == nil {
				s.nc.Publish(msg.Reply, data)
			}
		}
	}
	if err != nil {
		fmt.Printf("error %s: %v", op, err)
	}
}

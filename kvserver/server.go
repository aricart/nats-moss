package kvserver

import (
	"fmt"
	"github.com/couchbase/moss"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const DefaultPrefix = "keystore"

type KvOpts struct {
	Embed      bool
	Host       string
	Port       int
	DataDir    string
	Prefix     string
	subject    string
	store      *moss.Store
	collection moss.Collection
	gnatsd     *server.Server
	nc         *nats.Conn
}

func DefaultKvOpts() *KvOpts {
	kvopts := KvOpts{}
	kvopts.Embed = true
	kvopts.Host = "localhost"
	kvopts.Port = 4222
	kvopts.DataDir = "/tmp"
	kvopts.Prefix = DefaultPrefix

	return &kvopts
}

func (s *KvOpts) isEmbedded() bool {
	return s.Embed
}

func (s *KvOpts) Start() {
	s.handleSignals()
	s.maybeStartServer()
	s.startDB()
	s.startClient()
}

func (s *KvOpts) handleSignals() {
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

func (s *KvOpts) Stop() {
	if s.gnatsd != nil {
		fmt.Println("Stopping embedded gnatsd")
		s.gnatsd.Shutdown()
	}
	fmt.Println("Stopping store")
	s.store.Close()
}

func (s *KvOpts) maybeStartServer() {
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

		if s.isEmbedded() && !s.gnatsd.ReadyForConnections(5*time.Second) {
			panic("unable to start embedded server")
		}
	}
}

func (s *KvOpts) startDB() {
	var err error
	s.store, s.collection, err = moss.OpenStoreCollection(s.DataDir, moss.StoreOptions{}, moss.StorePersistOptions{})
	if err != nil {
		panic(fmt.Sprintf("error opening store collection: %v", err))
	}
}

func (s *KvOpts) startClient() {
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

func (s *KvOpts) handler(msg *nats.Msg) {
	if strings.HasPrefix(msg.Subject, "_INBOX.") {
		return
	}
	key := []byte(msg.Subject)
	if s.subject != "" {
		key = key[len(s.Prefix):]
	}

	if len(msg.Data) > 0 {
		// put
		batch, err := s.collection.NewBatch(0, 0)
		if err != nil {
			panic(fmt.Sprintf("error creating batch: %v", err))
		}
		defer batch.Close()
		batch.Set(key, msg.Data)
		s.collection.ExecuteBatch(batch, moss.WriteOptions{})
		fmt.Printf("[W] %s=%s\n", string(key), string(msg.Data))
	} else {
		if msg.Reply == "" {
			// delete
			batch, err := s.collection.NewBatch(0, 0)
			if err != nil {
				panic(fmt.Sprintf("error creating batch: %v", err))
			}
			defer batch.Close()
			batch.Set(key, []byte(""))
			s.collection.ExecuteBatch(batch, moss.WriteOptions{})
			fmt.Printf("[D] %s\n", string(key))

		} else {
			ss, err := s.collection.Snapshot()
			if err != nil {
				panic(fmt.Sprintf("error creating snapshot: %v", err))
			}
			defer ss.Close()
			data, err := ss.Get(key, moss.ReadOptions{})
			if err != nil {
				panic(fmt.Sprintf("error getting value: %v", err))
			}
			s.nc.Publish(msg.Reply, data)
			fmt.Printf("[R] %s=%s\n", string(key), string(data))

		}
	}
}

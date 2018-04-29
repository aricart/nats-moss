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
	"net"
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
	storeDone  chan string

	currentBatch    moss.Batch
	currentSnapshot moss.Snapshot
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

func (s *KvOpts) GetKvOpts() *KvOpts {
	v := DefaultKvOpts()
	v.Embed = s.Embed
	v.Host = s.Host
	v.Port = s.Port
	v.DataDir = s.DataDir
	v.Prefix = s.Prefix

	return v
}

func (s *KvOpts) isEmbedded() bool {
	return s.Embed
}

func (s *KvOpts) Start() {
	s.storeDone = make(chan string, 1)
	s.handleSignals()
	s.startDB()
	s.maybeStartServer()
	s.startClient()
}

func (s *KvOpts) GetEmbeddedPort() int {
	return s.gnatsd.Addr().(*net.TCPAddr).Port
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

	// close and save
	s.invalidateBatch()
	s.invalidateSnapshot()

	// wait for all writes to happen
	for {
		stats, er := s.collection.Stats()
		if er == nil && stats.CurDirtyOps <= 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	fmt.Println("Stopping store")
	err := s.collection.Close()
	if err != nil {
		panic(err)
	}

	err = s.store.Close()
	if err != nil {
		panic(err)
	}
	// wait for the store to finish
	<-s.storeDone
	close(s.storeDone)
	fmt.Println("stopped.")
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

		if s.Port == -1 {
			s.Port = s.GetEmbeddedPort()
		}
	}
}

func (s *KvOpts) storeEventHandler(event moss.Event) {
	if event.Kind == moss.EventKindClose {
		s.storeDone <- "done"
	}
}

func (s *KvOpts) startDB() {
	var err error

	opts := moss.CollectionOptions{
		OnEvent: s.storeEventHandler,
		OnError: func(err error) {
			panic(err)
		},
	}
	s.store, s.collection, err = moss.OpenStoreCollection(s.DataDir, moss.StoreOptions{
		CollectionOptions: opts,
		CompactionSync:    true,
	}, moss.StorePersistOptions{})
	if err != nil || s.store == nil || s.collection == nil {
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

func (s *KvOpts) put(key, value []byte) error {
	return s.getCurrentBatch().Set(key, value)
}

func (s *KvOpts) delete(key []byte) error {
	return s.getCurrentBatch().Del(key)
}

func (s *KvOpts) get(key []byte) []byte {
	data, err := s.getCurrentSnapshot().Get(key, moss.ReadOptions{})
	if err != nil {
		panic(err)
	}
	return data
}

func (s *KvOpts) getCurrentBatch() moss.Batch {
	var err error
	if s.currentBatch == nil {
		s.currentBatch, err = s.collection.NewBatch(0, 0)
		if err != nil {
			panic(err)
		}
	}
	return s.currentBatch
}

func (s *KvOpts) invalidateBatch() {
	var err error
	if s.currentBatch != nil {
		err = s.collection.ExecuteBatch(s.currentBatch, moss.WriteOptions{})
		if err != nil {
			panic(err)
		}
		s.currentBatch.Close()
		s.currentBatch = nil
	}
}

func (s *KvOpts) getCurrentSnapshot() moss.Snapshot {
	var err error
	if s.currentSnapshot == nil {
		s.currentSnapshot, err = s.collection.Snapshot()
		if err != nil {
			panic(err)
		}
	}
	return s.currentSnapshot
}

func (s *KvOpts) invalidateSnapshot() {
	var err error
	if s.currentSnapshot != nil {
		err = s.currentSnapshot.Close()
		if err != nil {
			panic(err)
		}
		s.currentSnapshot = nil
	}
}

func (s *KvOpts) handler(msg *nats.Msg) {
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
		s.invalidateSnapshot()
		op = "[P]"
		err = s.put(key, msg.Data)
		s.invalidateSnapshot()
	} else {
		if msg.Reply == "" {
			s.invalidateSnapshot()
			op = "[D]"
			err = s.delete(key)
		} else {
			s.invalidateBatch()
			var data []byte
			op = "[G]"
			data = s.get(key)
			s.nc.Publish(msg.Reply, data)
		}
	}
	if err != nil {
		fmt.Printf("error %s: %v", op, err)
	}
}

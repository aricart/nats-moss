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
	storeDone  chan string
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
	signal := <-s.storeDone
	fmt.Printf("got store event %s\n", signal)
	close(s.storeDone)
	fmt.Printf("stopped: %v\n", s.store.IsAborted())

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

func (s *KvOpts) storeEventHandler(event moss.Event) {
	if event.Kind == moss.EventKindClose {
		s.storeDone <- "done"
	}
}

func eventToString(e moss.Event) string {
	switch e.Kind {
	case moss.EventKindCloseStart:
		return "EventKindCloseStart"
	case moss.EventKindClose:
		return "EventKindClose"
	case moss.EventKindMergerProgress:
		return "EventKindMergerProgress"
	case moss.EventKindPersisterProgress:
		return "EventKindPersisterProgress"
	case moss.EventKindBatchExecuteStart:
		return "EventKindBatchExecuteStart"
	case moss.EventKindBatchExecute:
		return "EventKindBatchExecute"
	default:
		return "Unknown"
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
	// put
	batch, err := s.collection.NewBatch(0, 0)
	if err != nil {
		return err
	}
	batch.Set(key, value)
	err = s.collection.ExecuteBatch(batch, moss.WriteOptions{})
	if err != nil {
		return err
	}
	err = batch.Close()
	if err != nil {
		return err
	}
	//fmt.Printf("[W] %s=%s\n", string(key), string(value))
	return nil
}

func (s *KvOpts) delete(key []byte) error {
	batch, err := s.collection.NewBatch(0, 0)
	if err != nil {
		return err
	}
	batch.Set(key, []byte(""))
	err = s.collection.ExecuteBatch(batch, moss.WriteOptions{})
	if err != nil {
		return err
	}
	err = batch.Close()
	if err != nil {
		return err
	}
	//fmt.Printf("[D] %s\n", string(key))
	return nil
}

func (s *KvOpts) get(key []byte) ([]byte, error) {
	ss, err := s.collection.Snapshot()
	if err != nil {
		return nil, err
	}
	data, err := ss.Get(key, moss.ReadOptions{})
	if err != nil {
		return nil, err
	}
	err = ss.Close()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("[R] %s=%s\n", string(key), string(data))
	return data, nil
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
		op = "[P]"
		err = s.put(key, msg.Data)
	} else {
		if msg.Reply == "" {
			op = "[D]"
			err = s.delete(key)
		} else {
			var data []byte
			op = "[G]"
			data, err = s.get(key)
			s.nc.Publish(msg.Reply, data)
		}
	}
	if err != nil {
		fmt.Printf("error %s: %v", op, err)
	}
}

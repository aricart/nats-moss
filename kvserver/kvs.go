package kvserver

import (
	"github.com/couchbase/moss"
	"fmt"
	"time"
	"expvar"
	"io/ioutil"
)

type Kvs struct {
	store           *moss.Store
	collection      moss.Collection
	storeDone       chan string
	currentBatch    moss.Batch
	currentSnapshot moss.Snapshot
	dataDir         string

	Metrics
}

type Metrics struct {
	put Metric
	get Metric
	del Metric
}

func (m *Metrics) Dump() {
	fmt.Println("KVS Store")
	fmt.Printf("GET %s\n", m.get.Dump())
	fmt.Printf("PUT %s\n", m.put.Dump())
	fmt.Printf("DEL %s\n", m.del.Dump())
}

func (m *Metric) reset(name string) bool {
	didReset := false
	names := []string{"TimeNanos", "Requests", "KeyBytes", "ValueBytes"}
	for _, n := range names {
		varName := fmt.Sprintf("%s%s", name, n)
		v := expvar.Get(varName)
		if v != nil {
			i, ok := v.(*expvar.Int)
			if ok {
				i.Set(0)

				// rebind them to the new struct
				switch n {
				case "TimeNanos":
					m.nanos = i
				case "Requests":
					m.requests = i
				case "KeyBytes":
					m.keyBytes = i
				case "ValueBytes":
					m.valueBytes = i
				}
				didReset = true
			}
		}
	}
	return didReset
}

type Metric struct {
	nanos      *expvar.Int
	requests   *expvar.Int
	keyBytes   *expvar.Int
	valueBytes *expvar.Int
}

func (m *Metric) Dump() string {
	if m.requests.Value() == 0 {
		return "0"
	}
	d := time.Duration(m.nanos.Value())
	rate := float64(m.requests.Value()) / d.Seconds()
	return fmt.Sprintf("%d / %s [%.1f req/sec]", m.requests.Value(), d.String(), rate)
}

func (m *Metric) init(name string) {
	if ! m.reset(name) {
		m.nanos = expvar.NewInt(fmt.Sprintf("%sTimeNanos", name))
		m.requests = expvar.NewInt(fmt.Sprintf("%sRequests", name))
		m.keyBytes = expvar.NewInt(fmt.Sprintf("%sKeyBytes", name))
		m.valueBytes = expvar.NewInt(fmt.Sprintf("%sValueBytes", name))
	}
}

func NewKvs(dataDir string) *Kvs {
	if dataDir == "" {
		var err error
		dataDir, err = ioutil.TempDir("/tmp", "nks")
		if err != nil {
			panic(err)
		}
	}
	kvs := &Kvs{dataDir: dataDir,}

	kvs.get.init("get")
	kvs.put.init("put")
	kvs.del.init("del")

	return kvs
}

func (k *Kvs) storeEventHandler(event moss.Event) {
	if event.Kind == moss.EventKindClose {
		k.storeDone <- "done"
	}
}

func (k *Kvs) Start() {
	k.storeDone = make(chan string, 1)

	var err error

	opts := moss.CollectionOptions{
		OnEvent: k.storeEventHandler,
		OnError: func(err error) {
			panic(err)
		},
	}
	k.store, k.collection, err = moss.OpenStoreCollection(k.dataDir, moss.StoreOptions{
		CollectionOptions: opts,
		CompactionSync:    true,
	}, moss.StorePersistOptions{})
	if err != nil || k.store == nil || k.collection == nil {
		panic(fmt.Sprintf("error opening store collection: %v", err))
	}
}

func (k *Kvs) DumpMetrics() {
}

func (k *Kvs) Stop() {
	// close and save
	k.invalidateBatch()
	k.invalidateSnapshot()

	// wait for all writes to happen
	for {
		stats, er := k.collection.Stats()
		if er == nil && stats.CurDirtyOps <= 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	fmt.Println("Stopping store")
	err := k.collection.Close()
	if err != nil {
		panic(err)
	}

	err = k.store.Close()
	if err != nil {
		panic(err)
	}
	// wait for the store to finish
	<-k.storeDone
	close(k.storeDone)
	fmt.Println("stopped.")

	k.Dump()
}

func (k *Kvs) Put(key, value []byte) error {
	now := time.Now()
	k.put.requests.Add(1)
	k.put.valueBytes.Add(int64(len(value)))
	k.put.keyBytes.Add(int64(len(key)))

	k.invalidateSnapshot()

	err := k.getCurrentBatch().Set(key, value)

	k.put.nanos.Add(time.Since(now).Nanoseconds())

	return err
}

func (k *Kvs) Delete(key []byte) error {
	now := time.Now()
	k.del.requests.Add(1)
	k.del.keyBytes.Add(int64(len(key)))

	k.invalidateSnapshot()
	err := k.getCurrentBatch().Del(key)

	k.del.nanos.Add(time.Since(now).Nanoseconds())

	return err
}

func (k *Kvs) Get(key []byte) ([]byte, error) {
	now := time.Now()
	k.get.requests.Add(1)
	k.put.keyBytes.Add(int64(len(key)))

	k.invalidateBatch()
	data, err := k.getCurrentSnapshot().Get(key, moss.ReadOptions{})
	k.get.nanos.Add(time.Since(now).Nanoseconds())
	return data, err
}

func (k *Kvs) getCurrentBatch() moss.Batch {
	var err error
	if k.currentBatch == nil {
		k.currentBatch, err = k.collection.NewBatch(0, 0)
		if err != nil {
			panic(err)
		}
	}
	return k.currentBatch
}

func (k *Kvs) invalidateBatch() {
	var err error
	if k.currentBatch != nil {
		err = k.collection.ExecuteBatch(k.currentBatch, moss.WriteOptions{})
		if err != nil {
			panic(err)
		}
		k.currentBatch.Close()
		k.currentBatch = nil
	}
}

func (k *Kvs) getCurrentSnapshot() moss.Snapshot {
	var err error
	if k.currentSnapshot == nil {
		k.currentSnapshot, err = k.collection.Snapshot()
		if err != nil {
			panic(err)
		}
	}
	return k.currentSnapshot
}

func (k *Kvs) invalidateSnapshot() {
	var err error
	if k.currentSnapshot != nil {
		err = k.currentSnapshot.Close()
		if err != nil {
			panic(err)
		}
		k.currentSnapshot = nil
	}
}

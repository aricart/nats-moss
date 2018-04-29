package kvserver

import (
	"github.com/couchbase/moss"
	"fmt"
	"time"
)

type Kvs struct {
	store           *moss.Store
	collection      moss.Collection
	storeDone       chan string
	currentBatch    moss.Batch
	currentSnapshot moss.Snapshot
	dataDir         string
}

func NewKvs(dataDir string) *Kvs {
	return &Kvs{dataDir: dataDir,}
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
}

func (k *Kvs) Put(key, value []byte) error {
	k.invalidateSnapshot()
	return k.getCurrentBatch().Set(key, value)
}

func (k *Kvs) Delete(key []byte) error {
	k.invalidateSnapshot()
	return k.getCurrentBatch().Del(key)
}

func (k *Kvs) Get(key []byte) ([]byte, error) {
	k.invalidateBatch()
	return k.getCurrentSnapshot().Get(key, moss.ReadOptions{})
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

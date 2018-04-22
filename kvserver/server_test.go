package kvserver

import (
	"fmt"
	"github.com/nats-io/nats"
	"io/ioutil"
	"testing"
	"time"
)

func get(t *testing.T, nc *nats.Conn, key string) string {
	msg, err := nc.Request(key, []byte(""), time.Second)
	if err != nil {
		t.Fatalf("failed to request value from kvserver: %v", err)
	}
	return string(msg.Data)
}

func put(t *testing.T, nc *nats.Conn, key string, value string) {
	err := nc.Publish(key, []byte(value))
	if err != nil {
		t.Fatalf("failed to put value from kvserver: %v", err)
	}
}

func clear(t *testing.T, nc *nats.Conn, key string) {
	err := nc.Publish(key, []byte(""))
	if err != nil {
		t.Fatalf("failed to clear value from kvserver: %v", err)
	}
}

func TestStartStop(t *testing.T) {
	kvs := DefaultKvOpts()
	kvs.Start()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	msg, err := nc.Request("a", []byte(""), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to kvserver: %v", err)
	}
	if msg == nil {
		t.Fatal("Reponse was nil")
	}

	nc.Close()
	kvs.Stop()

	nc, err = nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err == nil {
		t.Fatalf("failed to stop: %v", err)
	}
}

func TestRWC(t *testing.T) {
	var err error

	kvs := DefaultKvOpts()
	kvs.DataDir, err = ioutil.TempDir("/tmp", "kvs")
	if err != nil {
		t.Fatal(err)
	}
	kvs.Start()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	put(t, nc, "a", "aaa")
	v := get(t, nc, "a")

	if v != "aaa" {
		t.Fatalf("unexpected value from kvserver: %v", err)
	}

	clear(t, nc, "a")
	v = get(t, nc, "a")
	if v != "" {
		t.Fatalf("unexpected value from kvserver: %v", err)
	}

	nc.Close()
	kvs.Stop()
}

//
//func TestValuesPresistRestart(t *testing.T) {
//	var err error
//
//	kvs := DefaultKvOpts()
//	kvs.DataDir, err = ioutil.TempDir("/tmp","kvs")
//	if err != nil {
//		t.Fatal(err)
//	}
//	kvs.Start()
//
//	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
//	if err != nil {
//		t.Fatalf("failed to connect: %v", err)
//	}
//
//	put(t, nc, "a", "aaa")
//	put(t, nc, "b", "bbb")
//	put(t, nc, "ab", "abab")
//
//	kvs.Stop()
//	nc.Close()
//
//	time.Sleep(2000)
//
//	kvs.Start()
//
//	nc, err = nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
//	if err != nil {
//		t.Fatalf("failed to connect: %v", err)
//	}
//
//	v := get(t, nc, "a")
//	if "aaa" != v {
//		t.Fatalf("failed to read expected value for a: %s", v)
//	}
//
//	v = get(t, nc, "b")
//	if "bbb" != v {
//		t.Fatalf("failed to read expected value for b: %s", v)
//	}
//
//	v = get(t, nc, "ab")
//	if "abab" != v {
//		t.Fatalf("failed to read expected value for ab: %s", v)
//	}
//
//	nc.Close()
//	kvs.Stop()
//}

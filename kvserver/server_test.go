package kvserver

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"io/ioutil"
	"sync"
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
	time.Sleep(time.Second * 1)

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

func TestValuesPresistRestart(t *testing.T) {
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
	put(t, nc, "b", "bbb")
	put(t, nc, "ab", "abab")

	v := get(t, nc, "a")
	if v != "aaa" {
		t.Fatalf("returned key %s doesn't match expected 'aaa'", v)
	}

	v = get(t, nc, "b")
	if v != "bbb" {
		t.Fatalf("returned key %s doesn't match expected 'bbb'", v)
	}

	v = get(t, nc, "ab")
	if v != "abab" {
		t.Fatalf("returned key %s doesn't match expected 'abab'", v)
	}

	kvs.Stop()
	nc.Close()

	kvs = kvs.GetKvOpts()
	kvs.Start()

	nc, err = nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	v = get(t, nc, "a")
	if "aaa" != v {
		t.Fatalf("failed to read expected value for a: %s", v)
	}

	v = get(t, nc, "b")
	if "bbb" != v {
		t.Fatalf("failed to read expected value for b: %s", v)
	}

	v = get(t, nc, "ab")
	if "abab" != v {
		t.Fatalf("failed to read expected value for ab: %s", v)
	}

	nc.Close()
	kvs.Stop()
}

func TestDoubleUpdate(t *testing.T) {
	var err error

	kvs := DefaultKvOpts()
	kvs.Embed = true
	kvs.Prefix = ""
	kvs.DataDir, err = ioutil.TempDir("/tmp", "clobber")
	if err != nil {
		t.Fatal(err)
	}
	kvs.Start()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	put(t, nc, "a", "a")
	put(t, nc, "a", "b")
	v := get(t, nc, "a")
	if v != "b" {
		t.Fatal("value didn't match")
	}
	nc.Close()
	kvs.Stop()
}

func TestPerf(t *testing.T) {
	var err error

	kvs := DefaultKvOpts()
	kvs.Embed = true
	kvs.Prefix = ""
	kvs.DataDir, err = ioutil.TempDir("/tmp", "perf")
	if err != nil {
		t.Fatal(err)
	}
	kvs.Start()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	count := 1000000
	buf := make([]string, count)
	start := time.Now()
	for i := 0; i < count; i++ {
		v := nats.NewInbox()[7:]
		put(t, nc, v, v)
		buf[i] = v
		if i%100 == 0 {
			nc.Flush()
		}
	}
	nc.Flush()

	c := make(chan string)
	// wait until we can get back the last item
	go func() {
		for {
			tt := get(t, nc, buf[count-1])
			if tt == buf[count-1] {
				c <- "done"
				break
			} else {
				fmt.Println("sleeping")
				time.Sleep(time.Millisecond * 250)
			}
		}
	}()

	<-c

	maxClients := 5
	wg := sync.WaitGroup{}

	work := count / maxClients
	fmt.Printf("Work per client: %d\n", work)

	for j := 0; j < maxClients; j++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
			if err != nil {
				panic(err)
			}
			start := work * id
			end := start + work

			fmt.Printf("client: %d start: %d end: %d\n", id, start, end)

			for i := start; i < end; i++ {
				vv := get(t, cc, buf[i])
				if vv != buf[i] {
					fmt.Printf("%s = %s\n", buf[i], vv)
					fmt.Printf("client %d didn't get correct value for %d\n", id, i)
					t.Fatal("client failed to get correct value")
					cc.Close()
					return
				}
			}

			cc.Close()
		}(j)
	}

	wg.Wait()

	kvs.Stop()
	nc.Close()

	length := time.Since(start)
	fmt.Println(length.String())
}

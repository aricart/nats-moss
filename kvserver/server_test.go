package kvserver

import (
	"fmt"
	"github.com/nats-io/go-nats"
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

func natsURL(kvs *KvServer) string {
	return fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port)
}

func TestStartStop(t *testing.T) {
	kvs := NewKvServer(nil)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
	if err != nil {
		t.Fatalf("failed to connect to: %v", err)
	}
	msg, err := nc.Request("a", []byte(""), time.Second*60)
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}
	if msg == nil {
		t.Fatal("Reponse was nil")
	}

	nc.Close()
	kvs.Stop()
	time.Sleep(time.Second * 1)

	nc, err = nats.Connect(natsURL(kvs))
	if err == nil {
		t.Fatalf("failed to stop: %v", err)
	}
}

func TestRWC(t *testing.T) {
	var err error

	kvs := NewKvServer(nil)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
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

	kvs := NewKvServer(nil)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
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

	kvs = NewKvServer(kvs.GetOptions())
	kvs.Start()

	nc, err = nats.Connect(natsURL(kvs))
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

	kvs := NewKvServer(nil)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
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
	kvs := NewKvServer(nil)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
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
	fmt.Printf("Adding %d keys %s\n", count, time.Since(start).String())

	start = time.Now()
	get(t, nc, "a")
	fmt.Printf("Time to store %d keys %s\n", count, time.Since(start).String())
	nc.Flush()
	start = time.Now()

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

	maxClients := 10
	wg := sync.WaitGroup{}

	work := count / maxClients
	fmt.Printf("Work per client: %d\n", work)

	m := Metric{}
	m.init("Client")

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
				start := time.Now()
				m.requests.Add(1)
				vv := get(t, cc, buf[i])
				m.nanos.Add(time.Since(start).Nanoseconds())
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
	fmt.Println("Client")
	fmt.Println(m.Dump())
	fmt.Printf("%d / %s [%.1f req/sec]\n", m.requests.Value(), length.String(), float64(m.requests.Value())/length.Seconds())


}

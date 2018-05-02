package kvserver

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"sync"
	"testing"
	"time"
)

func getAsync(t *testing.T, nc *nats.Conn, key string, prefix string, inbox string) {
	if prefix != "" {
		key = fmt.Sprintf("%s.%s", prefix, key)
	}
	err := nc.PublishRequest(key, inbox, []byte{})
	if err != nil {
		t.Fatalf("failed to request value from kvserver: %v", err)
	}
}

func get(t *testing.T, nc *nats.Conn, key string, prefix string) string {
	if prefix != "" {
		key = fmt.Sprintf("%s.%s", prefix, key)
	}
	msg, err := nc.Request(key, []byte{}, time.Second*10)
	if err != nil {
		t.Fatalf("failed to request value from kvserver: %v", err)
	}
	return string(msg.Data)
}

func put(t *testing.T, nc *nats.Conn, key string, value string, prefix string) {
	if prefix != "" {
		key = fmt.Sprintf("%s.%s", prefix, key)
	}
	err := nc.Publish(key, []byte(value))
	if err != nil {
		t.Fatalf("failed to put value from kvserver: %v", err)
	}
}

func clear(t *testing.T, nc *nats.Conn, key string, prefix string) {
	if prefix != "" {
		key = fmt.Sprintf("%s.%s", prefix, key)
	}
	err := nc.Publish(key, []byte{})
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

	put(t, nc, "a", "aaa", kvs.Prefix)
	v := get(t, nc, "a", kvs.Prefix)

	if v != "aaa" {
		t.Fatalf("unexpected value from kvserver: %v", err)
	}

	clear(t, nc, "a", kvs.Prefix)
	v = get(t, nc, "a", kvs.Prefix)
	if v != "" {
		t.Fatalf("unexpected value from kvserver: %v", err)
	}

	nc.Close()
	kvs.Stop()
}

func TestRWCWithPrefixes(t *testing.T) {
	var err error

	opts := DefaultKvServerOptions()
	opts.Prefix = "nks"
	kvs := NewKvServer(opts)
	kvs.Start()

	nc, err := nats.Connect(natsURL(kvs))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	put(t, nc, "a", "aaa", kvs.Prefix)
	v := get(t, nc, "a", kvs.Prefix)

	if v != "aaa" {
		t.Fatalf("unexpected value from kvserver: %v", err)
	}

	clear(t, nc, "a", kvs.Prefix)
	v = get(t, nc, "a", kvs.Prefix)
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

	put(t, nc, "a", "aaa", kvs.Prefix)
	put(t, nc, "b", "bbb", kvs.Prefix)
	put(t, nc, "ab", "abab", kvs.Prefix)

	v := get(t, nc, "a", kvs.Prefix)
	if v != "aaa" {
		t.Fatalf("returned key %s doesn't match expected 'aaa'", v)
	}

	v = get(t, nc, "b", kvs.Prefix)
	if v != "bbb" {
		t.Fatalf("returned key %s doesn't match expected 'bbb'", v)
	}

	v = get(t, nc, "ab", kvs.Prefix)
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

	v = get(t, nc, "a", kvs.Prefix)
	if "aaa" != v {
		t.Fatalf("failed to read expected value for a: %s", v)
	}

	v = get(t, nc, "b", kvs.Prefix)
	if "bbb" != v {
		t.Fatalf("failed to read expected value for b: %s", v)
	}

	v = get(t, nc, "ab", kvs.Prefix)
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

	put(t, nc, "a", "a", kvs.Prefix)
	put(t, nc, "a", "b", kvs.Prefix)
	v := get(t, nc, "a", kvs.Prefix)
	if v != "b" {
		t.Fatal("value didn't match")
	}
	nc.Close()
	kvs.Stop()
}

func TestPerf(t *testing.T) {
	var err error
	opts := DefaultKvServerOptions()
	opts.Prefix = "keystore"

	kvs := NewKvServer(opts)
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
		put(t, nc, v, v, kvs.Prefix)
		buf[i] = v
		//if i%10000 == 0 {
		//	nc.Flush()
		//}
	}
	nc.Flush()
	fmt.Printf("Adding %d keys %s\n", count, time.Since(start).String())

	start = time.Now()
	get(t, nc, "a", kvs.Prefix)
	fmt.Printf("Time to store %d keys %s\n", count, time.Since(start).String())
	nc.Flush()
	start = time.Now()

	c := make(chan string)
	// wait until we can get back the last item
	go func() {
		for {
			tt := get(t, nc, buf[count-1], kvs.Prefix)
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
				vv := get(t, cc, buf[i], kvs.Prefix)
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

func TestAsyncPerf(t *testing.T) {
	var err error
	opts := DefaultKvServerOptions()
	opts.Prefix = "keystore"

	kvs := NewKvServer(opts)
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
		put(t, nc, v, v, kvs.Prefix)
		buf[i] = v
		if i%1000 == 0 {
			nc.Flush()
		}
	}
	nc.Flush()
	fmt.Printf("Adding %d keys %s\n", count, time.Since(start).String())

	start = time.Now()
	get(t, nc, "a", kvs.Prefix)
	fmt.Printf("Time to store %d keys %s\n", count, time.Since(start).String())
	nc.Flush()
	start = time.Now()

	c := make(chan string)
	// wait until we can get back the last item
	go func() {
		for {
			tt := get(t, nc, buf[count-1], kvs.Prefix)
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
			responses := 0
			done := make(chan string)
			cc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", kvs.Host, kvs.Port))
			if err != nil {
				panic(err)
			}
			now := time.Now()
			inbox := nats.NewInbox()
			cc.Subscribe(inbox, func(msg *nats.Msg) {
				m.requests.Add(1)
				responses++
				if responses >= work {
					done <- "done"
				}
			})

			start := work * id
			end := start + work
			fmt.Printf("client: %d start: %d end: %d\n", id, start, end)

			for i := start; i < end; i++ {
				getAsync(t, cc, buf[i], kvs.Prefix, inbox)

				if i%1000 == 0 {
					cc.Flush()
				}
			}

			<-done
			m.nanos.Add(time.Since(now).Nanoseconds())
			cc.Close()
			wg.Done()
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

package kvserver

import (
	"expvar"
	"time"
	"fmt"
)

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

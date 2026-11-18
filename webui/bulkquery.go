package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Bulk query: parallel clients run point lookups by primary key on random ids for a set time,
// each through a prepared statement, spread over the nodes. One job runs at a time; the browser
// polls its progress.

type queryLoadRequest struct {
	Table   string `json:"table"`   // db.table
	StartID int64  `json:"startId"` // ids are drawn from startId .. startId+ids-1
	IDs     int64  `json:"ids"`
	Clients int    `json:"clients"`
	Seconds int    `json:"seconds"`
}

type queryLoadStatus struct {
	State     string  `json:"state"` // idle, running, done, cancelled or failed
	Table     string  `json:"table,omitempty"`
	SQL       string  `json:"sql,omitempty"`
	Clients   int     `json:"clients"`
	Seconds   int     `json:"seconds"`
	ElapsedMs float64 `json:"elapsedMs"`
	Queries   int64   `json:"queries"`
	Found     int64   `json:"found"`  // lookups that returned a row
	Errors    int64   `json:"errors"` // failed lookups (each one reconnects to the next node)
	P50Ms     float64 `json:"p50Ms"`
	P99Ms     float64 `json:"p99Ms"`
	Error     string  `json:"error,omitempty"`

	Nodes []queryLoadNode `json:"nodes"` // what each node answered
}

type queryLoadNode struct {
	Addr    string  `json:"addr"`
	Queries int64   `json:"queries"`
	Errors  int64   `json:"errors"`
	P50Ms   float64 `json:"p50Ms"`
	P99Ms   float64 `json:"p99Ms"`
}

// Latency histogram: buckets of 20 µs up to 200 ms; slower queries land in the last bucket.
const (
	latencyBucketMicros = 20
	latencyBuckets      = 10_000
)

type histogram [latencyBuckets + 1]atomic.Int64

func (h *histogram) add(d time.Duration) {
	h[min(int(d.Microseconds())/latencyBucketMicros, latencyBuckets)].Add(1)
}

// percentile is the latency in ms below which the fraction q of the queries in hs finished.
func percentile(q float64, hs ...*histogram) float64 {
	var counts [latencyBuckets + 1]int64
	var total int64
	for _, h := range hs {
		for i := range h {
			n := h[i].Load()
			counts[i] += n
			total += n
		}
	}
	if total == 0 {
		return 0
	}
	target, seen := int64(float64(total)*q), int64(0)
	for i, n := range counts {
		if seen += n; seen > target {
			return float64((i+1)*latencyBucketMicros) / 1000
		}
	}
	return float64(latencyBuckets*latencyBucketMicros) / 1000
}

// nodeLoad is what one node answered during a job.
type nodeLoad struct {
	queries atomic.Int64
	errors  atomic.Int64
	latency histogram
}

type queryLoadJob struct {
	req     queryLoadRequest
	sql     string
	start   time.Time
	cancel  context.CancelFunc
	queries atomic.Int64
	found   atomic.Int64
	errors  atomic.Int64
	nodes   []string
	perNode []nodeLoad

	mu    sync.Mutex
	state string
	err   string
	end   time.Time
}

func (j *queryLoadJob) status() queryLoadStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	end := j.end
	if j.state == "running" {
		end = time.Now()
	}
	nodes := make([]queryLoadNode, len(j.nodes))
	all := make([]*histogram, len(j.nodes))
	for i, addr := range j.nodes {
		n := &j.perNode[i]
		all[i] = &n.latency
		nodes[i] = queryLoadNode{
			Addr: addr, Queries: n.queries.Load(), Errors: n.errors.Load(),
			P50Ms: percentile(0.5, &n.latency), P99Ms: percentile(0.99, &n.latency),
		}
	}
	return queryLoadStatus{
		State: j.state, Table: j.req.Table, SQL: j.sql, Clients: j.req.Clients, Seconds: j.req.Seconds,
		ElapsedMs: float64(end.Sub(j.start).Microseconds()) / 1000,
		Queries:   j.queries.Load(), Found: j.found.Load(), Errors: j.errors.Load(),
		P50Ms: percentile(0.5, all...), P99Ms: percentile(0.99, all...), Error: j.err, Nodes: nodes,
	}
}

type queryLoad struct {
	c   *cluster
	mu  sync.Mutex
	job *queryLoadJob
}

func (l *queryLoad) status() queryLoadStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.job == nil {
		return queryLoadStatus{State: "idle"}
	}
	return l.job.status()
}

func (l *queryLoad) cancel() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.job != nil {
		l.job.cancel()
	}
}

func (l *queryLoad) start(ctx context.Context, req queryLoadRequest) error {
	if req.Clients == 0 {
		req.Clients = 32
	}
	if req.Seconds == 0 {
		req.Seconds = 10
	}
	switch {
	case req.IDs < 1 || req.IDs > 1_000_000_000_000:
		return errors.New("the number of ids must be between 1 and 1,000,000,000,000")
	case req.StartID < 0 || req.StartID > 1<<62:
		return errors.New("first id is out of range")
	case req.Clients < 1 || req.Clients > 1024:
		return errors.New("clients must be between 1 and 1,024")
	case req.Seconds < 1 || req.Seconds > 3600:
		return errors.New("duration must be between 1 and 3,600 seconds")
	}
	tables, _, err := l.c.tables(ctx)
	if err != nil {
		return err
	}
	var t *table
	for i := range tables {
		if tables[i].DB+"."+tables[i].Name == req.Table {
			t = &tables[i]
		}
	}
	if t == nil {
		return fmt.Errorf("unknown table %s", req.Table)
	}
	schema, key, err := bulkSchema(t, req.StartID+req.IDs-1) // the same table shape bulk insert needs
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.job != nil && l.job.status().State == "running" {
		return errors.New("a bulk query is already running")
	}
	jobCtx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Seconds)*time.Second)
	job := &queryLoadJob{
		req: req, start: time.Now(), cancel: cancel, state: "running",
		nodes: l.c.nodes, perNode: make([]nodeLoad, len(l.c.nodes)),
		sql: fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "%s" = $1`, t.DB, t.Name, schema.Field(key).Name),
	}
	l.job = job
	go job.run(jobCtx, l.c)
	return nil
}

func (j *queryLoadJob) run(ctx context.Context, c *cluster) {
	defer j.cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var failure error
	for w := 0; w < j.req.Clients; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := j.client(ctx, c, w); err != nil {
				once.Do(func() { failure = err })
				j.cancel() // stop the other clients
			}
		}()
	}
	wg.Wait()

	j.mu.Lock()
	defer j.mu.Unlock()
	j.end = time.Now()
	switch {
	case failure != nil:
		j.state, j.err = "failed", status.Convert(failure).Message()
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		j.state = "done" // ran for the whole duration
	default:
		j.state = "cancelled"
	}
}

// client is one client: it prepares the lookup on a node and runs it on random ids until the job
// ends. Clients are spread over the nodes: client w starts on node w. After an error it prepares again on the next node. Twenty errors in a row end the job.
func (j *queryLoadJob) client(ctx context.Context, c *cluster, w int) error {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(w)))
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", c.auth)
	// Closing a statement must still work after the job's context has ended.
	closeCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", c.auth)
	params := arrow.NewSchema([]arrow.Field{{Name: "$1", Type: arrow.PrimitiveTypes.Int64}}, nil)
	ids := array.NewInt64Builder(memory.DefaultAllocator)
	defer ids.Release()

	var cl *flightsql.Client
	var prep *flightsql.PreparedStatement
	closePrep := func() {
		if prep != nil {
			closing, cancel := context.WithTimeout(closeCtx, 2*time.Second)
			_ = prep.Close(closing)
			cancel()
			prep = nil
		}
	}
	defer closePrep()

	node, failures := w, 0
	for ctx.Err() == nil {
		begin := time.Now()
		rows, err := func() (int64, error) {
			if prep == nil {
				var err error
				if cl, err = c.client(c.nodes[node%len(c.nodes)]); err != nil {
					return 0, err
				}
				if prep, err = cl.Prepare(callCtx, j.sql); err != nil {
					return 0, err
				}
			}
			ids.Append(j.req.StartID + rng.Int64N(j.req.IDs))
			col := ids.NewArray()
			rec := array.NewRecord(params, []arrow.Array{col}, 1)
			col.Release()
			prep.SetParameters(rec)
			rec.Release()
			info, err := prep.Execute(callCtx)
			if err != nil {
				return 0, err
			}
			var rows int64
			err = fetch(callCtx, cl, info, func(r arrow.Record) bool {
				rows += r.NumRows()
				return true
			})
			return rows, err
		}()
		// The job ended while this lookup was in flight. (A call can report its deadline a moment
		// before the context does.)
		if deadline, ok := ctx.Deadline(); ctx.Err() != nil || (ok && !time.Now().Before(deadline)) {
			break
		}
		on := &j.perNode[node%len(c.nodes)]
		if err != nil {
			j.errors.Add(1)
			on.errors.Add(1)
			if failures++; failures >= 20 {
				return err
			}
			closePrep()
			node++
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		failures = 0
		j.queries.Add(1)
		if rows > 0 {
			j.found.Add(1)
		}
		on.queries.Add(1)
		on.latency.add(time.Since(begin))
	}
	return nil
}

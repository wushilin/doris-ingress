package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Bulk insert: fills a table with generated rows through Flight SQL bulk ingest (the call ADBC's
// adbc_ingest makes). The key column gets consecutive ids, every other column random values.
// One job runs at a time; the browser polls its progress.

type bulkRequest struct {
	Table   string `json:"table"` // db.table
	Rows    int64  `json:"rows"`
	StartID int64  `json:"startId"`
	Batch   int64  `json:"batch"`   // rows per ingest call
	Writers int    `json:"writers"` // parallel ingest calls, spread over the nodes
	// Rows per second over all writers. Left out: 50,000. Zero: no limit.
	Rate *int64 `json:"rate"`
}

const defaultBulkRate = 50_000

// pacer spaces batches out so that the writers together stay at a rows-per-second limit.
type pacer struct {
	rate float64 // rows per second; 0 is no limit
	mu   sync.Mutex
	next time.Time // when the next batch may start
}

// wait blocks until a batch of n rows may be sent, or ctx ends.
func (p *pacer) wait(ctx context.Context, n int64) {
	if p.rate <= 0 {
		return
	}
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now // unused time is not saved up
	}
	at := p.next
	p.next = p.next.Add(time.Duration(float64(n) / p.rate * float64(time.Second)))
	p.mu.Unlock()
	if d := time.Until(at); d > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
}

type bulkStatus struct {
	State     string  `json:"state"` // idle, running, done, cancelled or failed
	Table     string  `json:"table,omitempty"`
	Total     int64   `json:"total"`
	Done      int64   `json:"done"`
	StartID   int64   `json:"startId"`
	Batch     int64   `json:"batch"`
	Writers   int     `json:"writers"`
	Rate      int64   `json:"rate"` // rows per second limit; 0 is no limit
	ElapsedMs float64 `json:"elapsedMs"`
	Error     string  `json:"error,omitempty"`
	// Times a writer had to wait because the buffer was full (the cluster was loading to Doris).
	Waits int64 `json:"waits"`

	Nodes []bulkNode `json:"nodes"` // what each node took
}

type bulkNode struct {
	Addr string `json:"addr"`
	Rows int64  `json:"rows"`
}

type bulkJob struct {
	req      bulkRequest
	start    time.Time
	done     atomic.Int64
	waits    atomic.Int64
	nodes    []string
	nodeRows []atomic.Int64 // rows ingested through each node
	pace     pacer
	cancel   context.CancelFunc

	mu    sync.Mutex
	state string
	err   string
	end   time.Time
}

func (j *bulkJob) status() bulkStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	end := j.end
	if j.state == "running" {
		end = time.Now()
	}
	nodes := make([]bulkNode, len(j.nodes))
	for i, addr := range j.nodes {
		nodes[i] = bulkNode{Addr: addr, Rows: j.nodeRows[i].Load()}
	}
	return bulkStatus{
		Nodes: nodes,
		Waits: j.waits.Load(),
		Rate:  *j.req.Rate,
		State: j.state, Table: j.req.Table, Total: j.req.Rows, Done: j.done.Load(), StartID: j.req.StartID,
		Batch: j.req.Batch, Writers: j.req.Writers, ElapsedMs: float64(end.Sub(j.start).Microseconds()) / 1000, Error: j.err,
	}
}

type bulk struct {
	c   *cluster
	mu  sync.Mutex
	job *bulkJob
}

func (b *bulk) status() bulkStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.job == nil {
		return bulkStatus{State: "idle"}
	}
	return b.job.status()
}

func (b *bulk) cancel() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.job != nil {
		b.job.cancel()
	}
}

// start validates the request against the table and starts the job in the background.
func (b *bulk) start(ctx context.Context, req bulkRequest) error {
	if req.Batch == 0 {
		req.Batch = 10_000
	}
	if req.Writers == 0 {
		req.Writers = 8
	}
	if req.Rate == nil {
		rate := int64(defaultBulkRate)
		req.Rate = &rate
	}
	switch {
	case req.Rows < 1 || req.Rows > 1_000_000_000:
		return errors.New("rows must be between 1 and 1,000,000,000")
	case req.Batch < 1 || req.Batch > 100_000:
		return errors.New("batch must be between 1 and 100,000 rows")
	case req.Writers < 1 || req.Writers > 64:
		return errors.New("writers must be between 1 and 64")
	case *req.Rate < 0 || *req.Rate > 1_000_000_000:
		return errors.New("rows per second must be between 0 (no limit) and 1,000,000,000")
	case req.StartID < 0 || req.StartID > math.MaxInt64-req.Rows:
		return errors.New("start id is out of range")
	}
	tables, _, err := b.c.tables(ctx)
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
	schema, key, err := bulkSchema(t, req.StartID+req.Rows-1)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.job != nil && b.job.status().State == "running" {
		return errors.New("a bulk insert is already running")
	}
	jobCtx, cancel := context.WithCancel(context.Background())
	job := &bulkJob{
		req: req, start: time.Now(), cancel: cancel, state: "running",
		nodes: b.c.nodes, nodeRows: make([]atomic.Int64, len(b.c.nodes)),
	}
	job.pace.rate = float64(*req.Rate)
	b.job = job
	go job.run(jobCtx, b.c, t, schema, key)
	return nil
}

// bulkSchema is the table's own columns (without the hidden ones) and the index of its key
// column. The table needs one integer key column that can hold maxID, and every column that
// cannot be null needs a type this generator knows.
func bulkSchema(t *table, maxID int64) (*arrow.Schema, int, error) {
	var fields []arrow.Field
	key, keys := -1, 0
	for i, col := range t.Columns {
		if col.Name == "__seq" || col.Name == "__deleted" {
			continue
		}
		f := t.schema.Field(i)
		if col.Key {
			keys++
			key = len(fields)
		} else if !f.Nullable && !canGenerate(f.Type) {
			return nil, 0, fmt.Errorf("column %s: no generator for type %s, and it cannot be null", f.Name, f.Type)
		}
		fields = append(fields, f)
	}
	if keys != 1 {
		return nil, 0, fmt.Errorf("%s.%s has %d key columns; bulk insert needs exactly one", t.DB, t.Name, keys)
	}
	var limit int64
	switch fields[key].Type.ID() {
	case arrow.INT64, arrow.UINT64:
		limit = math.MaxInt64
	case arrow.INT32:
		limit = math.MaxInt32
	case arrow.UINT32:
		limit = math.MaxUint32
	default:
		return nil, 0, fmt.Errorf("key column %s is %s; bulk insert needs a 32- or 64-bit integer key", fields[key].Name, fields[key].Type)
	}
	if maxID > limit {
		return nil, 0, fmt.Errorf("id %d does not fit key column %s (%s)", maxID, fields[key].Name, fields[key].Type)
	}
	return arrow.NewSchema(fields, nil), key, nil
}

func canGenerate(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING, arrow.BOOL, arrow.TIMESTAMP, arrow.DATE32,
		arrow.DATE64, arrow.DECIMAL128:
		return true
	}
	return false
}

func (j *bulkJob) run(ctx context.Context, c *cluster, t *table, schema *arrow.Schema, key int) {
	defer j.cancel()
	var next atomic.Int64 // rows handed out to writers so far
	var wg sync.WaitGroup
	var once sync.Once
	var failure error
	for w := 0; w < j.req.Writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := j.write(ctx, c, t, schema, key, w, &next); err != nil && ctx.Err() == nil {
				once.Do(func() { failure = err })
				j.cancel() // stop the other writers
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
	case j.done.Load() < j.req.Rows:
		j.state = "cancelled"
	default:
		j.state = "done"
	}
}

// write is one writer: it takes the next batch of ids, generates the rows and ingests them, until
// none are left. Writers are spread over the nodes: writer w starts on node w. An unreachable
// node is retried on the next node. When the buffer is full, the writer waits and tries again, so
// the job slows down to the speed at which the cluster loads into Doris; it fails only after the
// buffer has stayed full for two minutes.
func (j *bulkJob) write(ctx context.Context, c *cluster, t *table, schema *arrow.Schema, key, w int, next *atomic.Int64) error {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(w)))
	opts := &flightsql.ExecuteIngestOpts{
		TableDefinitionOptions: &pb.CommandStatementIngest_TableDefinitionOptions{},
		Table:                  t.Name,
		Schema:                 &t.DB,
	}
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", c.auth)
	node := w
	for ctx.Err() == nil {
		offset := next.Add(j.req.Batch) - j.req.Batch
		if offset >= j.req.Rows {
			return nil
		}
		n := min(j.req.Batch, j.req.Rows-offset)
		j.pace.wait(ctx, n)
		if ctx.Err() != nil {
			return nil
		}
		rec := generate(schema, key, j.req.StartID+offset, n, rng)
		var fullSince time.Time
		for attempt := 0; ; {
			err := ingest(callCtx, c, c.nodes[node%len(c.nodes)], schema, rec, opts)
			if err == nil {
				break
			}
			code := status.Code(err)
			full := code == codes.ResourceExhausted
			if full && fullSince.IsZero() {
				fullSince = time.Now()
			}
			if !full {
				attempt++
			}
			retryable := full || code == codes.Unavailable || code == codes.DeadlineExceeded
			if ctx.Err() != nil || !retryable || attempt >= 4*len(c.nodes) || (full && time.Since(fullSince) > 2*time.Minute) {
				rec.Release()
				return err
			}
			pause := 200 * time.Millisecond
			if full {
				j.waits.Add(1)
				pause = 500 * time.Millisecond
			} else {
				node++
			}
			select {
			case <-ctx.Done():
			case <-time.After(pause):
			}
		}
		rec.Release()
		j.done.Add(n)
		j.nodeRows[node%len(c.nodes)].Add(n)
	}
	return nil
}

func ingest(ctx context.Context, c *cluster, addr string, schema *arrow.Schema, rec arrow.Record, opts *flightsql.ExecuteIngestOpts) error {
	cl, err := c.client(addr)
	if err != nil {
		return err
	}
	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return err
	}
	defer rdr.Release()
	_, err = cl.ExecuteIngest(ctx, rdr, opts)
	return err
}

var regions = []string{"sg", "us", "eu", "jp", "in", "au", "br", "de", "uk", "ca"}

// generate builds n rows: ids from, from+1, ... in the key column and random values elsewhere.
func generate(schema *arrow.Schema, key int, from, n int64, rng *rand.Rand) arrow.Record {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Reserve(int(n))
	now := time.Now()
	var buf []byte
	for i, f := range schema.Fields() {
		if i == key {
			for r := int64(0); r < n; r++ {
				switch kb := b.Field(i).(type) {
				case *array.Int64Builder:
					kb.Append(from + r)
				case *array.Uint64Builder:
					kb.Append(uint64(from + r))
				case *array.Int32Builder:
					kb.Append(int32(from + r))
				case *array.Uint32Builder:
					kb.Append(uint32(from + r))
				}
			}
			continue
		}
		switch fb := b.Field(i).(type) {
		case *array.Int8Builder:
			for range n {
				fb.Append(int8(rng.IntN(100)))
			}
		case *array.Int16Builder:
			for range n {
				fb.Append(int16(rng.IntN(10_000)))
			}
		case *array.Int32Builder:
			for range n {
				fb.Append(rng.Int32N(1_000_000))
			}
		case *array.Int64Builder:
			for range n {
				fb.Append(rng.Int64N(1_000_000_000))
			}
		case *array.Uint8Builder:
			for range n {
				fb.Append(uint8(rng.IntN(100)))
			}
		case *array.Uint16Builder:
			for range n {
				fb.Append(uint16(rng.IntN(10_000)))
			}
		case *array.Uint32Builder:
			for range n {
				fb.Append(rng.Uint32N(1_000_000))
			}
		case *array.Uint64Builder:
			for range n {
				fb.Append(rng.Uint64N(1_000_000_000))
			}
		case *array.Float32Builder:
			for range n {
				fb.Append(float32(rng.IntN(1_000_000)) / 100)
			}
		case *array.Float64Builder:
			for range n {
				fb.Append(float64(rng.IntN(1_000_000)) / 100)
			}
		case *array.BooleanBuilder:
			for range n {
				fb.Append(rng.IntN(2) == 1)
			}
		case *array.StringBuilder:
			for range n {
				buf = randomText(buf[:0], rng)
				fb.BinaryBuilder.Append(buf)
			}
		case *array.LargeStringBuilder:
			for range n {
				buf = randomText(buf[:0], rng)
				fb.BinaryBuilder.Append(buf)
			}
		case *array.TimestampBuilder:
			// A moment in the last 365 days, in the column's unit.
			unit := f.Type.(*arrow.TimestampType).Unit
			for range n {
				at := now.Add(-time.Duration(rng.Int64N(int64(365 * 24 * time.Hour))))
				ts, _ := arrow.TimestampFromTime(at, unit)
				fb.Append(ts)
			}
		case *array.Date32Builder:
			today := now.Unix() / 86_400
			for range n {
				fb.Append(arrow.Date32(today - rng.Int64N(365)))
			}
		case *array.Date64Builder:
			today := now.Unix() / 86_400
			for range n {
				fb.Append(arrow.Date64((today - rng.Int64N(365)) * 86_400_000))
			}
		case *array.Decimal128Builder:
			digits := min(f.Type.(*arrow.Decimal128Type).Precision, 9)
			limit := int64(math.Pow10(int(digits)))
			for range n {
				fb.Append(decimal128.FromI64(rng.Int64N(limit)))
			}
		default: // no generator: the column is nullable (checked before the job started)
			fb.AppendNulls(int(n))
		}
	}
	return b.NewRecord()
}

// randomText appends something like "sg-482913" to buf.
func randomText(buf []byte, rng *rand.Rand) []byte {
	buf = append(buf, regions[rng.IntN(len(regions))]...)
	buf = append(buf, '-')
	return strconv.AppendInt(buf, rng.Int64N(1_000_000), 10)
}

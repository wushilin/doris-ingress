// A small web SQL console for doris-ingress: browse tables, run queries, view results.
//
//	go run . -nodes 127.0.0.1:8815,127.0.0.1:8816 -user admin -password secret
//
// It talks Arrow Flight SQL to the nodes. Queries go to the nodes in turn, and move on to the
// next node when one is unreachable.
package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

//go:embed index.html
var indexHTML []byte

// cluster holds one lazily created Flight SQL client per node.
type cluster struct {
	nodes []string
	auth  string // "Basic ..." header, sent on every call

	mu      sync.Mutex
	clients map[string]*flightsql.Client
	turn    atomic.Uint64 // round-robin counter: which node gets the next call
}

func (c *cluster) client(addr string) (*flightsql.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[addr]; ok {
		return cl, nil
	}
	cl, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Reconnect quickly after a node restarts (the default backs off for up to two minutes).
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 200 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
			MinConnectTimeout: 2 * time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20)))
	if err != nil {
		return nil, err
	}
	c.clients[addr] = cl
	return cl, nil
}

// do runs fn against the next node in turn, and tries the other nodes while one is unreachable.
// Any other error (a bad query, for example) is returned as it is.
func (c *cluster) do(ctx context.Context, fn func(ctx context.Context, cl *flightsql.Client, addr string) error) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", c.auth)
	start := int(c.turn.Add(1) % uint64(len(c.nodes)))
	var err error
	for i := range c.nodes {
		addr := c.nodes[(start+i)%len(c.nodes)]
		var cl *flightsql.Client
		if cl, err = c.client(addr); err == nil {
			err = fn(ctx, cl, addr)
		}
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unavailable {
			return err
		}
	}
	return err
}

// fetch streams every record of a result to each, until each returns false.
func fetch(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo, each func(arrow.Record) bool) error {
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}
		for rdr.Next() {
			if !each(rdr.Record()) {
				rdr.Release()
				return nil
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

type column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Key      bool   `json:"key,omitempty"`
}

type table struct {
	Catalog string   `json:"catalog"`
	DB      string   `json:"db"`
	Name    string   `json:"name"`
	Columns []column `json:"columns"`

	schema *arrow.Schema // the Arrow fields behind Columns, in the same order
}

func (c *cluster) tables(ctx context.Context) ([]table, string, error) {
	var out []table
	var node string
	err := c.do(ctx, func(ctx context.Context, cl *flightsql.Client, addr string) error {
		out, node = nil, addr
		info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
		if err != nil {
			return err
		}
		// Result columns: catalog_name, db_schema_name, table_name, table_type, table_schema.
		err = fetch(ctx, cl, info, func(rec arrow.Record) bool {
			schemas, _ := rec.Column(4).(*array.Binary)
			for r := 0; r < int(rec.NumRows()); r++ {
				t := table{Catalog: rec.Column(0).ValueStr(r), DB: rec.Column(1).ValueStr(r), Name: rec.Column(2).ValueStr(r), Columns: []column{}}
				if schemas != nil {
					if s, err := flight.DeserializeSchema(schemas.Value(r), memory.DefaultAllocator); err == nil {
						t.schema = s
						for _, f := range s.Fields() {
							t.Columns = append(t.Columns, column{Name: f.Name, Type: f.Type.String(), Nullable: f.Nullable})
						}
					}
				}
				out = append(out, t)
			}
			return true
		})
		if err != nil {
			return err
		}
		for i := range out {
			markKeys(ctx, cl, &out[i])
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].DB != out[j].DB {
			return out[i].DB < out[j].DB
		}
		return out[i].Name < out[j].Name
	})
	return out, node, err
}

// markKeys flags the primary-key columns of t. The table is still listed when this fails.
func markKeys(ctx context.Context, cl *flightsql.Client, t *table) {
	info, err := cl.GetPrimaryKeys(ctx, flightsql.TableRef{DBSchema: &t.DB, Table: t.Name})
	if err != nil {
		return
	}
	keys := map[string]bool{}
	_ = fetch(ctx, cl, info, func(rec arrow.Record) bool {
		for r := 0; r < int(rec.NumRows()); r++ {
			keys[rec.Column(3).ValueStr(r)] = true // column_name
		}
		return true
	})
	for i := range t.Columns {
		t.Columns[i].Key = keys[t.Columns[i].Name]
	}
}

type queryResult struct {
	Columns   []column    `json:"columns"`
	Rows      [][]*string `json:"rows"` // nil is SQL NULL
	Truncated bool        `json:"truncated"`
	ElapsedMs float64     `json:"elapsedMs"`
	Node      string      `json:"node"`
}

func (c *cluster) query(ctx context.Context, sql string, maxRows int) (*queryResult, error) {
	var res *queryResult
	err := c.do(ctx, func(ctx context.Context, cl *flightsql.Client, addr string) (err error) {
		res, err = runQuery(ctx, cl, addr, sql, maxRows)
		return err
	})
	return res, err
}

// runQuery runs sql on one node and returns at most maxRows rows, as text.
func runQuery(ctx context.Context, cl *flightsql.Client, addr, sql string, maxRows int) (*queryResult, error) {
	start := time.Now()
	res := &queryResult{Node: addr, Columns: []column{}, Rows: [][]*string{}}
	info, err := cl.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	err = fetch(ctx, cl, info, func(rec arrow.Record) bool {
		if len(res.Columns) == 0 {
			for _, f := range rec.Schema().Fields() {
				res.Columns = append(res.Columns, column{Name: f.Name, Type: f.Type.String(), Nullable: f.Nullable})
			}
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			if len(res.Rows) >= maxRows {
				res.Truncated = true
				return false
			}
			row := make([]*string, rec.NumCols())
			for i := range row {
				// A column of Arrow type null (a bare NULL literal) has no validity bitmap.
				if col := rec.Column(i); col.DataType().ID() != arrow.NULL && !col.IsNull(r) {
					v := col.ValueStr(r)
					row[i] = &v
				}
			}
			res.Rows = append(res.Rows, row)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	res.ElapsedMs = float64(time.Since(start).Microseconds()) / 1000
	return res, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusBadRequest
	if status.Code(err) == codes.Unavailable {
		code = http.StatusBadGateway
	}
	writeJSON(w, code, map[string]string{"error": status.Convert(err).Message()})
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "address the web UI listens on")
	nodes := flag.String("nodes", "127.0.0.1:8815", "comma-separated Flight SQL addresses of the doris-ingress nodes")
	user := flag.String("user", "admin", "Flight SQL user")
	password := flag.String("password", os.Getenv("DORIS_INGRESS_ADMIN_PASSWORD"), "Flight SQL password (default: $DORIS_INGRESS_ADMIN_PASSWORD)")
	maxRows := flag.Int("max-rows", 5000, "most rows returned to the browser per query")
	timeout := flag.Duration("timeout", 60*time.Second, "per-query timeout")
	flag.Parse()

	c := &cluster{
		auth:    "Basic " + base64.StdEncoding.EncodeToString([]byte(*user+":"+*password)),
		clients: map[string]*flightsql.Client{},
	}
	for _, n := range strings.Split(*nodes, ",") {
		if n = strings.TrimSpace(n); n != "" {
			c.nodes = append(c.nodes, n)
		}
	}
	if len(c.nodes) == 0 {
		log.Fatal("no nodes given")
	}

	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	http.HandleFunc("GET /api/tables", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), *timeout)
		defer cancel()
		tables, node, err := c.tables(ctx)
		if err != nil {
			writeErr(w, err)
			return
		}
		if tables == nil {
			tables = []table{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tables": tables, "node": node})
	})
	http.HandleFunc("POST /api/query", func(w http.ResponseWriter, r *http.Request) {
		// Browsers cannot send JSON cross-site without a preflight, so this blocks other sites.
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		var req struct {
			SQL string `json:"sql"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
			return
		}
		sql := strings.TrimSpace(strings.TrimRight(strings.TrimSpace(req.SQL), ";"))
		if sql == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty query"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), *timeout)
		defer cancel()
		res, err := c.query(ctx, sql, *maxRows)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})

	http.HandleFunc("GET /api/cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, c.overviewCached(r.Context()))
	})
	// The same overview, pushed over a WebSocket twice a second.
	http.HandleFunc("GET /api/cluster/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil) // refuses other origins
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := conn.CloseRead(r.Context()) // ends when the browser goes away
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			if err := wsjson.Write(ctx, conn, c.overviewCached(ctx)); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	})

	// Bulk insert of generated rows: POST starts a job, GET reports progress, DELETE cancels.
	jobs := &bulk{c: c}
	http.HandleFunc("GET /api/bulk", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, jobs.status())
	})
	http.HandleFunc("DELETE /api/bulk", func(w http.ResponseWriter, _ *http.Request) {
		jobs.cancel()
		writeJSON(w, http.StatusOK, jobs.status())
	})
	http.HandleFunc("POST /api/bulk", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		var req bulkRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), *timeout)
		defer cancel()
		if err := jobs.start(ctx, req); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, jobs.status())
	})

	// Bulk query (parallel point lookups for a set time): the same three calls.
	lookups := &queryLoad{c: c}
	http.HandleFunc("GET /api/bulkquery", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, lookups.status())
	})
	http.HandleFunc("DELETE /api/bulkquery", func(w http.ResponseWriter, _ *http.Request) {
		lookups.cancel()
		writeJSON(w, http.StatusOK, lookups.status())
	})
	http.HandleFunc("POST /api/bulkquery", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		var req queryLoadRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), *timeout)
		defer cancel()
		if err := lookups.start(ctx, req); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, lookups.status())
	})

	log.Printf("doris-ingress SQL console on http://%s (nodes: %s)", *listen, strings.Join(c.nodes, ", "))
	log.Fatal(http.ListenAndServe(*listen, nil))
}

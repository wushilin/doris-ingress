package main

import (
	"context"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Cluster overview: every node is asked about itself (system.node), directly and in parallel, so
// a node that is down or cut off shows as such instead of being hidden by failover.

type nodeStatus struct {
	Addr      string             `json:"addr"`
	OK        bool               `json:"ok"`
	Error     string             `json:"error,omitempty"`
	LatencyMs float64            `json:"latencyMs"`
	Info      map[string]*string `json:"info,omitempty"` // the node's system.node row, by column
	// Row versions this node holds in memory, over all tables (from its system.tables).
	BufferedRows int64 `json:"bufferedRows"`

	tables *queryResult
}

type overview struct {
	Nodes      []nodeStatus `json:"nodes"`
	Tables     *queryResult `json:"tables,omitempty"` // system.tables of one node
	TablesFrom string       `json:"tablesFrom,omitempty"`
}

var (
	overviewMu   sync.Mutex
	overviewAt   time.Time
	overviewLast overview
)

// overviewCached is the overview, at most 400 ms old: several browsers share one round of
// questions to the nodes.
func (c *cluster) overviewCached(ctx context.Context) overview {
	overviewMu.Lock()
	defer overviewMu.Unlock()
	if time.Since(overviewAt) > 400*time.Millisecond {
		overviewLast, overviewAt = c.overview(ctx), time.Now()
	}
	return overviewLast
}

func (c *cluster) overview(ctx context.Context) overview {
	// Not tied to the asking browser: the result is shared.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", c.auth)

	out := overview{Nodes: make([]nodeStatus, len(c.nodes))}
	var wg sync.WaitGroup
	for i, addr := range c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := &out.Nodes[i]
			n.Addr = addr
			begin := time.Now()
			res, err := c.queryNode(ctx, addr, "SELECT * FROM system.node", 1)
			n.LatencyMs = float64(time.Since(begin).Microseconds()) / 1000
			switch {
			case err != nil:
				n.Error = status.Convert(err).Message()
			case len(res.Rows) != 1:
				n.Error = "system.node returned no row"
			default:
				n.OK, n.Info = true, map[string]*string{}
				for i, col := range res.Columns {
					n.Info[col.Name] = res.Rows[0][i]
				}
				if t, err := c.queryNode(ctx, addr, "SELECT * FROM system.tables ORDER BY db, name", 10_000); err == nil {
					n.tables = t
					for i, col := range t.Columns {
						if col.Name != "buffered_rows" {
							continue
						}
						for _, row := range t.Rows {
							if row[i] != nil {
								rows, _ := strconv.ParseInt(*row[i], 10, 64)
								n.BufferedRows += rows
							}
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	// Table statistics from the leader, or from any node that answered when there is no leader.
	from := -1
	for i, n := range out.Nodes {
		if !n.OK {
			continue
		}
		if leader := n.Info["is_leader"]; leader != nil && *leader == "true" {
			from = i
			break
		}
		if from < 0 {
			from = i
		}
	}
	if from >= 0 && out.Nodes[from].tables != nil {
		out.Tables, out.TablesFrom = out.Nodes[from].tables, c.nodes[from]
	}
	return out
}

// queryNode runs a query on one given node, without failover. ctx must carry the credentials.
func (c *cluster) queryNode(ctx context.Context, addr, sql string, maxRows int) (*queryResult, error) {
	cl, err := c.client(addr)
	if err != nil {
		return nil, err
	}
	return runQuery(ctx, cl, addr, sql, maxRows)
}

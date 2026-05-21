// Package lagcache periodically polls Prometheus for per-tier Kafka lag
// and exposes the latest snapshot for cheap synchronous lookup (e.g. from
// HTTP middleware).
//
// Why this exists: API needs to decide "is downstream backed up?" on every
// POST /jobs. Querying Prometheus on each request is too slow and adds a
// hard dependency on Prom being up. Instead, a background goroutine refreshes
// a local map every 5s; the request path just does an RLock + map read.
package lagcache

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// LagCache 持有 topic→lag 的快照。
type LagCache struct {
	mu  sync.RWMutex
	lag map[string]int64
}

func New() *LagCache {
	return &LagCache{lag: make(map[string]int64)}
}

// Get 当前 topic 的 lag。topic 不存在时返回 0（保守：当 "没积压"）。
func (c *LagCache) Get(topic string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lag[topic]
}

// Snapshot 返回所有 topic 的当前快照（调试/admin 用）。
func (c *LagCache) Snapshot() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int64, len(c.lag))
	for k, v := range c.lag {
		out[k] = v
	}
	return out
}

// Run 启动后台 polling，每 interval 拉一次。ctx 关闭时退出。
// 是 blocking 调用——通常在自己的 goroutine 里跑。
func (c *LagCache) Run(ctx context.Context, promURL string, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	c.refresh(ctx, promURL, log) // 起手立刻拉一次
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx, promURL, log)
		}
	}
}

// PromQL 响应的最小可读结构。
type promResp struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"` // [timestamp, "stringValue"]
		} `json:"result"`
	} `json:"data"`
}

func (c *LagCache) refresh(ctx context.Context, promURL string, log *slog.Logger) {
	q := url.Values{}
	q.Set("query", `sum by (topic) (pp_kafka_consumer_lag)`)
	reqURL := promURL + "/api/v1/query?" + q.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	hc := &http.Client{Timeout: 5 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		log.Warn("lag refresh failed", "err", err)
		return
	}
	defer resp.Body.Close()

	var pr promResp
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		log.Warn("lag refresh decode", "err", err)
		return
	}
	if pr.Status != "success" {
		return
	}

	next := make(map[string]int64, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		topic := r.Metric["topic"]
		valStr, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		next[topic] = int64(v)
	}

	c.mu.Lock()
	c.lag = next
	c.mu.Unlock()
}

package lagcache

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/759257989/processing-platform/internal/jobs"
)

// 每 tier 的 lag 阈值——超过就 backpressure。
// 数字根据 tier 容忍度调：realtime 严，bulk 松。
var DefaultThresholds = map[jobs.Tier]int64{
	jobs.TierRealtime: 100,
	jobs.TierStandard: 1000,
	jobs.TierBulk:     5000,
}

// Backpressure 返回一个 gin middleware：
//   - 仅在 POST /jobs 上生效
//   - peek 请求 body 找到 type，算 tier，查 lag cache
//   - lag 超 tier 阈值 → 返回 503 + Retry-After: 5，并 Abort 后续 handler
//   - 否则 c.Next() 继续往下
//
// 中间件读了 body 后必须用 replayReader 把 body 复原，否则后面 gin handler
// 拿到的 c.Request.Body 是空的（HTTP body 是 stream，读完即空）。
func Backpressure(cache *LagCache, thresholds map[jobs.Tier]int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 只对 POST /jobs 生效
		if c.Request.Method != http.MethodPost || c.FullPath() != "/jobs" {
			c.Next()
			return
		}

		// 读 body，解出 type，复原 body
		body, _ := io.ReadAll(c.Request.Body)
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		var req struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			c.Next() // 解不出 type，让 handler 报真 error
			return
		}
		tier, err := jobs.TierFor(jobs.Type(req.Type))
		if err != nil {
			c.Next() // unknown type，handler 自己拒
			return
		}
		topic := "jobs." + string(tier)
		lag := cache.Get(topic)
		threshold := thresholds[tier]
		if threshold > 0 && lag > threshold {
			c.Header("Retry-After", "5")              // 客户端应该等 5s 再重试
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error":     "overloaded",
				"tier":      string(tier),
				"lag":       lag,
				"threshold": threshold,
			})
			return
		}
		c.Next()
	}
}

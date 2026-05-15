package observability

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GinMetrics returns a middleware that records pp_http_* metrics.
// Usage: r.Use(observability.GinMetrics(obs.Metrics))
func GinMetrics(m *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		// Use FullPath (route template) not URL.Path — otherwise /jobs/<uuid>
		// blows up label cardinality. FullPath returns "/jobs/:id".
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := strconv.Itoa(c.Writer.Status())

		m.HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, status).Inc()
		m.HTTPRequestDuration.WithLabelValues(c.Request.Method, route, status).
			Observe(time.Since(start).Seconds())
	}
}

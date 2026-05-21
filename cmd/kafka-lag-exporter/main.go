// cmd/kafka-lag-exporter: tiny Prometheus exporter for Kafka consumer lag.
//
// 跑成 Deployment（replicas=1，单 instance 足够）。每 POLL_INTERVAL 周期：
//  1. 对每个 (group, topic) 配对，先 Metadata 查 partition 列表
//  2. ListOffsets(LastOffset) 拿 log-end offsets
//  3. OffsetFetch(group) 拿 committed offsets
//  4. lag = log-end - committed，写到 pp_kafka_consumer_lag gauge
//
// Prometheus 通过 ServiceMonitor scrape :9090/metrics。
// HPA 通过 prometheus-adapter 把这个 metric 暴露成 custom metrics API（Phase 2）。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// 要监控的 consumer group → topic 配对。
// 这里硬编码够用——3 个 tier 是项目宪法。要更动态可以扫 Kafka 所有 group，
// 但 cluster role / RBAC 会复杂，YAGNI。
var groupTopics = map[string]string{
	"worker-realtime": "jobs.realtime",
	"worker-standard": "jobs.standard",
	"worker-bulk":     "jobs.bulk",
}

// pp_kafka_consumer_lag — 跟 internal/observability/metrics.go 里的 gauge 同名同 label。
// Dashboard 和 alert rule 已经引用这个 metric，我们这里"接管"它的填充。
var lagGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "pp",
		Subsystem: "kafka",
		Name:      "consumer_lag",
		Help:      "Kafka consumer lag per (topic, group, partition).",
	},
	[]string{"topic", "group", "partition"},
)

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "pp-kafka-controller-headless:9092"), ",")
	interval, _ := time.ParseDuration(envOr("POLL_INTERVAL", "15s"))
	metricsAddr := envOr("METRICS_ADDR", ":9090")

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "kafka-lag-exporter")
	slog.SetDefault(log)

	prometheus.MustRegister(lagGauge)

	// /metrics + /healthz HTTP server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})
	go func() {
		log.Info("listening", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}

	log.Info("starting poll loop", "interval", interval, "brokers", brokers)
	// 起手立即跑一次，再进 ticker
	collectAll(client, log)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		collectAll(client, log)
	}
}

func collectAll(client *kafka.Client, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for group, topic := range groupTopics {
		if err := collectOne(ctx, client, group, topic); err != nil {
			log.Warn("collect failed", "group", group, "topic", topic, "err", err)
		}
	}
}

func collectOne(ctx context.Context, client *kafka.Client, group, topic string) error {
	// 1) Metadata：拿这个 topic 的所有 partition id
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return err
	}
	var partitionIDs []int
	for _, t := range meta.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			partitionIDs = append(partitionIDs, p.ID)
		}
	}
	if len(partitionIDs) == 0 {
		return nil // topic 还没创建过，跳过
	}

	// 2) ListOffsets(LastOffset)：每个 partition 当前的 log-end offset
	offsetReqs := make([]kafka.OffsetRequest, 0, len(partitionIDs))
	for _, pid := range partitionIDs {
		offsetReqs = append(offsetReqs, kafka.OffsetRequest{Partition: pid, Timestamp: kafka.LastOffset})
	}
	endResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{topic: offsetReqs},
	})
	if err != nil {
		return err
	}
	endOffsets := make(map[int]int64, len(partitionIDs))
	for _, po := range endResp.Topics[topic] {
		endOffsets[po.Partition] = po.LastOffset
	}

	// 3) OffsetFetch：consumer group 当前提交到第几
	fetchResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: partitionIDs},
	})
	if err != nil {
		return err
	}

	// 4) 写 gauge：lag = end - committed (重平衡期间负数归零)
	for _, po := range fetchResp.Topics[topic] {
		end, ok := endOffsets[po.Partition]
		if !ok {
			continue
		}
		lag := end - po.CommittedOffset
		if lag < 0 {
			lag = 0
		}
		lagGauge.WithLabelValues(topic, group, strconv.Itoa(po.Partition)).Set(float64(lag))
	}
	return nil
}

func envOr(k, fb string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fb
}

// cmd/devsim: 模拟 N 个 MQTT 设备持续推 telemetry。
//
// 设计：
//   - N 个 goroutine，每个对应一个虚拟设备
//   - 每个 device 独立 MQTT client（共享 broker 连接的话，1 个 client 单线程发，做不出 fan-out）
//   - 每条 message 间隔随机 [interval-jitter, interval+jitter]，避免 thundering herd
//   - 1% chance per cycle to "go offline"（停 30s）模拟设备离网
//   - 自己暴露 /metrics：published_total / failed_total / devices_active
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	publishedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pp", Subsystem: "devsim",
		Name: "messages_published_total", Help: "Total MQTT messages successfully published.",
	})
	failedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "pp", Subsystem: "devsim",
		Name: "messages_failed_total", Help: "Total MQTT publish failures.",
	})
	devicesActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "pp", Subsystem: "devsim",
		Name: "devices_active", Help: "Current number of devices in online state.",
	})
)

func main() {
	broker := envOr("MQTT_BROKER", "tcp://mosquitto:1883")
	deviceCount, _ := strconv.Atoi(envOr("DEVICE_COUNT", "100"))
	intervalMin, _ := time.ParseDuration(envOr("INTERVAL_MIN", "1s"))
	intervalMax, _ := time.ParseDuration(envOr("INTERVAL_MAX", "5s"))
	failureRate, _ := strconv.ParseFloat(envOr("FAILURE_RATE", "0.01"), 64)
	metricsAddr := envOr("METRICS_ADDR", ":9090")

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "devsim")
	log.Info("starting", "devices", deviceCount, "broker", broker,
		"interval", fmt.Sprintf("%v..%v", intervalMin, intervalMax), "failure_rate", failureRate)

	prometheus.MustRegister(publishedTotal, failedTotal, devicesActive)

	// /metrics HTTP
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	go func() {
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Error("http server", "err", err)
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 起 N 个 goroutine，每个一台设备
	for i := 0; i < deviceCount; i++ {
		deviceID := fmt.Sprintf("dev-%05d", i)
		go runDevice(ctx, deviceID, broker, intervalMin, intervalMax, failureRate, log)
	}

	log.Info("all devices started; waiting for signal")
	<-ctx.Done()
	log.Info("shutting down")
	time.Sleep(2 * time.Second) // 给 MQTT client 一点 disconnect 时间
}

// 一台设备的生命循环。
func runDevice(ctx context.Context, deviceID, broker string,
	intervalMin, intervalMax time.Duration, failureRate float64, log *slog.Logger) {

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(deviceID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetCleanSession(true)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		log.Warn("connect failed", "device", deviceID, "err", token.Error())
		return
	}
	defer client.Disconnect(250)

	devicesActive.Inc()
	defer devicesActive.Dec()

	topic := "devices/" + deviceID + "/telemetry"
	jitter := func() time.Duration {
		span := intervalMax - intervalMin
		return intervalMin + time.Duration(rand.Int63n(int64(span)))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter()):
		}

		// 1% chance 离线 30s
		if rand.Float64() < failureRate {
			devicesActive.Dec()
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
			devicesActive.Inc()
			continue
		}

		payload, _ := json.Marshal(map[string]any{
			"value":     30 + rand.Float64()*40, // 模拟温度 30~70
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})

		if token := client.Publish(topic, 1, false, payload); token.WaitTimeout(5*time.Second) && token.Error() != nil {
			failedTotal.Inc()
			continue
		}
		publishedTotal.Inc()
	}
}

func envOr(k, fb string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fb
}

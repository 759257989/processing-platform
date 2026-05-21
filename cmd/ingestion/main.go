// cmd/ingestion: subscribes to MQTT telemetry topics and converts each
// message into a TELEMETRY_PROCESSING job.
//
// Why a separate service rather than letting devices POST to /jobs directly:
// MQTT is the standard IoT protocol — devices have weak networking, sleep,
// and reconnect. Mosquitto handles those concerns. The ingestion service
// turns "MQTT messages on a topic" into "jobs in our system" — bridging
// two protocols is its only job.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/jobsubmitter"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	mqttBroker := envOr("MQTT_BROKER", "tcp://mosquitto:1883")
	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	pgDSN := mustEnv("POSTGRES_DSN")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer := kafka.NewProducer(brokers)
	defer producer.Close()

	st, err := store.New(ctx, pgDSN)
	if err != nil {
		log.Error("postgres connect", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// 关键修复：用 JobSubmitter 而不是直接 Publish。
	// Submitter 内部走 CreateJob → Publish → UpdateJobState(QUEUED) 三步，
	// 跟 API 的 /jobs 提交路径一致——worker 这边 GetJob 才能找到行。
	submitter := jobsubmitter.New(producer, st)

	opts := mqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID("ingestion-" + randSuffix()).
		SetAutoReconnect(true).
		// Don't drop messages on temporary disconnects.
		SetCleanSession(false).
		// Per-connection timeout. Fail fast if Mosquitto is dead.
		SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Error("mqtt connect", "err", token.Error())
		os.Exit(1)
	}
	defer client.Disconnect(1000)
	log.Info("mqtt connected", "broker", mqttBroker)

	// Subscribe to telemetry from any device. The "+" wildcard captures
	// exactly one topic level — matches "devices/abc/telemetry" but not
	// "devices/abc/cmd/x".
	handler := func(c mqtt.Client, m mqtt.Message) {
		if err := handleMessage(ctx, log, submitter, m); err != nil {
			log.Error("handle mqtt message", "err", err, "topic", m.Topic())
		}
	}
	if token := client.Subscribe("devices/+/telemetry", 1 /* QoS 1 */, handler); token.Wait() && token.Error() != nil {
		log.Error("mqtt subscribe", "err", token.Error())
		os.Exit(1)
	}
	log.Info("ingestion ready")

	<-ctx.Done()
	log.Info("shutting down")
}

func handleMessage(ctx context.Context, log *slog.Logger, submitter *jobsubmitter.Submitter, m mqtt.Message) error {
	// Topic format: devices/<device_id>/telemetry
	parts := strings.Split(m.Topic(), "/")
	if len(parts) != 3 || parts[0] != "devices" || parts[2] != "telemetry" {
		return fmt.Errorf("unexpected topic: %s", m.Topic())
	}
	deviceID := parts[1]

	// Try to parse the payload — if it's not JSON, wrap in a generic shape.
	var payload json.RawMessage
	if err := json.Unmarshal(m.Payload(), &payload); err != nil {
		payload = json.RawMessage(fmt.Sprintf(`{"raw":%q}`, string(m.Payload())))
	}

	// Idempotency: device_id + minute bucket. MQTT QoS 1 may redeliver the
	// same message — without idempotency, we'd create duplicate jobs. The
	// minute bucket means same device within the same minute is treated as
	// one logical event.
	idemp := fmt.Sprintf("mqtt-%s-%d", deviceID, time.Now().Unix()/60)

	// 通过 Submitter 走 CreateJob → Publish → UpdateJobState(QUEUED)。
	// 跟 API /jobs 路径一致——worker 端 GetJob 才能找到 row。
	return submitter.Submit(ctx, jobs.TypeTelemetryProcessing, deviceID, idemp, payload)
}

func randSuffix() string {
	b := make([]byte, 4)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		os.Stderr.WriteString("env " + k + " required\n")
		os.Exit(1)
	}
	return v
}

func envOr(k, fb string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fb
}


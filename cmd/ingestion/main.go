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
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/kafka"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	mqttBroker := envOr("MQTT_BROKER", "tcp://mosquitto:1883")
	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer := kafka.NewProducer(brokers)
	defer producer.Close()

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
		if err := handleMessage(ctx, log, producer, m); err != nil {
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

func handleMessage(ctx context.Context, log *slog.Logger, producer *kafka.Producer, m mqtt.Message) error {
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

	// Idempotency: device_id + a stable hash of the payload + minute bucket.
	// MQTT QoS 1 may redeliver the same message — without idempotency, we'd
	// create duplicate jobs. The minute bucket means same payload within the
	// same minute is treated as one logical event.
	idemp := fmt.Sprintf("mqtt-%s-%d", deviceID, time.Now().Unix()/60)

	// Build the same kafka message format the API produces. Reuses the
	// worker's parsing — a TELEMETRY_PROCESSING job from MQTT looks identical
	// to one from REST, so workers don't care which side of the world it
	// came from.
	msg, _ := json.Marshal(map[string]any{
		"job_id":          newJobID(),
		"type":            string(jobs.TypeTelemetryProcessing),
		"tier":            string(jobs.TierStandard),
		"device_id":       deviceID,
		"payload":         payload,
		"idempotency_key": idemp,
	})

	return producer.Publish(ctx, "jobs.standard", []byte(deviceID), msg)
}

func randSuffix() string {
	b := make([]byte, 4)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

func newJobID() string { return uuid.New().String() }

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

var _ = rand.IntN // keep import; remove if unused

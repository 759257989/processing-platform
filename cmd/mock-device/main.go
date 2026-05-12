// cmd/mock-device: a tunable fake device-control endpoint.
// Knobs via env:
//   - FAIL_RATE   (float, e.g. 0.1 → reject 10% of requests with 500)
//   - LATENCY_MS  (int,   e.g. 200 → add 200ms per request)
// The simulator (Stage 5) uses these to exercise retry, DLQ, and SLO alerts.
package main

import (
    "encoding/json"
    "fmt"
    "log/slog"
    "math/rand/v2"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

    failRate, _ := strconv.ParseFloat(envOr("FAIL_RATE", "0"), 64)
    latency, _ := strconv.Atoi(envOr("LATENCY_MS", "0"))

    log.Info("mock-device starting", "fail_rate", failRate, "latency_ms", latency)

    handler := func(w http.ResponseWriter, r *http.Request) {
        if latency > 0 {
            time.Sleep(time.Duration(latency) * time.Millisecond)
        }
        if rand.Float64() < failRate {
            http.Error(w, "injected failure", http.StatusInternalServerError)
            return
        }

        var body map[string]any
        _ = json.NewDecoder(r.Body).Decode(&body)
        log.Info("mock-device call", "path", r.URL.Path, "body", body)

        // Per-path canned responses.
        switch r.URL.Path {
        case "/command":
            json.NewEncoder(w).Encode(map[string]any{"ok": true})
        case "/health":
            // Healthy 90% of the time; otherwise unhealthy with last_seen.
            if rand.Float64() < 0.9 {
                json.NewEncoder(w).Encode(map[string]any{"healthy": true, "last_seen_ago": "5s"})
            } else {
                json.NewEncoder(w).Encode(map[string]any{"healthy": false, "last_seen_ago": "5m"})
            }
        case "/firmware":
            json.NewEncoder(w).Encode(map[string]any{"ok": true})
        default:
            http.Error(w, "not found", http.StatusNotFound)
        }
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/command", handler)
    mux.HandleFunc("/health", handler)
    mux.HandleFunc("/firmware", handler)
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
    })

    addr := envOr("LISTEN_ADDR", ":8080")
    log.Info("mock-device listening", "addr", addr)
    if err := http.ListenAndServe(addr, mux); err != nil {
        log.Error("server", "err", err)
        os.Exit(1)
    }
    _ = fmt.Sprintf
}

func envOr(k, fb string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return fb
}

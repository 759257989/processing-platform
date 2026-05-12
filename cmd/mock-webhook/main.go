// cmd/mock-webhook: receives alert posts. Just logs them.
// The Alertmanager test in Stage 4 will route to this.
package main

import (
    "encoding/json"
    "io"
    "log/slog"
    "net/http"
    "os"
)

func main() {
    log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

    mux := http.NewServeMux()
    mux.HandleFunc("/alert", func(w http.ResponseWriter, r *http.Request) {
        body, _ := io.ReadAll(r.Body)
        var pretty any
        _ = json.Unmarshal(body, &pretty)
        log.Info("alert received", "body", pretty)
        w.WriteHeader(http.StatusNoContent)
    })
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
    })

    addr := envOr("LISTEN_ADDR", ":8080")
    log.Info("mock-webhook listening", "addr", addr)
    if err := http.ListenAndServe(addr, mux); err != nil {
        log.Error("server", "err", err)
        os.Exit(1)
    }
}

func envOr(k, fb string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return fb
}

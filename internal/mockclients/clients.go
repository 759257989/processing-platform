// Package mockclients wraps HTTP calls to the mock-device and mock-webhook
// services. "Mock" only refers to the *target*: these are real HTTP clients
// talking to fake servers we control. In production they'd be swapped for
// real device-control APIs and a real alerting backend.
package mockclients

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "time"
)

// ---- DeviceClient ----

type DeviceClient struct {
    http *http.Client
    base string
}

func NewDeviceClient(httpC *http.Client, baseURL string) *DeviceClient {
    return &DeviceClient{http: httpC, base: baseURL}
}

func (c *DeviceClient) SendCommand(ctx context.Context, deviceID, command string, args []byte) ([]byte, error) {
    body, _ := json.Marshal(map[string]any{
        "device_id": deviceID,
        "command":   command,
        "arguments": json.RawMessage(args),
    })
    return c.post(ctx, "/command", body)
}

func (c *DeviceClient) GetHealth(ctx context.Context, deviceID string) (bool, string, error) {
    body, err := c.post(ctx, "/health", []byte(fmt.Sprintf(`{"device_id":%q}`, deviceID)))
    if err != nil {
        return false, "", err
    }
    var resp struct {
        Healthy      bool   `json:"healthy"`
        LastSeenAgo  string `json:"last_seen_ago"`
    }
    if err := json.Unmarshal(body, &resp); err != nil {
        return false, "", err
    }
    return resp.Healthy, resp.LastSeenAgo, nil
}

func (c *DeviceClient) PushFirmware(ctx context.Context, deviceID, version string) error {
    body, _ := json.Marshal(map[string]string{
        "device_id":      deviceID,
        "target_version": version,
    })
    _, err := c.post(ctx, "/firmware", body)
    return err
}

func (c *DeviceClient) post(ctx context.Context, path string, body []byte) ([]byte, error) {
    // Per-call timeout. Keeps a slow downstream from blocking a worker forever.
    // Realtime tier needs this most, but applying everywhere is simpler.
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
    if err != nil {
        return nil, err
    }
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    raw, _ := io.ReadAll(resp.Body)
    if resp.StatusCode >= 400 {
        return nil, fmt.Errorf("device call %s: HTTP %d: %s", path, resp.StatusCode, string(raw))
    }
    return raw, nil
}

// ---- WebhookClient ----

type WebhookClient struct {
    http *http.Client
    base string
}

func NewWebhookClient(httpC *http.Client, baseURL string) *WebhookClient {
    return &WebhookClient{http: httpC, base: baseURL}
}

func (c *WebhookClient) PostAlert(ctx context.Context, severity, message string, payload []byte) error {
    body, _ := json.Marshal(map[string]any{
        "severity": severity,
        "message":  message,
        "payload":  json.RawMessage(payload),
    })

    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/alert", bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        raw, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("webhook: HTTP %d: %s", resp.StatusCode, string(raw))
    }
    return nil
}

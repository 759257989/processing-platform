// Package retry computes the next-retry timestamp for a failed job.
package retry

import (
    "math"
    "math/rand/v2"
    "time"
)

// Delay computes the delay before retry attempt N (1-indexed: attempt 1 is
// the FIRST retry, not the original try). Jitter is ±50% of base.
//
// Attempt 1 → ~1s     (0.5s–1.5s)
// Attempt 2 → ~5s     (2.5s–7.5s)
// Attempt 3 → ~25s    (12.5s–37.5s)
// Attempt N → ~5^(N-1) seconds
func Delay(attempt int) time.Duration {
    if attempt < 1 {
        attempt = 1
    }
    base := time.Duration(math.Pow(5, float64(attempt-1))) * time.Second
    // Jitter: random multiplier in [0.5, 1.5).
    jitter := 0.5 + rand.Float64()
    return time.Duration(float64(base) * jitter)
}

// NextAt returns now + Delay(attempt). The retry-router uses this to schedule
// the republish.
func NextAt(now time.Time, attempt int) time.Time {
    return now.Add(Delay(attempt))
}

package retry

import (
    "testing"
    "time"
)

func TestDelay_GrowsWithAttempt(t *testing.T) {
    // Smoke: delay(2) should be much bigger than delay(1) on average.
    var sum1, sum2 time.Duration
    const N = 100
    for i := 0; i < N; i++ {
        sum1 += Delay(1)
        sum2 += Delay(2)
    }
    if sum2 <= sum1 {
        t.Fatalf("expected delay(2) avg > delay(1) avg; got %v vs %v", sum2/N, sum1/N)
    }
}

func TestDelay_Bounded(t *testing.T) {
    // Delay 1 should always be in [0.5s, 1.5s].
    for i := 0; i < 100; i++ {
        d := Delay(1)
        if d < 500*time.Millisecond || d > 1500*time.Millisecond {
            t.Fatalf("delay 1 out of range: %v", d)
        }
    }
}

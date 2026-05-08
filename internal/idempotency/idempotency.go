// Package idempotency provides idempotency-key-based deduplication
// for job submissions. Backed by Redis.
package idempotency

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/google/uuid"
    "github.com/redis/go-redis/v9"
)

const (
    keyPrefix = "idem:"
    ttl       = 24 * time.Hour
)

// Acquirer is the contract for an idempotency store.
// We define it as an interface so tests can mock it without a real Redis.
type Acquirer interface {
    // Acquire tries to claim a unique slot for `key`. Returns:
    //   - jobID, true, nil  if the key was new (caller proceeds with new job).
    //   - jobID, false, nil if the key was already claimed (caller returns existing).
    //   - "", false, error  on any other error.
    Acquire(ctx context.Context, key string) (jobID uuid.UUID, isNew bool, err error)
}

// RedisAcquirer is the production implementation backed by Redis.
type RedisAcquirer struct {
    rdb *redis.Client
}

// NewRedis builds a RedisAcquirer wrapping the given Redis client.
func NewRedis(rdb *redis.Client) *RedisAcquirer {
    return &RedisAcquirer{rdb: rdb}
}

func (r *RedisAcquirer) Acquire(ctx context.Context, key string) (uuid.UUID, bool, error) {
    redisKey := keyPrefix + key

    // Allocate a job ID up-front. If we win the SETNX race, this is the new
    // job's ID. If we lose, we discard it and read the winner's ID.
    jobID := uuid.New()

    // SetNX: SET key value NX EX <ttl>.
    // Returns true if the key was set (we won), false if it already existed (we lost).
    ok, err := r.rdb.SetNX(ctx, redisKey, jobID.String(), ttl).Result()
    if err != nil {
        return uuid.Nil, false, fmt.Errorf("redis SETNX: %w", err)
    }
    if ok {
        return jobID, true, nil
    }

    // Lost the race — fetch the winning job ID.
    existing, err := r.rdb.Get(ctx, redisKey).Result()
    if err != nil {
        if errors.Is(err, redis.Nil) {
            // The key expired between SETNX and GET — extremely rare; let caller retry.
            return uuid.Nil, false, errors.New("idempotency key vanished mid-flight")
        }
        return uuid.Nil, false, fmt.Errorf("redis GET: %w", err)
    }

    parsed, err := uuid.Parse(existing)
    if err != nil {
        return uuid.Nil, false, fmt.Errorf("parsing stored job id %q: %w", existing, err)
    }

    return parsed, false, nil
}

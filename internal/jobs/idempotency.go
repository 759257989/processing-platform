package jobs

import (
    "fmt"
    "regexp"
)

// idempotencyKeyPattern: alphanumeric, dash, underscore, between 8 and 128 chars.
// We don't want clients sending us multi-megabyte keys or keys with control characters.
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

// ValidateIdempotencyKey checks the format. Empty / too short / too long /
// containing forbidden characters → error.
func ValidateIdempotencyKey(key string) error {
    if !idempotencyKeyPattern.MatchString(key) {
        return fmt.Errorf("idempotency_key must match %s", idempotencyKeyPattern.String())
    }
    return nil
}

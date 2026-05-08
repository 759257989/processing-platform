package jobs

import (
    "strings"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestValidateIdempotencyKey(t *testing.T) {
    valid := []string{
        "abc12345",
        "abcd-efgh-ijkl",
        strings.Repeat("a", 128),
    }
    for _, k := range valid {
        assert.NoError(t, ValidateIdempotencyKey(k), "expected %q to be valid", k)
    }

    invalid := []string{
        "",                            // empty
        "abc",                         // too short
        strings.Repeat("a", 129),      // too long
        "with spaces here",            // contains space
        "abcd!@#$",                    // contains special chars
    }
    for _, k := range invalid {
        assert.Error(t, ValidateIdempotencyKey(k), "expected %q to be invalid", k)
    }
}

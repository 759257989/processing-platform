package jobs

import (
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestTransition_Valid(t *testing.T) {
    cases := []struct {
        from, to State
    }{
        {StateAccepted, StateQueued},
        {StateQueued, StateRunning},
        {StateRunning, StateSuccess},
        {StateRunning, StateFailed},
        {StateRunning, StateRetry},
        {StateRetry, StateQueued},
    }
    for _, c := range cases {
        t.Run(string(c.from)+"_to_"+string(c.to), func(t *testing.T) {
            err := Transition(c.from, c.to)
            assert.NoError(t, err)
        })
    }
}

func TestTransition_Invalid(t *testing.T) {
    cases := []struct {
        from, to State
    }{
        {StateAccepted, StateRunning},  // skipped QUEUED
        {StateSuccess, StateRunning},   // terminal
        {StateFailed, StateQueued},     // terminal
        {StateRunning, StateAccepted},  // backwards
    }
    for _, c := range cases {
        t.Run(string(c.from)+"_to_"+string(c.to), func(t *testing.T) {
            err := Transition(c.from, c.to)
            assert.Error(t, err)
        })
    }
}

func TestIsTerminal(t *testing.T) {
    assert.True(t, IsTerminal(StateSuccess))
    assert.True(t, IsTerminal(StateFailed))
    assert.False(t, IsTerminal(StateRunning))
    assert.False(t, IsTerminal(StateQueued))
}

package jobs

import "fmt"

// validTransitions captures the allowed state transitions.
// A→B exists in the map iff transition A→B is allowed.
var validTransitions = map[State]map[State]bool{
    StateAccepted: {StateQueued: true, StateFailed: true},
    StateQueued:   {StateRunning: true, StateFailed: true},
    StateRunning:  {StateSuccess: true, StateFailed: true, StateRetry: true},
    StateRetry:    {StateQueued: true, StateFailed: true},
    StateSuccess:  {}, // terminal
    StateFailed:   {}, // terminal
}

// Transition checks whether moving from `from` to `to` is allowed.
// Returns nil if it is, or a descriptive error if not.
func Transition(from, to State) error {
    allowed, ok := validTransitions[from]
    if !ok {
        return fmt.Errorf("unknown source state %q", from)
    }
    if !allowed[to] {
        return fmt.Errorf("invalid transition: %s → %s", from, to)
    }
    return nil
}

// IsTerminal reports whether state is a final state (SUCCESS or FAILED).
func IsTerminal(s State) bool {
    return s == StateSuccess || s == StateFailed
}

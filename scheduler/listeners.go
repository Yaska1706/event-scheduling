package scheduler

import "time"

// Listeners has attached events to listener
type Listeners map[string]ListenFunc

// ListenFunc listens to events
type ListenFunc func(string)

// Event structure
type Event struct {
	ID      uint
	Name    string
	Payload string
	RunAt   time.Time
}

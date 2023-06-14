package distribution

import (
	"strings"
)

const (
	EventNodeAdded EventType = iota
	EventNodeRemoved
)

// Event describes a distribution change - a change in the list of nodes.
type Event struct {
	Type    EventType
	NodeID  string
	Message string
}

type EventType int

type Events []Event

// Messages converts events to a string for logging purposes.
func (v Events) Messages() string {
	var out strings.Builder
	last := len(v) - 1
	for i, e := range v {
		out.WriteString(e.Message)
		if i != last {
			out.WriteString("; ")
		}
	}
	return out.String()
}

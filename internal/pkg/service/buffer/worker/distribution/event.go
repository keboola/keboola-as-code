package distribution

import (
	"strings"
)

const (
	EventTypeAdd EventType = iota
	EventTypeRemove
)

type EventType int

type Events []Event

type Event struct {
	Type    EventType
	NodeID  string
	Message string
}

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

package distribution

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

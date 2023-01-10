package distribution

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvents_Messages(t *testing.T) {
	t.Parallel()

	v := Events{}

	v = append(v, Event{Type: EventTypeAdd, NodeID: "node-1", Message: `found a new node "node-1"`})
	assert.Equal(t, `found a new node "node-1"`, v.Messages())

	v = append(v, Event{Type: EventTypeRemove, NodeID: "node-2", Message: `the node "node-2" gone`})
	assert.Equal(t, `found a new node "node-1"; the node "node-2" gone`, v.Messages())
}

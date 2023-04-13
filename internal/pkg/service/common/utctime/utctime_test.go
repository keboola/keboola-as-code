package utctime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatTime(t *testing.T) {
	t.Parallel()
	now, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	assert.NoError(t, err)
	assert.Equal(t, "2006-01-02T08:04:05.000Z", FormatTime(now))
}

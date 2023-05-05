package validator

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
)

func TestValidateMinDuration(t *testing.T) {
	t.Parallel()
	err := New().ValidateValue(10*time.Millisecond, "minDuration=100ms")
	assert.Error(t, err)
	assert.Equal(t, `must be 100ms or greater`, err.Error())
}

func TestValidateMinBytes(t *testing.T) {
	t.Parallel()
	err := New().ValidateValue(datasize.ByteSize(10), "minBytes=1kB")
	assert.Error(t, err)
	assert.Equal(t, `must be 1KB or greater`, err.Error())
}

func TestValidateMaxDuration(t *testing.T) {
	t.Parallel()
	err := New().ValidateValue(200*time.Millisecond, "maxDuration=100ms")
	assert.Error(t, err)
	assert.Equal(t, `must be 100ms or less`, err.Error())
}

func TestValidateMaxBytes(t *testing.T) {
	t.Parallel()
	err := New().ValidateValue(datasize.ByteSize(2000), "maxBytes=1KB")
	assert.Error(t, err)
	assert.Equal(t, `must be 1KB or less`, err.Error())
}

package diskalloc

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
)

func TestConfig_ForNextSlice_Disabled_NoPreviousSlice(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	cfg.Enabled = false
	cfg.Static = 123 * datasize.MB
	assert.Equal(t, datasize.ByteSize(0), cfg.ForNextSlice(0))
}

func TestConfig_ForNextSlice_Disabled_PreviousSlice(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	cfg.Enabled = false
	cfg.Static = 123 * datasize.MB
	assert.Equal(t, datasize.ByteSize(0), cfg.ForNextSlice(100*datasize.MB))
}

func TestConfig_ForNextSlice_Enabled_NoPreviousSlice(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	cfg.Enabled = true
	cfg.Static = 123 * datasize.MB
	assert.Equal(t, 123*datasize.MB, cfg.ForNextSlice(0))
}

func TestConfig_ForNextSlice_Enabled_PreviousSlice(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	cfg.Enabled = true
	cfg.Static = 123 * datasize.MB
	cfg.Relative = 300
	assert.Equal(t, 300*datasize.MB, cfg.ForNextSlice(100*datasize.MB))
}

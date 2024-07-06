package diskalloc

import "github.com/c2h5oh/datasize"

// Config configures allocation of the disk space for file slices.
type Config struct {
	// Enabled enables disk space allocation.
	Enabled bool `json:"enabled" configKey:"enabled" configUsage:"Allocate disk space for each slice."`
	// Static size of the first slice in a sink.
	Static datasize.ByteSize `json:"static" configKey:"static" validate:"required" configUsage:"Size of allocated disk space for a slice."`
	// Relative size of new slice.
	// The size of the slice will be % from the average slice size.
	// Use 0 to disable relative allocation.
	Relative int `json:"relative" configKey:"relative" validate:"min=100,max=500" configUsage:"Allocate disk space as % from the previous slice size."`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Enabled  *bool              `json:"enabled,omitempty"`
	Static   *datasize.ByteSize `json:"static,omitempty"`
	Relative *int               `json:"relative,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Enabled:  true,
		Relative: 110,
		Static:   100 * datasize.MB,
	}
}

func (c Config) ForNextSlice(prevSliceSize datasize.ByteSize) datasize.ByteSize {
	// Calculated pre-allocated disk space
	if c.Enabled {
		if c.Relative > 0 && prevSliceSize > 0 {
			return (prevSliceSize * datasize.ByteSize(c.Relative)) / 100
		} else {
			return c.Static
		}
	}

	return 0
}

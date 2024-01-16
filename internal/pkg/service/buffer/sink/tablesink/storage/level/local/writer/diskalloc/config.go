package diskalloc

import "github.com/c2h5oh/datasize"

// Config configures allocation of the disk space for file slices.
type Config struct {
	// Enabled enables disk space allocation.
	Enabled bool `json:"enabled" configKey:"enabled" configUsage:"Allocate disk space for each slice."`
	// Size of the first slice in a sink, or the size of each slice if DynamicSize is disabled.
	Size datasize.ByteSize `json:"size" configKey:"size" validate:"required" configUsage:"Size of allocated disk space for a slice."`
	// SizePercent enables dynamic size of allocated disk space.
	// If enabled, the size of the slice will be % from the average slice size.
	// The size of the first slice in a sink will be Size.
	SizePercent int `json:"sizePercent" configKey:"sizePercent" validate:"min=100,max=500" configUsage:"Allocate disk space as % from the previous slice size."`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Enabled:     true,
		SizePercent: 110,
		Size:        100 * datasize.MB,
	}
}

func (c Config) ForNextSlice(prevSliceSize datasize.ByteSize) datasize.ByteSize {
	// Calculated pre-allocated disk space
	if c.Enabled {
		if c.SizePercent > 0 && prevSliceSize > 0 {
			return (prevSliceSize * datasize.ByteSize(c.SizePercent)) / 100
		} else {
			return c.Size
		}
	}

	return 0
}

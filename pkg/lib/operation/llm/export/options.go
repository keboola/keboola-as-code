package export

// Options for the llm export operation.
type Options struct {
	// Force skips confirmation when directory contains existing files.
	Force bool
	// WithSamples enables table data samples in export.
	WithSamples bool
	// WithoutSamples disables table data samples in export.
	WithoutSamples bool
	// SampleLimit is the maximum number of rows per table sample.
	SampleLimit int
	// MaxSamples is the maximum number of tables to sample.
	MaxSamples int
}

// ShouldIncludeSamples returns true if samples should be included in the export.
func (o Options) ShouldIncludeSamples() bool {
	if o.WithoutSamples {
		return false
	}
	return o.WithSamples
}

// GetSampleLimit returns the sample limit, clamped to valid range.
func (o Options) GetSampleLimit() uint {
	if o.SampleLimit <= 0 {
		return 100
	}
	if o.SampleLimit > 1000 {
		return 1000
	}
	return uint(o.SampleLimit)
}

// GetMaxSamples returns the max samples, with default if not set.
func (o Options) GetMaxSamples() int {
	if o.MaxSamples <= 0 {
		return 50
	}
	return o.MaxSamples
}

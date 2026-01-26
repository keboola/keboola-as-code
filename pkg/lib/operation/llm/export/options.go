package export

// Options for the llm export operation.
type Options struct {
	// Force skips confirmation when directory contains existing files.
	Force bool
	// WithSamples enables table data samples in export.
	WithSamples bool
	// SampleLimit is the maximum number of rows per table sample.
	SampleLimit int
	// MaxSamples is the maximum number of tables to sample.
	MaxSamples int
}

// ShouldIncludeSamples returns true if samples should be included in the export.
func (o Options) ShouldIncludeSamples() bool {
	return o.WithSamples
}

// EffectiveSampleLimit returns the sample limit, clamped to valid range.
func (o Options) EffectiveSampleLimit() uint {
	if o.SampleLimit <= 0 {
		return 100
	}
	if o.SampleLimit > 1000 {
		return 1000
	}
	return uint(o.SampleLimit)
}

// EffectiveMaxSamples returns the max samples, clamped to valid range.
func (o Options) EffectiveMaxSamples() int {
	const (
		defaultMaxSamples = 50
		maxAllowedSamples = 100
	)
	if o.MaxSamples <= 0 {
		return defaultMaxSamples
	}
	if o.MaxSamples > maxAllowedSamples {
		return maxAllowedSamples
	}
	return o.MaxSamples
}

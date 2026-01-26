package export

// Sample limit constants - shared between CLI defaults and clamping.
const (
	// DefaultSampleLimit is the default number of rows per table sample.
	DefaultSampleLimit = 100
	// MaxSampleLimit is the maximum allowed rows per table sample.
	MaxSampleLimit = 1000
	// DefaultMaxSamples is the default number of tables to sample.
	DefaultMaxSamples = 50
	// MaxAllowedSamples is the maximum allowed number of tables to sample.
	MaxAllowedSamples = 100
)

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
		return DefaultSampleLimit
	}
	if o.SampleLimit > MaxSampleLimit {
		return MaxSampleLimit
	}
	return uint(o.SampleLimit)
}

// EffectiveMaxSamples returns the max samples, clamped to valid range.
func (o Options) EffectiveMaxSamples() int {
	if o.MaxSamples <= 0 {
		return DefaultMaxSamples
	}
	if o.MaxSamples > MaxAllowedSamples {
		return MaxAllowedSamples
	}
	return o.MaxSamples
}
